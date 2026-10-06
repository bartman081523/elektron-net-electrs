//! Elektron Net FX: conversion rates without an exchange.
//!
//! A live market may exist (the P2P order book on the project's kdf daemon),
//! so rates come from a source chain, tried in this order:
//!
//! 1. `fx_orderbook_rpc_url` - the live P2P order book (kdf `orderbook` RPC,
//!    pair from `fx_orderbook_base`/`fx_orderbook_rel`). The mid of the best
//!    ask/bid is the market rate; empty books and unreachable daemons just
//!    fall through to the next source. BTC-denominated books are converted
//!    into fiat with the reference prices from `fx_btc_prices_url` (mempool
//!    /api/v1/prices shape); USD/USDT books are used directly.
//! 2. `fx_rate_url` - the project registry's `rate.json` (published reference
//!    rate). The expected shape is flexible: either a flat
//!    `{"USD": ..., "EUR": ...}` object or a `{"rates": {...}}` wrapper; an
//!    optional top-level `updated_unix` (publish time) is honored when
//!    present.
//! 3. A mining cost-floor model, used as last resort (e.g. until the first
//!    `rate.json` lands): the energy cost of mining 1 ELEK at the current
//!    network hashrate (`getnetworkhashps` on the local daemon). This is an
//!    *honest lower bound* - a market can be far above it - and every rate
//!    rendered from it is labeled as a cost-floor, not as a market price.
//!
//! Sources (1)-(3) make one origin for the rate (the electrs instance that
//! also serves the history index): the elek-web `/fx` route, the SPA's price
//! suggestion, the mempool fork and the Electrum wallet all consume this
//! node's output instead of re-deriving their own model.
//!
//! The resulting snapshot is exposed four ways (per user decision):
//!   * a dynamic line appended to the Electrum console banner,
//!   * the "blockchain.fx.rates" Electrum RPC (added to `electrum.rs`),
//!   * a JSON file in mempool's /api/v1/prices shape (`fx_snapshot_path`),
//!     so the elektron-net mempool fork can consume rates without any
//!     exchange integration of its own,
//!   * a JSON file with full source/market metadata (`fx_rates_path`),
//!     served as `/fx/rates.json` by elek-web for the SPA's rate display.
//!
//! All fetching happens on one background thread (`crate::thread::spawn`),
//! matching electrs' synchronous thread/channel architecture. Failures are
//! logged and tolerated: the last known-good snapshot is kept, and the
//! thread only ever stops with the process.

use anyhow::{Context, Result};
use parking_lot::RwLock;
use serde_json::{json, Map, Value};
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bitcoincore_rpc::{Client, RpcApi};

use crate::config::{Config, SensitiveAuth};
use crate::{daemon, thread};

// Elektron Net consensus parameters (see the elektron-net repo):
// GetBlockSubsidy starts at 5 * COIN (src/validation.cpp) and the target
// block interval is 60 s (nPowTargetSpacing, src/kernel/chainparams.cpp).
const SUBSIDY_ELEK: f64 = 5.0;
const BLOCK_INTERVAL_SECS: f64 = 60.0;
// Cost-floor model assumptions. 45 J/TH corresponds to BM1368-class
// hardware (the Bitaxe Gamma class actually mining this network today).
const J_PER_TERAHASH: f64 = 45.0;
const USD_PER_KWH: f64 = 0.30;
// Used to derive EUR when a source only provides USD.
const EUR_PER_USD: f64 = 0.92;

const REGISTRY_HTTP_TIMEOUT: Duration = Duration::from_secs(30);
const ORDERBOOK_HTTP_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateSource {
    P2pMarket,
    Registry,
    MiningCostFloor,
    None,
}

impl RateSource {
    fn as_str(&self) -> &'static str {
        match self {
            RateSource::P2pMarket => "p2p_market",
            RateSource::Registry => "registry",
            RateSource::MiningCostFloor => "mining_cost_floor",
            RateSource::None => "none",
        }
    }

    // Used in the banner line: an explicit human-readable label so a
    // cost-floor rate can never be mistaken for a market price. The P2P
    // market label is completed with the pair by banner_line() (the pair
    // lives in the snapshot).
    fn banner_label(&self) -> &'static str {
        match self {
            RateSource::P2pMarket => "live P2P order book",
            RateSource::Registry => "project registry reference rate",
            RateSource::MiningCostFloor => "mining cost-floor, no market exists",
            RateSource::None => "no source available",
        }
    }
}

/// The part of a rate that comes from a real (thin) P2P order book. `mid` is
/// always present when at least one best price exists (one-sided books use
/// their side's best price), so consumers can render one number without
/// spread semantics.
#[derive(Debug, Clone)]
pub struct Market {
    /// The pair the book was read as, e.g. "ELEK/BTC".
    pub pair: String,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub mid: Option<f64>,
    pub asks: usize,
    pub bids: usize,
}

#[derive(Debug, Clone)]
pub struct RateSnapshot {
    pub usd: Option<f64>,
    pub eur: Option<f64>,
    /// When this snapshot was fetched/computed (or published, per the
    /// registry's own `updated_unix` if provided).
    pub updated_unix: u64,
    pub source: RateSource,
    /// Market detail when the rate came from the P2P order book (None for
    /// registry/cost-floor sources).
    pub market: Option<Market>,
    /// USD price of 1 BTC at snapshot time, when the reference fetch
    /// succeeded. Consumers (SPA) use it to convert the ELEK rate into
    /// BTC-denominated price suggestions even between fills.
    pub usd_per_btc: Option<f64>,
}

impl RateSnapshot {
    fn unavailable() -> Self {
        Self {
            usd: None,
            eur: None,
            updated_unix: 0,
            source: RateSource::None,
            market: None,
            usd_per_btc: None,
        }
    }
}

/// Shared, thread-safe holder for the latest rate snapshot. Constructed in
/// `server::serve()` and handed both to the fetching thread and to the
/// Electrum `Rpc` (for the banner and `blockchain.fx.rates`).
pub struct RateState {
    snapshot: RwLock<RateSnapshot>,
}

impl RateState {
    pub fn new() -> Self {
        Self {
            snapshot: RwLock::new(RateSnapshot::unavailable()),
        }
    }

    pub fn get(&self) -> RateSnapshot {
        self.snapshot.read().clone()
    }

    fn set(&self, snapshot: RateSnapshot) {
        *self.snapshot.write() = snapshot;
    }

    /// Dynamic banner line (appended to the configured `server_banner`).
    pub fn banner_line(&self) -> String {
        let snapshot = self.get();
        match snapshot.usd {
            Some(usd) => {
                // P2P-market rates name their pair ("live P2P order book
                // (ELEK/BTC)"), so a thin registry/market rate is never
                // mistaken for something deeper than it is.
                let mut label = snapshot.source.banner_label().to_owned();
                if let Some(market) = &snapshot.market {
                    label = format!("{} ({})", label, market.pair);
                }
                let mut line = format!("1 ELEK ~= ${} USD", fmt_amount(usd));
                if let Some(eur) = snapshot.eur {
                    line.push_str(&format!(" ~= {} EUR", fmt_amount(eur)));
                }
                line.push_str(&format!(" ({})", label));
                line
            }
            None => "No conversion rate available yet - no market exists for ELEK and no \
                     reference rate is published (see the fx_rate_url option / registry rate.json)"
                .to_owned(),
        }
    }

    /// Payload of the "blockchain.fx.rates" Electrum RPC.
    pub fn rpc_json(&self) -> Value {
        let snapshot = self.get();
        let age_secs = match snapshot.source {
            RateSource::None => None,
            _ => Some(unix_now().saturating_sub(snapshot.updated_unix)),
        };
        json!({
            "ticker": "ELEK",
            "usd": snapshot.usd,
            "eur": snapshot.eur,
            "source": snapshot.source.as_str(),
            "updated_unix": snapshot.updated_unix,
            "age_secs": age_secs,
            "usd_per_btc": snapshot.usd_per_btc,
            "market": snapshot.market.as_ref().map(market_json),
        })
    }

    /// JSON in mempool's `/api/v1/prices` shape: `{"time":..., "USD":...,
    /// "EUR":..., ...}` with -1 for currencies without a live rate. When no
    /// rate is available at all, this renders as `{"time":0, ...all -1}`,
    /// which consumers treat as "prices disabled" - the same form the
    /// elektron-net mempool instances served before this feature existed.
    pub fn mempool_prices_json(&self) -> Value {
        let snapshot = self.get();
        let mut prices = Map::new();
        prices.insert("time".into(), json!(snapshot.updated_unix));
        for currency in MEMPOOL_CURRENCIES {
            let rate = match currency {
                "USD" => snapshot.usd,
                "EUR" => snapshot.eur,
                _ => None,
            };
            prices.insert(currency.into(), json!(rate.unwrap_or(-1.0)));
        }
        Value::Object(prices)
    }

    /// Atomically write the mempool-shaped snapshot file (tmp + rename).
    pub fn write_mempool_snapshot(&self, path: &Path) -> Result<()> {
        write_file_atomic(path, serde_json::to_string_pretty(&self.mempool_prices_json())?)
    }

    /// Rich-shape rates file (source/market metadata, no -1 padding):
    /// consumed by elek-web's `/fx` route for the SPA's rate display.
    /// `usd: null` when no source is available - consumers treat that as
    /// "no rate", never as a price of zero.
    pub fn write_rates_json(&self, path: &Path) -> Result<()> {
        let snapshot = self.get();
        let age_secs = match snapshot.source {
            RateSource::None => None,
            _ => Some(unix_now().saturating_sub(snapshot.updated_unix)),
        };
        let body = json!({
            "ticker": "ELEK",
            "time": snapshot.updated_unix,
            "usd": snapshot.usd,
            "eur": snapshot.eur,
            "source": snapshot.source.as_str(),
            "age_secs": age_secs,
            "usd_per_btc": snapshot.usd_per_btc,
            "market": snapshot.market.as_ref().map(market_json),
        });
        write_file_atomic(path, serde_json::to_string_pretty(&body)?)
    }
}

/// Compact market description for RPC/file consumers.
fn market_json(market: &Market) -> Value {
    json!({
        "pair": market.pair,
        "best_bid": market.best_bid,
        "best_ask": market.best_ask,
        "mid": market.mid,
        "asks": market.asks,
        "bids": market.bids,
    })
}

/// Atomic file write (tmp + rename), shared by both snapshot writers.
fn write_file_atomic(path: &Path, body: String) -> Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, body)
        .with_context(|| format!("failed to write fx snapshot {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("failed to move fx snapshot to {}", path.display()))?;
    Ok(())
}

// Currency list of mempool's /api/v1/prices endpoint; USD is live here, all
// others are rendered as -1 by mempool_prices_json.
const MEMPOOL_CURRENCIES: [&str; 29] = [
    "USD", "EUR", "GBP", "CAD", "AUD", "IDR", "NZD", "SGD", "BRL", "CLP", "CNY", "KRW", "RUB",
    "JPY", "CHF", "NOK", "ISK", "TWD", "DKK", "PLN", "SEK", "ILS", "ARS", "VND", "TRY", "HKD",
    "MXN", "INR", "ZAR",
];

/// Sensitive JSON-RPC body credential with a redacted Debug (configs are
/// Debug-printed into the journal at startup).
#[derive(Clone)]
pub struct SensitiveUserPass(pub String);
impl fmt::Debug for SensitiveUserPass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<sensitive>")
    }
}

/// The kdf daemon whose P2P order book defines the market rate when one
/// exists. `userpass` goes into the JSON-RPC body only, never into logs.
pub(crate) struct ObConfig {
    /// HTTP JSON-RPC root URL (e.g. `http://127.0.0.1:7796`).
    url: String,
    userpass: SensitiveUserPass,
    base: String,
    rel: String,
}

/// FX-only projection of `Config`, moveable into a thread (avoids putting
/// `Clone` on the whole `Config`, which holds sensitive auth state).
pub(crate) struct FxConfig {
    rate_url: Option<String>,
    refresh: Duration,
    snapshot_path: Option<PathBuf>,
    rates_path: Option<PathBuf>,
    orderbook: Option<ObConfig>,
    btc_prices_url: Option<String>,
    daemon_rpc_addr: SocketAddr,
    daemon_auth: SensitiveAuth,
    jsonrpc_timeout: Duration,
}

impl FxConfig {
    pub(crate) fn from_config(config: &Config) -> Self {
        // An empty fx_rate_url (config file or CLI) disables the registry source.
        let rate_url = match config.fx_rate_url.as_str() {
            "" => None,
            url => Some(url.to_owned()),
        };
        // Same convention for the P2P market source: an empty url disables it.
        let orderbook = match config.fx_orderbook_rpc_url.trim() {
            "" => None,
            url => Some(ObConfig {
                url: url.trim().trim_end_matches('/').to_owned(),
                userpass: SensitiveUserPass(config.fx_orderbook_userpass.0.trim().to_owned()),
                base: config.fx_orderbook_base.trim().to_owned(),
                rel: config.fx_orderbook_rel.trim().to_owned(),
            }),
        };
        Self {
            rate_url,
            refresh: config.fx_refresh.max(Duration::from_secs(1)),
            snapshot_path: config.fx_snapshot_path.clone(),
            rates_path: config.fx_rates_path.clone(),
            orderbook,
            btc_prices_url: match config.fx_btc_prices_url.as_str() {
                "" => None,
                url => Some(url.to_owned()),
            },
            daemon_rpc_addr: config.daemon_rpc_addr,
            daemon_auth: config.daemon_auth.clone(),
            jsonrpc_timeout: config.jsonrpc_timeout,
        }
    }
}

/// Spawn the background fetching thread. Called from `server::serve()`.
pub fn spawn_fetcher(fx_config: FxConfig, state: Arc<RateState>) {
    thread::spawn("fx_rates", move || fetch_loop(fx_config, state));
}

/// Fetch-and-refresh loop. Never returns; per-cycle failures (market daemon
/// unreachable, registry down, daemon RPC down) are logged and retried on
/// the next cycle.
fn fetch_loop(cfg: FxConfig, state: Arc<RateState>) -> Result<()> {
    info!(
        "fx: rates thread started (market: {:?}, registry: {:?}, refresh: {:?}, snapshot: {:?}, rates: {:?})",
        cfg.orderbook.as_ref().map(|ob| format!("{}/{}", ob.base, ob.rel)),
        cfg.rate_url,
        cfg.refresh,
        cfg.snapshot_path,
        cfg.rates_path
    );
    let mut daemon_client: Option<Client> = None;
    loop {
        let mut snapshot = None;

        // BTC reference prices are needed exactly when the market pair is
        // BTC-denominated; they also ride along on every other snapshot so
        // consumers can render ELEK-in-BTC suggestions between fills.
        let btc_rates = match (&cfg.orderbook, &cfg.btc_prices_url) {
            (Some(ob), Some(url)) if is_btc_rel(&ob.rel) => match fetch_btc_prices(url) {
                Ok(rates) => Some(rates),
                Err(e) => {
                    debug!("fx: BTC reference prices unavailable: {:#}", e);
                    None
                }
            },
            _ => None,
        };
        let usd_per_btc = btc_rates.as_ref().map(|(usd, _)| *usd);

        // Source 1: the live P2P order book. Empty books and unreachable
        // daemons are ordinary states - the lookup falls through.
        if let Some(ob) = &cfg.orderbook {
            match fetch_orderbook(ob) {
                Ok(Some(market)) => match market_rate(&market, &ob.rel, btc_rates.as_ref()) {
                    Ok((usd, eur)) => {
                        debug!("fx: P2P market rate from {}", market.pair);
                        snapshot = Some(RateSnapshot {
                            usd: Some(usd),
                            eur: Some(eur),
                            updated_unix: unix_now(),
                            source: RateSource::P2pMarket,
                            market: Some(market),
                            usd_per_btc,
                        });
                    }
                    Err(e) => debug!("fx: P2P market rate not convertible: {:#}", e),
                },
                Ok(None) => debug!("fx: P2P order book is empty"),
                Err(e) => debug!("fx: P2P order book unavailable: {:#}", e),
            }
        }

        if snapshot.is_none() {
            if let Some(url) = &cfg.rate_url {
                match fetch_registry(url) {
                    Ok((usd, eur, published)) => {
                        debug!("fx: registry rates fetched");
                        snapshot = Some(RateSnapshot {
                            usd,
                            eur: eur.or(usd.map(|usd| usd * EUR_PER_USD)),
                            updated_unix: published.unwrap_or_else(unix_now),
                            source: RateSource::Registry,
                            market: None,
                            usd_per_btc,
                        });
                    }
                    // Expected until the registry's rate.json exists (HTTP
                    // 404), so log at debug only - the per-cycle retry must
                    // not spam the journal, while RUST_LOG=debug still shows
                    // every attempt.
                    Err(e) => debug!("fx: registry rates unavailable: {:#}", e),
                }
            }
        }

        if snapshot.is_none() {
            // Cost-floor fallback (user-selected combination: market first,
            // then registry, then the mining-energy model).
            if daemon_client.is_none() {
                match daemon::connect_rpc(cfg.daemon_rpc_addr, &cfg.daemon_auth, cfg.jsonrpc_timeout)
                {
                    Ok(client) => daemon_client = Some(client),
                    Err(e) => warn!("fx: daemon RPC unavailable for cost-floor model: {:#}", e),
                }
            }
            if let Some(client) = &daemon_client {
                match mining_cost_floor(client) {
                    Ok(usd) => {
                        snapshot = Some(RateSnapshot {
                            usd: Some(usd),
                            eur: Some(usd * EUR_PER_USD),
                            updated_unix: unix_now(),
                            source: RateSource::MiningCostFloor,
                            market: None,
                            usd_per_btc,
                        });
                    }
                    Err(e) => {
                        // Drop the client so a changed/restarted daemon is
                        // re-connecting (not silently stale) next cycle.
                        daemon_client = None;
                        warn!("fx: cost-floor model failed: {:#}", e);
                    }
                }
            }
        }

        if let Some(snapshot) = snapshot {
            info!(
                "fx: rates updated: 1 ELEK ~= {} USD{} ({})",
                fmt_amount(snapshot.usd.unwrap_or(0.0)),
                match snapshot.eur {
                    Some(eur) => format!(" ~= {} EUR", fmt_amount(eur)),
                    None => String::new(),
                },
                snapshot.source.as_str()
            );
            state.set(snapshot);
        } else {
            debug!("fx: no rate source available, keeping last snapshot");
        }

        if let Some(path) = &cfg.snapshot_path {
            if let Err(e) = state.write_mempool_snapshot(path) {
                warn!("fx: {}", e);
            }
        }
        if let Some(path) = &cfg.rates_path {
            if let Err(e) = state.write_rates_json(path) {
                warn!("fx: {}", e);
            }
        }

        std::thread::sleep(cfg.refresh);
    }
}

/// Tickers of the BTC asset family (main-/test-/regnet BTC come as
/// different kdf tickers but share the BTC fiat rate).
fn is_btc_rel(rel: &str) -> bool {
    matches!(rel.to_uppercase().as_str(), "BTC" | "RBTC" | "TBTC")
}

/// mempool's `/api/v1/prices` shape: `{"time":..., "USD":..., "EUR":...,
/// ...}` with -1 for currencies without a live rate (rejected as
/// non-positive, like every other value here).
fn fetch_btc_prices(url: &str) -> Result<(f64, f64)> {
    let response = ureq::get(url)
        .timeout(REGISTRY_HTTP_TIMEOUT)
        .call()
        .context("BTC reference price fetch failed")?;
    let status = response.status();
    if status / 100 != 2 {
        anyhow::bail!("BTC reference price fetch got HTTP {}", status);
    }
    let body = response
        .into_string()
        .context("BTC reference price response is not UTF-8")?;
    let value: Value = serde_json::from_str(&body).context("BTC reference prices are not valid JSON")?;
    let usd = value.get("USD").and_then(f64_from_value);
    let eur = value.get("EUR").and_then(f64_from_value);
    match (usd, eur) {
        (Some(usd), Some(eur)) => Ok((usd, eur)),
        (Some(usd), None) => Ok((usd, usd * EUR_PER_USD)),
        _ => anyhow::bail!("BTC reference prices have no USD rate"),
    }
}

/// POST the market daemon's legacy `orderbook` RPC (fields travel at the
/// body's top level, not inside `params`; the response is
/// `{"asks": [...], "bids": [...], ...}` possibly wrapped in `result`).
fn fetch_orderbook(ob: &ObConfig) -> Result<Option<Market>> {
    let body = serde_json::to_string(&json!({
        "userpass": ob.userpass.0,
        "method": "orderbook",
        "base": ob.base,
        "rel": ob.rel,
    }))?;
    let response = ureq::post(&ob.url)
        .timeout(ORDERBOOK_HTTP_TIMEOUT)
        .set("Content-Type", "application/json")
        .send_string(&body)
        .context("market daemon RPC failed")?;
    let status = response.status();
    if status / 100 != 2 {
        anyhow::bail!("market daemon RPC got HTTP {}", status);
    }
    let text = response
        .into_string()
        .context("market daemon response is not UTF-8")?;
    let value: Value = serde_json::from_str(&text).context("market daemon response is not valid JSON")?;
    parse_orderbook(&value, &format!("{}/{}", ob.base, ob.rel))
}

/// Best prices out of an orderbook payload: `asks`/`bids` rows carry a
/// `price` (JSON number or string). Empty books return Ok(None) - that is an
/// ordinary market state, the lookup falls through to the next source.
fn parse_orderbook(value: &Value, pair: &str) -> Result<Option<Market>> {
    let root = value.get("result").filter(|value| value.is_object()).unwrap_or(value);
    let asks = root
        .get("asks")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("orderbook response has no asks"))?;
    let bids = root
        .get("bids")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("orderbook response has no bids"))?;
    let best_ask = asks.iter().filter_map(row_price).reduce(f64::min);
    let best_bid = bids.iter().filter_map(row_price).reduce(f64::max);
    if best_ask.is_none() && best_bid.is_none() {
        return Ok(None);
    }
    Ok(Some(Market {
        pair: pair.to_owned(),
        best_bid,
        best_ask,
        mid: best_bid
            .zip(best_ask)
            .map(|(bid, ask)| (bid + ask) / 2.0)
            .or(best_ask)
            .or(best_bid),
        asks: asks.len(),
        bids: bids.len(),
    }))
}

/// Row price as number-or-string; non-positive rows are ignored.
fn row_price(row: &Value) -> Option<f64> {
    row.get("price").and_then(f64_from_value)
}

/// Convert a book mid into (usd, eur). Known rels: USD/USDT (the price is
/// already fiat), BTC (multiplied by the fetched BTC reference prices).
fn market_rate(market: &Market, rel: &str, btc: Option<&(f64, f64)>) -> Result<(f64, f64)> {
    let price = market.mid.ok_or_else(|| anyhow::anyhow!("order book is empty"))?;
    if !price.is_finite() || price <= 0.0 {
        anyhow::bail!("nonsensical order book price: {}", price);
    }
    match rel.to_uppercase().as_str() {
        "USD" | "USDT" => Ok((price, price * EUR_PER_USD)),
        // Regtest/testnet BTC tickers share the BTC fiat rate.
        "BTC" | "RBTC" | "TBTC" => {
            let (per_btc_usd, per_btc_eur) = btc.context("no BTC reference prices fetched")?;
            Ok((price * per_btc_usd, price * per_btc_eur))
        }
        other => anyhow::bail!("no fiat conversion known for rel {}", other),
    }
}

fn fetch_registry(url: &str) -> Result<(Option<f64>, Option<f64>, Option<u64>)> {
    let response = ureq::get(url)
        .timeout(REGISTRY_HTTP_TIMEOUT)
        .call()
        .context("registry rate fetch failed")?;
    let status = response.status();
    if status / 100 != 2 {
        // Non-2xx (notably 404 while rate.json does not exist yet).
        anyhow::bail!("registry rate fetch got HTTP {}", status);
    }
    let body = response
        .into_string()
        .context("registry rate response is not UTF-8")?;
    parse_registry_rates(&body)
}

/// Parse the registry's rate.json. Accepts both a flat `{"USD":...,"EUR":...}`
/// object and a `{"rates":{...}}` wrapper; an optional top-level
/// `updated_unix` publish time is passed through. Values may be JSON numbers
/// or numeric strings; non-positive/non-finite rates are rejected.
fn parse_registry_rates(body: &str) -> Result<(Option<f64>, Option<f64>, Option<u64>)> {
    let value: Value = serde_json::from_str(body).context("rate.json is not valid JSON")?;
    let rates = value.get("rates").unwrap_or(&value);
    if !rates.is_object() {
        anyhow::bail!("rate.json has no rates object");
    }
    let usd = rates.get("USD").and_then(f64_from_value);
    let eur = rates.get("EUR").and_then(f64_from_value);
    if usd.is_none() && eur.is_none() {
        anyhow::bail!("rate.json contains no USD/EUR rates");
    }
    let published = value.get("updated_unix").and_then(Value::as_u64);
    Ok((usd, eur, published))
}

fn f64_from_value(value: &Value) -> Option<f64> {
    let rate = match value {
        Value::Number(number) => number.as_f64(),
        Value::String(string) => string.trim().parse().ok(),
        _ => None,
    };
    rate.filter(|rate| rate.is_finite() && *rate > 0.0)
}

/// Mining cost-floor fallback via a daemon RPC of our own (the main `Daemon`
/// object is owned by the electrum `Rpc`; a second client avoids any lock
/// contention and re-connects on its own terms).
fn mining_cost_floor(client: &Client) -> Result<f64> {
    let hashrate: f64 = client
        .call("getnetworkhashps", &[json!(120u32)])
        .context("getnetworkhashps failed")?;
    if !hashrate.is_finite() || hashrate <= 0.0 {
        anyhow::bail!("nonsensical network hashrate: {}", hashrate);
    }
    Ok(mining_cost_floor_usd_per_elek(hashrate))
}

/// Pure cost-floor computation: energy cost per day / mined coins per day.
/// 2026-09-21 ground truth for this network: ~8 TH/s total (one Bitaxe Gamma
/// at 1.07 TH/s finds ~12% of blocks) => ~360 W network-wide => ~$2.59/day at
/// $0.30/kWh against 7200 mined ELEK/day (~$0.00036/ELEK). This is a *floor*,
/// labeled as such everywhere it is rendered.
fn mining_cost_floor_usd_per_elek(hashrate_hs: f64) -> f64 {
    let network_power_w = hashrate_hs * (J_PER_TERAHASH / 1e12);
    let daily_cost_usd = network_power_w / 1000.0 * 24.0 * USD_PER_KWH;
    let daily_coins = SUBSIDY_ELEK * 86400.0 / BLOCK_INTERVAL_SECS;
    daily_cost_usd / daily_coins
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before 1970")
        .as_secs()
}

/// Compact decimal rendering for small rates: up to 10 fractional digits,
/// trailing zeros trimmed ("0.25", "0.00036", "1").
fn fmt_amount(value: f64) -> String {
    let formatted = format!("{:.10}", value);
    let formatted = formatted.trim_end_matches('0').trim_end_matches('.');
    formatted.to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_registry_flat() {
        let (usd, eur, published) = parse_registry_rates(r#"{"USD": 0.25, "EUR": 0.21}"#).unwrap();
        assert_eq!(usd, Some(0.25));
        assert_eq!(eur, Some(0.21));
        assert_eq!(published, None);
    }

    #[test]
    fn test_parse_registry_wrapped() {
        let (usd, eur, published) = parse_registry_rates(
            r#"{"ticker": "ELEK", "updated_unix": 1758421200, "rates": {"USD": 1.5}}"#,
        )
        .unwrap();
        assert_eq!(usd, Some(1.5));
        assert_eq!(eur, None);
        assert_eq!(published, Some(1758421200));
    }

    #[test]
    fn test_parse_registry_string_values_and_rejects() {
        let (usd, _, _) = parse_registry_rates(r#"{"rates": {"USD": "0.5"}}"#).unwrap();
        assert_eq!(usd, Some(0.5));
        // Non-positive rates are filtered, so no USD/EUR remains -> error.
        assert!(parse_registry_rates(r#"{"rates": {"USD": -1}}"#).is_err());
        assert!(parse_registry_rates(r#"{"rates": {"GBP": 0.2}}"#).is_err());
        assert!(parse_registry_rates(r#"not json"#).is_err());
    }

    #[test]
    fn test_mempool_prices_shape() {
        let state = RateState::new();
        let prices = state.mempool_prices_json();
        assert_eq!(prices["time"], 0); // nothing available yet -> disabled form
        assert_eq!(prices["USD"], -1.0);
        assert_eq!(prices["EUR"], -1.0);
        assert_eq!(prices["ZAR"], -1.0);

        state.set(RateSnapshot {
            usd: Some(0.25),
            eur: Some(0.23),
            updated_unix: 1758421200,
            source: RateSource::Registry,
            market: None,
            usd_per_btc: None,
        });
        let prices = state.mempool_prices_json();
        assert_eq!(prices["time"], 1_758_421_200);
        assert_eq!(prices["USD"], 0.25);
        assert_eq!(prices["EUR"], 0.23);
        assert_eq!(prices["GBP"], -1.0);
    }

    #[test]
    fn test_banner_labels_sources() {
        let state = RateState::new();
        assert!(state.banner_line().contains("no market exists"));

        state.set(RateSnapshot {
            usd: Some(0.25),
            eur: Some(0.23),
            updated_unix: 1758421200,
            source: RateSource::Registry,
            market: None,
            usd_per_btc: None,
        });
        let line = state.banner_line();
        assert!(line.contains("1 ELEK ~= $0.25"));
        assert!(line.contains("0.23 EUR"));
        assert!(line.contains("registry reference rate"));

        state.set(RateSnapshot {
            usd: Some(0.00036),
            eur: Some(0.00033),
            updated_unix: 1758421200,
            source: RateSource::MiningCostFloor,
            market: None,
            usd_per_btc: None,
        });
        let line = state.banner_line();
        assert!(line.contains("$0.00036"));
        assert!(line.contains("cost-floor"));
    }

    #[test]
    fn test_banner_labels_p2p_market_pair() {
        let state = RateState::new();
        state.set(RateSnapshot {
            // coherent with the book: 0.0008 BTC x 65_000 USD/BTC = $52
            usd: Some(52.0),
            eur: Some(47.84),
            updated_unix: 1758421200,
            source: RateSource::P2pMarket,
            market: Some(Market {
                pair: "ELEK/BTC".to_owned(),
                best_bid: None,
                best_ask: Some(0.0008),
                mid: Some(0.0008),
                asks: 1,
                bids: 0,
            }),
            usd_per_btc: Some(65_000.0),
        });
        let line = state.banner_line();
        assert!(line.contains("live P2P order book"));
        assert!(line.contains("ELEK/BTC"));
        assert!(line.contains("$52"));
        let rpc = state.rpc_json();
        assert_eq!(rpc["source"], "p2p_market");
        assert_eq!(rpc["usd_per_btc"], 65_000.0);
        assert_eq!(rpc["market"]["pair"], "ELEK/BTC");
        assert_eq!(rpc["market"]["asks"], 1);
        // The mempool shape stays a pure fiat list: no market metadata.
        assert_eq!(state.mempool_prices_json()["USD"], 52.0);
    }

    #[test]
    fn test_parse_orderbook_both_sides() {
        let value: Value = serde_json::from_str(
            r#"{"asks":[{"price":"0.0012"},{"price":0.001}],"bids":[{"price":"0.0011"}]}"#,
        )
        .unwrap();
        let market = parse_orderbook(&value, "ELEK/BTC").unwrap().unwrap();
        assert_eq!(market.pair, "ELEK/BTC");
        assert_eq!(market.best_ask, Some(0.001));
        assert_eq!(market.best_bid, Some(0.0011));
        assert_eq!(market.mid, Some((0.001 + 0.0011) / 2.0));
        assert_eq!(market.asks, 2);
        assert_eq!(market.bids, 1);
    }

    #[test]
    fn test_parse_orderbook_one_sided_and_empty() {
        let value: Value = serde_json::from_str(r#"{"asks":[{"price":"0.0012"}],"bids":[]}"#)
            .unwrap();
        let market = parse_orderbook(&value, "ELEK/rBTC").unwrap().unwrap();
        assert_eq!(market.best_ask, Some(0.0012));
        assert_eq!(market.mid, Some(0.0012));

        // An empty book is an ordinary state, signaled as None - not an error.
        let value: Value = serde_json::from_str(r#"{"asks":[],"bids":[]}"#).unwrap();
        assert!(parse_orderbook(&value, "ELEK/rBTC").unwrap().is_none());

        // Rows without a usable price count as empty, too.
        let value: Value = serde_json::from_str(r#"{"asks":[{"price":-1}],"bids":[{"other":1}]}"#)
            .unwrap();
        assert!(parse_orderbook(&value, "ELEK/rBTC").unwrap().is_none());
    }

    #[test]
    fn test_parse_orderbook_result_wrapper() {
        let value: Value =
            serde_json::from_str(r#"{"result":{"asks":[{"price":2}],"bids":[]}}"#).unwrap();
        let market = parse_orderbook(&value, "a/b").unwrap().unwrap();
        assert_eq!(market.mid, Some(2.0));
    }

    #[test]
    fn test_market_rate_rels() {
        let market = Market {
            pair: "ELEK/USDT".to_owned(),
            best_bid: None,
            best_ask: Some(0.5),
            mid: Some(0.5),
            asks: 1,
            bids: 0,
        };
        let (usd, eur) = market_rate(&market, "USDT", None).unwrap();
        assert_eq!(usd, 0.5);
        assert_eq!(eur, 0.5 * EUR_PER_USD);

        let market = Market {
            pair: "ELEK/BTC".to_owned(),
            best_bid: Some(0.0001),
            best_ask: Some(0.0002),
            mid: Some(0.00015),
            asks: 1,
            bids: 1,
        };
        let (usd, eur) =
            market_rate(&market, "BTC", Some(&(60_000.0, 55_000.0))).unwrap();
        assert_eq!(usd, 9.0);
        assert_eq!(eur, 8.25);
        // Regtest/testnet tickers of the BTC family use the same rate.
        let (usd, eur) =
            market_rate(&market, "rBTC", Some(&(60_000.0, 55_000.0))).unwrap();
        assert_eq!(usd, 9.0);
        assert_eq!(eur, 8.25);

        // No BTC reference prices - or an exotic rel - fall through to the
        // next source instead of inventing a rate.
        assert!(market_rate(&market, "BTC", None).is_err());
        assert!(market_rate(&market, "XMR", Some(&(1.0, 1.0))).is_err());
    }

    #[test]
    fn test_mining_cost_floor() {
        // 2026-09-21 measured reality: ~8e12 H/s network-wide.
        // 8e12 H/s * 45 J/TH = 360 W; 0.36 kW * 24 h * $0.30 = $2.592/day;
        // 7200 ELEK/day => $0.00036/ELEK.
        let usd = mining_cost_floor_usd_per_elek(8e12);
        assert!((usd - 0.00036).abs() < 1e-12);
    }

    #[test]
    fn test_fmt_amount() {
        assert_eq!(fmt_amount(0.25), "0.25");
        assert_eq!(fmt_amount(0.00036), "0.00036");
        assert_eq!(fmt_amount(1.0), "1");
        assert_eq!(fmt_amount(12.345), "12.345");
    }
}