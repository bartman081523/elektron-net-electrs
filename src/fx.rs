//! Elektron Net FX: conversion rates without an exchange.
//!
//! There is no market for ELEK anywhere, so conversion rates are defined by
//! the project rather than discovered from order books. Two sources, tried
//! in this order:
//!
//! 1. `fx_rate_url` - the project registry's `rate.json` (published reference
//!    rate). The expected shape is flexible: either a flat
//!    `{"USD": ..., "EUR": ...}` object or a `{"rates": {...}}` wrapper; an
//!    optional top-level `updated_unix` (publish time) is honored when
//!    present.
//! 2. A mining cost-floor model, used as fallback when the registry file is
//!    unavailable (e.g. until the first `rate.json` lands): the energy cost
//!    of mining 1 ELEK at the current network hashrate (`getnetworkhashps`
//!    on the local daemon). This is an *honest lower bound* - a market, if
//!    one ever emerges, can be far above it - and every rate rendered from
//!    it is labeled as a cost-floor, not as a market price.
//!
//! The resulting snapshot is exposed three ways (per user decision):
//!   * a dynamic line appended to the Electrum console banner,
//!   * the "blockchain.fx.rates" Electrum RPC (added to `electrum.rs`),
//!   * a JSON file in mempool's /api/v1/prices shape (`fx_snapshot_path`),
//!     so the elektron-net mempool fork can consume rates without any
//!     exchange integration of its own.
//!
//! All fetching happens on one background thread (`crate::thread::spawn`),
//! matching electrs' synchronous thread/channel architecture. Failures are
//! logged and tolerated: the last known-good snapshot is kept, and the
//! thread only ever stops with the process.

use anyhow::{Context, Result};
use parking_lot::RwLock;
use serde_json::{json, Map, Value};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateSource {
    Registry,
    MiningCostFloor,
    None,
}

impl RateSource {
    fn as_str(&self) -> &'static str {
        match self {
            RateSource::Registry => "registry",
            RateSource::MiningCostFloor => "mining_cost_floor",
            RateSource::None => "none",
        }
    }

    // Used in the banner line: an explicit human-readable label so a
    // cost-floor rate can never be mistaken for a market price.
    fn banner_label(&self) -> &'static str {
        match self {
            RateSource::Registry => "project registry reference rate",
            RateSource::MiningCostFloor => "mining cost-floor, no market exists",
            RateSource::None => "no source available",
        }
    }
}

#[derive(Debug, Clone)]
pub struct RateSnapshot {
    pub usd: Option<f64>,
    pub eur: Option<f64>,
    /// When this snapshot was fetched/computed (or published, per the
    /// registry's own `updated_unix` if provided).
    pub updated_unix: u64,
    pub source: RateSource,
}

impl RateSnapshot {
    fn unavailable() -> Self {
        Self {
            usd: None,
            eur: None,
            updated_unix: 0,
            source: RateSource::None,
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
                let mut line = format!("1 ELEK ~= ${} USD", fmt_amount(usd));
                if let Some(eur) = snapshot.eur {
                    line.push_str(&format!(" ~= {} EUR", fmt_amount(eur)));
                }
                line.push_str(&format!(" ({})", snapshot.source.banner_label()));
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
        let body = serde_json::to_string_pretty(&self.mempool_prices_json())?;
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, body)
            .with_context(|| format!("failed to write fx snapshot {}", tmp.display()))?;
        std::fs::rename(&tmp, path)
            .with_context(|| format!("failed to move fx snapshot to {}", path.display()))?;
        Ok(())
    }
}

// Currency list of mempool's /api/v1/prices endpoint; USD is live here, all
// others are rendered as -1 by mempool_prices_json.
const MEMPOOL_CURRENCIES: [&str; 29] = [
    "USD", "EUR", "GBP", "CAD", "AUD", "IDR", "NZD", "SGD", "BRL", "CLP", "CNY", "KRW", "RUB",
    "JPY", "CHF", "NOK", "ISK", "TWD", "DKK", "PLN", "SEK", "ILS", "ARS", "VND", "TRY", "HKD",
    "MXN", "INR", "ZAR",
];

/// FX-only projection of `Config`, moveable into a thread (avoids putting
/// `Clone` on the whole `Config`, which holds sensitive auth state).
pub(crate) struct FxConfig {
    rate_url: Option<String>,
    refresh: Duration,
    snapshot_path: Option<PathBuf>,
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
        Self {
            rate_url,
            refresh: config.fx_refresh.max(Duration::from_secs(1)),
            snapshot_path: config.fx_snapshot_path.clone(),
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

/// Fetch-and-refresh loop. Never returns; per-cycle failures (registry
/// unreachable, daemon RPC down) are logged and retried on the next cycle.
fn fetch_loop(cfg: FxConfig, state: Arc<RateState>) -> Result<()> {
    info!(
        "fx: rates thread started (registry: {:?}, refresh: {:?}, snapshot: {:?})",
        cfg.rate_url, cfg.refresh, cfg.snapshot_path
    );
    let mut daemon_client: Option<Client> = None;
    loop {
        let mut snapshot = None;

        if let Some(url) = &cfg.rate_url {
            match fetch_registry(url) {
                Ok((usd, eur, published)) => {
                    debug!("fx: registry rates fetched");
                    snapshot = Some(RateSnapshot {
                        usd,
                        eur: eur.or(usd.map(|usd| usd * EUR_PER_USD)),
                        updated_unix: published.unwrap_or_else(unix_now),
                        source: RateSource::Registry,
                    });
                }
                // Expected until the registry's rate.json exists (HTTP 404),
                // so log at debug only - the per-cycle retry must not spam
                // the journal, while RUST_LOG=debug still shows every attempt.
                Err(e) => debug!("fx: registry rates unavailable: {:#}", e),
            }
        }

        if snapshot.is_none() {
            // Cost-floor fallback (user-selected combination: registry first,
            // mining-energy model when the registry is unreachable).
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

        std::thread::sleep(cfg.refresh);
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
        });
        let line = state.banner_line();
        assert!(line.contains("$0.00036"));
        assert!(line.contains("cost-floor"));
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