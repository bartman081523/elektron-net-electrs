# Elektron Net FX: conversion rates without an exchange

There is no exchange for ELEK (both public mempool instances served
`{"time":0,"USD":-1,...}` with the fiat feed disabled exactly for that reason).
A live market *can* exist where trades are possible - the P2P order book on the
project's kdf daemon - so electrs derives conversion rates from a source chain
that prefers that market over project-published references and keeps an honest
energy-cost bound as the last resort. This electrs instance is the **single
point of responsibility** for the rate: elek-web's `/fx` route, the SPA's price
suggestion, the mempool fork and the Electrum wallet consume this node's output
instead of each re-deriving a model of their own.

## Rate sources, tried in order

1. **Live P2P order book** (`fx_orderbook_rpc_url` + `fx_orderbook_userpass`/
   `fx_orderbook_base`/`fx_orderbook_rel`): POST the kdf `orderbook` RPC for
   one pair (e.g. `ELEK`/`BTC`) and take the mid of the best ask/bid;
   one-sided books use their side's best price. An empty book or an
   unreachable daemon is an ordinary state - the lookup falls through.
   - USD/USDT books are used as USD rates directly.
   - BTC-family books (`BTC`, `rBTC`, `tBTC`, case-insensitive) are converted
     with the reference prices from `fx_btc_prices_url` (mempool
     `/api/v1/prices` shape, default `https://mempool.space`; set to `""` to
     disable - a BTC book then cannot produce a rate).
   - Set `fx_orderbook_rpc_url = ""` (the default) to disable this source.
     The `userpass` travels in the JSON-RPC body only and is never logged.

2. **Registry reference rate** (`fx_rate_url`, default
   `https://raw.githubusercontent.com/kutlusoy/elektron-net-registry/main/rate.json`):
   the project's published reference conversion rate. Accepted shapes:
   - flat: `{"USD": 0.25, "EUR": 0.21}`
   - wrapped: `{"ticker": "ELEK", "updated_unix": 1758421200, "rates": {"USD": 0.25}}`
   - values may be JSON numbers or numeric strings; non-positive rates are
     rejected. Missing EUR is derived with an internal EUR/USD factor.

   Set `fx_rate_url = ""` to disable this source.

3. **Mining cost-floor model** (fallback while 1./2. are unavailable):
   the energy cost of mining 1 ELEK at the current network hashrate:
   `getnetworkhashps` (own daemon RPC connection) × assumed 45 J/TH ×
   $0.30/kWh, divided by 5 ELEK mined per 60 s target (= 7200 ELEK/day).
   This is an **honest lower bound**, not a market price - a market can be
   far above it. Every rate from this source is labeled `mining_cost_floor`
   in all outputs.

## Exposure (per decision: banner + RPC + two JSON files)

- **Banner**: a live line appended to the configured `server_banner`,
  e.g. `1 ELEK ~= $0.25 ~= 0.21 EUR (project registry reference rate)` or,
  from a market book, `1 ELEK ~= $8 USD ~= 7.36 EUR (live P2P order book
  (ELEK/BTC))`, served on `server.banner` and the Electrum console. When
  nothing is available, the line says so explicitly instead of showing a
  made-up rate.
- **RPC**: `blockchain.fx.rates` (Electrum RPC, non-standard extension):
  ```json
  {"ticker":"ELEK","usd":0.25,"eur":0.21,"source":"registry","updated_unix":1758421200,"age_secs":42,
   "usd_per_btc":null,"market":null}
  ```
  `age_secs`, `usd_per_btc` and `market` are absent (null) while not
  applicable; `usd_per_btc` carries the BTC reference price so wallet/SPA
  consumers can convert the ELEK rate into BTC denominations.
- **Snapshot file** (`fx_snapshot_path`, optional): written atomically
  (tmp+rename) in **mempool `/api/v1/prices` shape**:
  ```json
  {"time": 1758421200, "USD": 0.25, "EUR": 0.21, "GBP": -1.0, ... "ZAR": -1.0}
  ```
  all listed currencies render as `-1` except USD/EUR; `time: 0` with all
  `-1` means "no rate available" - the same form the mempool instances
  served before, so the elektron-net mempool fork can consume this file
  without any exchange integration.
- **Rich rates file** (`fx_rates_path`, optional): full snapshot with
  source/market metadata, no `-1` padding:
  ```json
  {"ticker":"ELEK","time":1758421200,"usd":8.0,"eur":7.36,"source":"p2p_market",
   "age_secs":42,"usd_per_btc":65000.0,
   "market":{"pair":"ELEK/BTC","best_bid":null,"best_ask":8.0,"mid":8.0,"asks":1,"bids":0}}
  ```
  elek-web serves it same-origin as `/fx/rates.json` (env `MM_WEB_FX_RATES`
  pointing at this file) when set; unset or missing file → HTTP 404 →
  the SPA treats the rate as disabled rather than inventing one.

## Failure behavior

The fetcher thread is deliberately non-fatal: an empty/unreachable order
book, registry errors (notably HTTP 404 while `rate.json` does not exist
yet) and daemon-RPC errors all log at debug/warn and fall through to the
next source (daemon-RPC errors for the cost-floor model reconnect on a
later cycle). The last known-good snapshot is kept until replaced. The
loop runs every `fx_refresh_secs` (default 300) and dies only with the
process.

## Testing locally

`/etc/electrs/fx-test.toml` (not shipped; configure `auth` yourself, the
repo carries no credentials) runs an isolated instance on port 50099 with
its own db_dir and 15 s refresh. To verify the registry source end to end,
serve a fake rate.json and set `fx_rate_url` to its URL:

```console
$ mkdir fx-registry-test && cat > fx-registry-test/rate.json <<'EOF'
{"ticker": "ELEK", "updated_unix": 1790000000,
 "rates": {"USD": 0.25, "EUR": 0.21}}
EOF
$ python3 -m http.server 5098 --bind 127.0.0.1 -d fx-registry-test &
$ electrs --conf fx-test.toml   # then: nc 127.0.0.1 50099, server.banner
```

Expected banner line: `1 ELEK ~= $0.25 USD ~= 0.21 EUR (project registry
reference rate)`. With the default registry URL (rate.json not yet
published) the same instance falls back to the cost-floor rate and labels
it `mining cost-floor, no market exists`.

To verify the P2P market source, point a test config at a running kdf that
has resting orders (e.g. a regtest daemon at 127.0.0.1:7794, pair
`tELEK`/`rBTC`), give it the daemon's `userpass`, and configure
`fx_btc_prices_url` (or use an `ELEK`/`USDT` book instead of conversion).
A fresh `BTC`-denominated book then labels the banner
`live P2P order book (ELEK/BTC)`, and `fx_rates_path` gains `source:
"p2p_market"` with the book's best prices.

## 2026-09-21 ground truth for the cost floor

Network ≈ 8 TH/s total (a single Bitaxe Gamma at 1.07 TH/s finds ~12% of
blocks) → ~360 W network-wide → ~$2.59/day at $0.30/kWh against
7200 mined ELEK/day ⇒ ≈ **$0.00036/ELEK**. Any real reference rate will be
orders of magnitude above this floor; the floor exists so the server can
always render *something* honest rather than nothing.

## Proposed initial registry rate

`rate.json` as prepared in the registry clone's `rate-json` branch and
mirrored at a contributor fork for testing:

```json
{"ticker": "ELEK", "updated_unix": 1790788077,
 "rates": {"USD": 0.25, "EUR": 0.21}}
```

0.25 USD / 0.21 EUR sit roughly 700x above the measured cost floor and
are meant as the *project reference rate*, not a market-derived price;
as designed, the P2P order book (source 1 above) now takes precedence over
this reference file whenever a market exists, with the cost floor remaining
the always-available emergency bound.

Before this lands at the project registry, remember to swap any
consumers configured against the contributor-fork test URL back to the
default URL from `config_specification.toml` (local
`electrs.toml` overrides it explicitly).