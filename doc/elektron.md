# Elektron Net Fork Notes

This repository is a **permanently-diverged fork** of
[`romanz/electrs`](https://github.com/romanz/electrs), based on **v0.10.10**,
adapted for [Elektron Net](https://github.com/kutlusoy/elektron-net). Upstream
explicitly does not support altcoins, so Elektron-specific changes will never
be upstreamed; the fork picks up upstream protocol/security fixes by manual
porting instead.

v0.10.10 was chosen deliberately over the newer 0.11.x line: in 0.11.x the
indexing internals moved into the external `bindex` crate, while the Elektron
adaptations (pruned-node bootstrap, see below) need to modify exactly those
internals — 0.10.x keeps them all in this repository (`src/index.rs`,
`src/p2p.rs`, `src/db.rs`).

The full design rationale lives in the elektron-net repo:
[`doc-elektron/guideline-electrs-fork-integration.md`](https://github.com/kutlusoy/elektron-net/blob/main/doc-elektron/guideline-electrs-fork-integration.md).
This file documents only what is already implemented here.

## Pruned daemon is the expected steady state

Elektron Net enforces mandatory pruning on every node
(`MandatoryPruneDepth` = 197,280 blocks ≈ 137 days at 60-second blocks).
A non-pruned node does not exist on this network.

Upstream electrs refuses to start against a pruned daemon
(`electrs requires non-pruned bitcoind node`). This fork removes that
hard-fail: a pruned daemon is accepted and logged. Consequences for clients:

- **Headers are always complete.** The daemon retains the full header chain,
  so header subscription, checkpoints, and chain-of-work verification are
  unaffected by pruning.
- **Block bodies older than the retention window are gone** — network-wide,
  by design. Anything that needs the full block (merkle proofs, see below)
  is impossible for those heights; this is not a server malfunction.

## P2P: network magic and protocol version

electrs 0.10.x downloads blocks from the daemon over the Bitcoin P2P
protocol, so two Elektron Net specifics matter here:

- **Network magic.** Upstream only allows overriding the magic on signet;
  this fork allows it on every network (upstream later did the same in
  0.11.1). Running against `elektrond` mainnet requires:

  ```toml
  # electrs.toml
  signet_magic = "e1ec7a6e"   # Elektron Net mainnet magic
  ```

  (The option keeps its upstream name for config compatibility.)

- **Protocol version.** `elektrond` requires peers to advertise protocol
  version ≥ 70017 from genesis and disconnects anything lower. The P2P
  handshake (`src/p2p.rs`) therefore advertises 70017 instead of
  rust-bitcoin's default constant.

- **Genesis block.** `Network::Bitcoin` serves as the internal Elektron
  mainnet stand-in, so `src/chain.rs` seeds the header chain with Elektron
  Net's genesis header (hash
  `00000006b054338443f1a5d5534df21eab0d13232028158ae198edbb169f9dad`, built
  from the chainparams.cpp constants and self-checked at startup) instead of
  rust-bitcoin's Bitcoin genesis, since otherwise no header from `elektrond`
  could ever attach ("missing prev_blockhash"). Consequence: this binary
  cannot index Bitcoin mainnet.

  **Elektron Net regtest** gets the same treatment, but repurposing
  `Network::Testnet` instead of `Network::Regtest`: this fork's own CI
  integration test (`tests/run.sh`) runs electrs against a real, stock
  `bitcoind` in regtest mode, so `Network::Regtest` has to keep meaning
  actual Bitcoin regtest. Elektron's own regtest genesis (hash
  `752eeff40cb87937e666970e00ffdca686f950c5feca8894d7714086848c6d4f`, same
  merkle root as mainnet's genesis but Bitcoin's stock regtest
  timestamp/nonce/bits and a 5 COIN instead of 50 COIN coinbase reward) is
  seeded when `network = "testnet"` is configured. Point electrs at an
  actual Elektron Net node running `-regtest` this way, not because it has
  anything to do with a real testnet. An Elektron *testnet* (the actual
  network, not this stand-in) deployment would still need its own separate
  genesis override added first.

## Typed "block pruned" Electrum error (code 3)

`blockchain.transaction.get_merkle` and `blockchain.transaction.id_from_pos`
need the block's full txid list, fetched from the daemon via RPC. For heights
older than the retention window that call fails on every Elektron Net node,
permanently.

Instead of forwarding the daemon's raw error text (upstream behavior, error
code 2, indistinguishable from any other RPC failure), this fork returns a
stable, typed error:

```json
{"code": 3, "message": "block at height <h> is pruned - unavailable by design (Elektron Net mandatory pruning)"}
```

Wallet clients SHOULD treat code 3 as "proof unavailable by design" (a
permanent property of the requested height, safe to cache) and MUST NOT treat
it as a server error worth retrying or failing over for.

Error codes used by this server:

| Code | Meaning |
|------|---------|
| 1 | bad request (upstream) |
| 2 | daemon RPC error, forwarded (upstream) |
| 3 | block pruned — unavailable by design (this fork) |
| -32700 … -32603 | standard JSON-RPC errors (upstream) |

## Elektron testnet: `Network::Signet` as the stand-in

The Elektron Net chainparams define a distinct **testnet** network
(`CTestNetParams`: its own genesis, P2P magic `0b110907`, target spacing
60 s, coinbase maturity 100, mandatory pruning with a shallower depth). electrs
has a hardwired `Network` enum with no testnet slot free for a new chain:

- `Network::Regtest` already serves as the stand-in for the Elektron
  *regtest* chain (`elektron_regtest_genesis_header()`).
- `Network::Signet` is unused everywhere in this codebase and its CI, so it
  is repurposed as the Elektron-testnet stand-in instead of touching the Rust
  bitcoin crate's enum: this fork adds
  `elektron_testnet_genesis_header()` (genesis time 1781164421, nonce
  7265942, bits `0x1d7fffff`, merkle root
  `0a7087d8…b19b17` — all taken from `CTestNetParams`, asserted against the
  real genesis block hash `00000078233832d6…c73e4f`) and seeds
  `Network::Signet` with it.

Configuration for an Elektron testnet daemon:

```
network = "signet"
signet_magic = "0b110907"   # elektrond testnet P2P magic (CTestNetParams)
```

No utxo snapshot is needed on a young testnet: start electrs while the
chain is still shorter than the daemon's retention window and let it index
from genesis, exactly like a non-pruned network.

Everything Bitcoin-specific on this slot (headers, blocks, scripthash
indexing, electrum protocol) behaves identically to the other slots; the
only Elektron-specific parts are the genesis constants and the magic.

## Not yet implemented (planned, see the integration guideline)

- **§3.2 UTXO-snapshot bootstrap** of the scripthash index on first start
  (from `dumptxoutset` or the node's automatic checkpoint snapshots), with the
  bootstrap height recorded as the index's effective genesis. Until this
  lands, the index can only be built by syncing blocks that are still within
  the daemon's retention window.
- **§3.3 network identity**: mainnet bech32 HRP `be`; base58 prefixes and the
  RPC port already match Bitcoin's and need no override. (electrs itself works
  on scripthashes, so this mainly affects log/debug output.)
