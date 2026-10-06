use anyhow::{Context, Result};
use bitcoin::consensus::{deserialize, Decodable, Encodable};
use bitcoin::hashes::Hash;
use bitcoin::{BlockHash, OutPoint, Txid};
use bitcoin_slices::{bsl, Visit, Visitor};
use std::fs::File;
use std::io::BufReader;
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::thread;

use crate::{
    chain::{Chain, NewHeader},
    daemon::Daemon,
    db::{DBStore, WriteBatch},
    metrics::{self, Gauge, Histogram, Metrics},
    signals::ExitFlag,
    snapshot,
    types::{
        bsl_txid, HashPrefixRow, HeaderRow, ScriptHash, ScriptHashRow, SerBlock,
        SnapshotUnspentRow, SpendingPrefixRow, TxidRow,
    },
};

#[derive(Clone)]
struct Stats {
    update_duration: Histogram,
    update_size: Histogram,
    height: Gauge,
    db_properties: Gauge,
}

impl Stats {
    fn new(metrics: &Metrics) -> Self {
        Self {
            update_duration: metrics.histogram_vec(
                "index_update_duration",
                "Index update duration (in seconds)",
                "step",
                metrics::default_duration_buckets(),
            ),
            update_size: metrics.histogram_vec(
                "index_update_size",
                "Index update size (in bytes)",
                "step",
                metrics::default_size_buckets(),
            ),
            height: metrics.gauge("index_height", "Indexed block height", "type"),
            db_properties: metrics.gauge("index_db_properties", "Index DB properties", "name"),
        }
    }

    fn observe_duration<T>(&self, label: &str, f: impl FnOnce() -> T) -> T {
        self.update_duration.observe_duration(label, f)
    }

    fn observe_size<const N: usize>(&self, label: &str, rows: &[[u8; N]]) {
        self.update_size.observe(label, (rows.len() * N) as f64);
    }

    fn observe_batch(&self, batch: &WriteBatch) {
        self.observe_size("write_funding_rows", &batch.funding_rows);
        self.observe_size("write_spending_rows", &batch.spending_rows);
        self.observe_size("write_txid_rows", &batch.txid_rows);
        self.observe_size("write_header_rows", &batch.header_rows);
        debug!(
            "writing {} funding and {} spending rows from {} transactions, {} blocks",
            batch.funding_rows.len(),
            batch.spending_rows.len(),
            batch.txid_rows.len(),
            batch.header_rows.len()
        );
    }

    fn observe_chain(&self, chain: &Chain) {
        self.height.set("tip", chain.height() as f64);
    }

    fn observe_db(&self, store: &DBStore) {
        for (cf, name, value) in store.get_properties() {
            self.db_properties
                .set(&format!("{}:{}", name, cf), value as f64);
        }
    }
}

/// Confirmed transactions' address index
pub struct Index {
    store: DBStore,
    batch_size: usize,
    lookup_limit: Option<usize>,
    chain: Chain,
    stats: Stats,
    is_ready: bool,
    flush_needed: bool,
    // §3.2 UTXO-snapshot bootstrap (see doc/utxo-snapshot-bootstrap-plan.md).
    // `None` means "sync from genesis as usual", the safe default.
    utxo_snapshot_dir: Option<PathBuf>,
    network_magic: [u8; 4],
}

impl Index {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn load(
        store: DBStore,
        mut chain: Chain,
        metrics: &Metrics,
        batch_size: usize,
        lookup_limit: Option<usize>,
        reindex_last_blocks: usize,
        utxo_snapshot_dir: Option<PathBuf>,
        network_magic: [u8; 4],
    ) -> Result<Self> {
        if let Some(row) = store.get_tip() {
            let tip = deserialize(&row).expect("invalid tip");
            let headers = store
                .iter_headers()
                .map(|row| HeaderRow::from_db_row(row).header);
            chain.load(headers, tip);
            chain.drop_last_headers(reindex_last_blocks);
        };
        let stats = Stats::new(metrics);
        stats.observe_chain(&chain);
        stats.observe_db(&store);
        Ok(Index {
            store,
            batch_size,
            lookup_limit,
            chain,
            stats,
            is_ready: false,
            flush_needed: false,
            utxo_snapshot_dir,
            network_magic,
        })
    }

    /// Runs the §3.2 UTXO-snapshot bootstrap exactly once, against a fresh
    /// (never-synced) index: calls `dumptxoutset` on the daemon, parses the
    /// resulting file (`crate::snapshot`), and seeds `SNAPSHOT_UNSPENT_CF`
    /// with every entry. Block bodies at or below the returned height are
    /// never fetched afterwards (see `index_blocks()`) -- only their
    /// headers are, which stay available on a pruned daemon.
    fn bootstrap(&self, daemon: &Daemon) -> Result<()> {
        let dir = self
            .utxo_snapshot_dir
            .as_ref()
            .expect("bootstrap() only called when utxo_snapshot_dir is set");
        let path = dir.join("electrs-bootstrap.dat");

        // dumptxoutset refuses to overwrite an existing file. Harmless to
        // find one here: either this is a from-scratch bootstrap and a
        // previous attempt left a stale file behind, or `ensure_no_prune_gap`
        // is re-running the bootstrap and the first run's file is still
        // sitting on the shared mount -- either way we're about to replace
        // it with a fresh one and have no further use for the old contents.
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to remove stale snapshot file {}", path.display())
                })
            }
        }

        info!("starting UTXO-snapshot bootstrap: {}", path.display());
        let (base_height, base_hash) =
            daemon.dump_txoutset(&path).context("dumptxoutset failed")?;

        let file = File::open(&path)
            .with_context(|| format!("failed to open snapshot file {}", path.display()))?;
        let mut reader = BufReader::new(file);
        let metadata = snapshot::SnapshotMetadata::read(&mut reader, self.network_magic)
            .context("failed to read snapshot metadata")?;
        if metadata.base_blockhash != base_hash {
            bail!(
                "snapshot file base blockhash {} does not match dumptxoutset response {}",
                metadata.base_blockhash,
                base_hash
            );
        }

        let coins = snapshot::read_coins(&mut reader, metadata.coins_count)
            .context("failed to read snapshot coins")?;
        let rows: Vec<_> = coins
            .iter()
            .map(|coin| {
                let scripthash = ScriptHash::new(&coin.script_pubkey);
                SnapshotUnspentRow::new(
                    scripthash,
                    coin.outpoint.txid,
                    coin.outpoint.vout,
                    coin.amount.to_sat(),
                    coin.height as usize,
                )
                .to_db_row()
            })
            .collect();

        self.store.write_snapshot_bootstrap(base_height, &rows);
        info!(
            "UTXO-snapshot bootstrap complete: {} coins at height {} ({})",
            rows.len(),
            base_height,
            base_hash
        );
        Ok(())
    }

    /// Guards against a gap opening up between what we've captured (via
    /// bootstrap or ordinary indexing) and what the daemon still has block
    /// bodies for. Left unindexed while electrs is offline (or otherwise
    /// falls behind), heights above our bootstrap height but at or below the
    /// daemon's *current* prune floor have neither a snapshot entry nor an
    /// indexed body -- `index_blocks()` would ask the daemon for one of
    /// these over P2P anyway, the daemon replies `notfound`, and `p2p.rs`
    /// treats that as an unsupported message and kills the connection,
    /// crashing electrs. Re-running the §3.2 bootstrap captures the UTXO set
    /// fresh as of *now*, so it always covers the gap regardless of how far
    /// behind we fell (not just the one-checkpoint case).
    fn ensure_no_prune_gap(&self, daemon: &Daemon, next_height: u32) -> Result<()> {
        let prune_height = match daemon
            .get_prune_height()
            .context("get_prune_height failed")?
        {
            Some(prune_height) => prune_height,
            None => return Ok(()), // daemon reports itself as not pruned
        };
        let bootstrap_height = self.store.get_bootstrap_height().unwrap_or(0);
        if prune_height <= bootstrap_height || next_height > prune_height {
            return Ok(()); // still within what we've already captured or indexed
        }
        ensure!(
            self.utxo_snapshot_dir.is_some(),
            "daemon has pruned up to height {prune_height}, past our last covered height \
             {bootstrap_height}, but no utxo_snapshot_dir is configured to re-bootstrap from -- \
             configure it and wipe db_dir to recover"
        );
        warn!(
            "daemon pruned past our last covered height ({bootstrap_height} -> {prune_height}), \
             re-running the UTXO-snapshot bootstrap to close the gap"
        );
        self.bootstrap(daemon)
    }

    pub(crate) fn chain(&self) -> &Chain {
        &self.chain
    }

    /// §3.2 bootstrap-seeded unspent outputs for `scripthash` (empty if no
    /// bootstrap ever ran, or none of its outputs belong to this
    /// scripthash). Each entry is `(outpoint, value in sats, height)`,
    /// self-contained: unlike `filter_by_funding`, resolving these needs no
    /// re-fetch of a historical block, since that data is gone by design
    /// for anything at or below the bootstrap height.
    pub(crate) fn get_snapshot_unspent(
        &self,
        scripthash: ScriptHash,
    ) -> Vec<(OutPoint, u64, usize)> {
        self.store
            .iter_snapshot_unspent(SnapshotUnspentRow::scan_prefix(scripthash))
            .map(|row| {
                let row = SnapshotUnspentRow::from_db_row(row);
                (row.outpoint(), row.value, row.height())
            })
            .collect()
    }

    pub(crate) fn limit_result<T>(&self, entries: impl Iterator<Item = T>) -> Result<Vec<T>> {
        let mut entries = entries.fuse();
        let result: Vec<T> = match self.lookup_limit {
            Some(lookup_limit) => entries.by_ref().take(lookup_limit).collect(),
            None => entries.by_ref().collect(),
        };
        if entries.next().is_some() {
            bail!(">{} index entries, query may take too long", result.len())
        }
        Ok(result)
    }

    pub(crate) fn filter_by_txid(&self, txid: Txid) -> impl Iterator<Item = BlockHash> + '_ {
        self.store
            .iter_txid(TxidRow::scan_prefix(txid))
            .map(|row| HashPrefixRow::from_db_row(row).height())
            .filter_map(move |height| self.chain.get_block_hash(height))
    }

    pub(crate) fn filter_by_funding(
        &self,
        scripthash: ScriptHash,
    ) -> impl Iterator<Item = BlockHash> + '_ {
        self.store
            .iter_funding(ScriptHashRow::scan_prefix(scripthash))
            .map(|row| HashPrefixRow::from_db_row(row).height())
            .filter_map(move |height| self.chain.get_block_hash(height))
    }

    pub(crate) fn filter_by_spending(
        &self,
        outpoint: OutPoint,
    ) -> impl Iterator<Item = BlockHash> + '_ {
        self.store
            .iter_spending(SpendingPrefixRow::scan_prefix(outpoint))
            .map(|row| HashPrefixRow::from_db_row(row).height())
            .filter_map(move |height| self.chain.get_block_hash(height))
    }

    // Return `Ok(true)` when the chain is fully synced and the index is compacted.
    pub(crate) fn sync(&mut self, daemon: &Daemon, exit_flag: &ExitFlag) -> Result<bool> {
        if self.store.get_tip().is_none()
            && self.store.get_bootstrap_height().is_none()
            && self.utxo_snapshot_dir.is_some()
        {
            self.bootstrap(daemon)?;
        }

        let new_headers = self
            .stats
            .observe_duration("headers", || daemon.get_new_headers(&self.chain))?;

        if let Some(first) = new_headers.first() {
            self.ensure_no_prune_gap(daemon, first.height() as u32)?;
        }

        match (new_headers.first(), new_headers.last()) {
            (Some(first), Some(last)) => {
                let count = new_headers.len();
                info!(
                    "indexing {} blocks: [{}..{}]",
                    count,
                    first.height(),
                    last.height()
                );
            }
            _ => {
                if self.flush_needed {
                    self.store.flush(); // full compaction is performed on the first flush call
                    self.flush_needed = false;
                }
                self.is_ready = true;
                return Ok(true); // no more blocks to index (done for now)
            }
        }

        thread::scope(|scope| -> Result<()> {
            let (tx, rx) = crossbeam_channel::bounded(1);

            let chunks = new_headers.chunks(self.batch_size);
            let index = &self; // to be moved into reader thread
            let reader = thread::Builder::new()
                .name("index_build".into())
                .spawn_scoped(scope, move || -> Result<()> {
                    for chunk in chunks {
                        exit_flag.poll().with_context(|| {
                            format!(
                                "indexing interrupted at height: {}",
                                chunk.first().unwrap().height()
                            )
                        })?;
                        let batch = index.index_blocks(daemon, chunk)?;
                        tx.send(batch).context("writer disconnected")?;
                    }
                    Ok(()) // `tx` is dropped, to stop the iteration on `rx`
                })
                .expect("spawn failed");

            let index = &self; // to be moved into writer thread
            let writer = thread::Builder::new()
                .name("index_write".into())
                .spawn_scoped(scope, move || {
                    let stats = &index.stats;
                    for mut batch in rx {
                        stats.observe_duration("sort", || batch.sort()); // pre-sort to optimize DB writes
                        stats.observe_batch(&batch);
                        stats.observe_duration("write", || index.store.write(&batch));
                        stats.observe_db(&index.store);
                    }
                })
                .expect("spawn failed");

            reader.join().expect("reader thread panic")?;
            writer.join().expect("writer thread panic");
            Ok(())
        })?;
        self.chain.update(new_headers);
        self.stats.observe_chain(&self.chain);
        self.flush_needed = true;
        Ok(false) // sync is not done
    }

    fn index_blocks(&self, daemon: &Daemon, chunk: &[NewHeader]) -> Result<WriteBatch> {
        let mut batch = WriteBatch::default();

        // Always set the batch's tip marker from the chunk itself, up
        // front. `index_single_block()` below re-derives (and overwrites)
        // this from each fetched block body as well, which is redundant
        // but harmless when bodies ARE fetched -- what matters is that it's
        // set correctly even when they're not: a chunk entirely at or below
        // the bootstrap height fetches no bodies at all, and would
        // otherwise leave this all-zeros, corrupting HEADERS_CF's TIP_KEY
        // if such a chunk happens to be the last one written.
        let chunk_tip_hash = chunk.last().expect("chunk is never empty").hash();
        let len = chunk_tip_hash
            .consensus_encode(&mut (&mut batch.tip_row as &mut [u8]))
            .expect("in-memory writers don't error");
        debug_assert_eq!(len, BlockHash::LEN);

        // Heights below the daemon's prune floor have no body to fetch --
        // record only the header, straight from the already-fetched
        // `NewHeader` (P2P `getheaders`, body-independent). Heights from the
        // prune floor up -- including anything below the §3.2 bootstrap
        // height that the daemon still retains -- are fetched and indexed
        // normally, so the scripthash history covers the full retention
        // window instead of stopping at the bootstrap height. The snapshot's
        // seeded unspent rows intentionally overlap with the funding rows
        // written here: `Unspent::build` inserts snapshot rows first and
        // folds the ordinary entries over them, keyed by outpoint, so
        // there's no double counting either way.
        let bootstrap_height = self.store.get_bootstrap_height();
        let prune_height = daemon.get_prune_height().ok().flatten();
        let mut to_fetch: Vec<&NewHeader> = Vec::with_capacity(chunk.len());
        for new_header in chunk {
            let body_unavailable = match prune_height {
                // pruneheight = first unpruned block; everything before it is
                // pruned away on this daemon
                Some(p) => (new_header.height() as u32) < p,
                // daemon reports no pruning: keep the old behavior (header
                // rows up to the bootstrap height, bodies from there on)
                None => bootstrap_height
                    .map(|h| new_header.height() as u32 <= h)
                    .unwrap_or(false),
            };
            if body_unavailable {
                batch
                    .header_rows
                    .push(HeaderRow::new(new_header.header()).to_db_row());
            } else {
                to_fetch.push(new_header);
            }
        }

        if to_fetch.is_empty() {
            return Ok(batch);
        }

        let blockhashes: Vec<BlockHash> = to_fetch.iter().map(|h| h.hash()).collect();
        let mut heights = to_fetch.iter().map(|h| h.height());

        daemon.for_blocks(blockhashes, |blockhash, block| {
            let height = heights.next().expect("unexpected block");
            self.stats.observe_duration("block", || {
                index_single_block(blockhash, block, height, &mut batch);
            });
            self.stats.height.set("tip", height as f64);
        })?;
        let heights: Vec<_> = heights.collect();
        assert!(
            heights.is_empty(),
            "some blocks were not indexed: {:?}",
            heights
        );
        Ok(batch)
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.is_ready
    }
}

fn index_single_block(
    block_hash: BlockHash,
    block: SerBlock,
    height: usize,
    batch: &mut WriteBatch,
) {
    struct IndexBlockVisitor<'a> {
        batch: &'a mut WriteBatch,
        height: usize,
    }

    impl Visitor for IndexBlockVisitor<'_> {
        fn visit_transaction(&mut self, tx: &bsl::Transaction) -> ControlFlow<()> {
            let txid = bsl_txid(tx);
            self.batch
                .txid_rows
                .push(TxidRow::row(txid, self.height).to_db_row());
            ControlFlow::Continue(())
        }

        fn visit_tx_out(&mut self, _vout: usize, tx_out: &bsl::TxOut) -> ControlFlow<()> {
            let script = bitcoin::Script::from_bytes(tx_out.script_pubkey());
            // skip indexing unspendable outputs
            if !script.is_op_return() {
                let row = ScriptHashRow::row(ScriptHash::new(script), self.height);
                self.batch.funding_rows.push(row.to_db_row());
            }
            ControlFlow::Continue(())
        }

        fn visit_tx_in(&mut self, _vin: usize, tx_in: &bsl::TxIn) -> ControlFlow<()> {
            let prevout: OutPoint = tx_in.prevout().into();
            // skip indexing coinbase transactions' input
            if !prevout.is_null() {
                let row = SpendingPrefixRow::row(prevout, self.height);
                self.batch.spending_rows.push(row.to_db_row());
            }
            ControlFlow::Continue(())
        }

        fn visit_block_header(&mut self, header: &bsl::BlockHeader) -> ControlFlow<()> {
            let header = bitcoin::block::Header::consensus_decode(&mut header.as_ref())
                .expect("block header was already validated");
            self.batch
                .header_rows
                .push(HeaderRow::new(header).to_db_row());
            ControlFlow::Continue(())
        }
    }

    let mut index_block = IndexBlockVisitor { batch, height };
    bsl::Block::visit(&block, &mut index_block).expect("core returned invalid block");

    let len = block_hash
        .consensus_encode(&mut (&mut batch.tip_row as &mut [u8]))
        .expect("in-memory writers don't error");
    debug_assert_eq!(len, BlockHash::LEN);
}
