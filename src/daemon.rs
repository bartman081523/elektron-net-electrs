use anyhow::{Context, Result};

use bitcoin::{consensus::deserialize, hashes::hex::FromHex};
use bitcoin::{Amount, BlockHash, Transaction, Txid};
use bitcoincore_rpc::{json, jsonrpc, Auth, Client, RpcApi};
use crossbeam_channel::Receiver;
use parking_lot::Mutex;
use serde::Serialize;
use serde_json::{json, value::RawValue, Value};

use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use crate::{
    chain::{Chain, NewHeader},
    config::{Config, SensitiveAuth},
    metrics::Metrics,
    p2p::Connection,
    signals::ExitFlag,
    types::SerBlock,
};

enum PollResult {
    Done(Result<()>),
    Retry,
}

/// Minimal `getblockchaininfo` response - queried directly instead of through
/// bitcoincore-rpc's typed `get_blockchain_info()` helper. That helper first
/// fetches the daemon version and, for anything reporting < 0.19 (190000),
/// takes a legacy compatibility path that expects the pre-0.19
/// `softforks`/`bip9_softforks` fields and fails with "the JSON result had an
/// unexpected structure" when they're absent. Elektron Net restarted its
/// version scheme at 4.0.x (CLIENT_VERSION ~40002), so elektrond always
/// triggers that legacy path even though its RPC output is modern.
#[derive(Deserialize)]
struct BlockchainInfo {
    blocks: u64,
    headers: u64,
    #[serde(rename = "initialblockdownload")]
    initial_block_download: bool,
    pruned: bool,
    // Absent when `pruned` is false.
    #[serde(default)]
    pruneheight: Option<u32>,
}

fn get_blockchain_info(client: &Client) -> bitcoincore_rpc::Result<BlockchainInfo> {
    client.call("getblockchaininfo", &[])
}

fn rpc_poll(client: &mut Client, skip_block_download_wait: bool) -> PollResult {
    match get_blockchain_info(client) {
        Ok(info) => {
            if skip_block_download_wait {
                // bitcoind RPC is available, don't wait for block download to finish
                return PollResult::Done(Ok(()));
            }
            let left_blocks = info.headers - info.blocks;
            if info.initial_block_download || left_blocks > 0 {
                info!(
                    "waiting for {} blocks to download{}",
                    left_blocks,
                    if info.initial_block_download {
                        " (IBD)"
                    } else {
                        ""
                    }
                );
                return PollResult::Retry;
            }
            PollResult::Done(Ok(()))
        }
        Err(err) => {
            if let Some(e) = extract_bitcoind_error(&err) {
                if e.code == -28 {
                    debug!("waiting for RPC warmup: {}", e.message);
                    return PollResult::Retry;
                }
            }
            PollResult::Done(Err(err).context("daemon not available"))
        }
    }
}

fn read_cookie(path: &Path) -> Result<(String, String)> {
    // Load username and password from bitcoind cookie file:
    // * https://github.com/bitcoin/bitcoin/pull/6388/commits/71cbeaad9a929ba6a7b62d9b37a09b214ae00c1a
    // * https://bitcoin.stackexchange.com/questions/46782/rpc-cookie-authentication
    let mut file = File::open(path)
        .with_context(|| format!("failed to open bitcoind cookie file: {}", path.display()))?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .with_context(|| format!("failed to read bitcoind cookie from {}", path.display()))?;

    let parts: Vec<&str> = contents.splitn(2, ':').collect();
    ensure!(
        parts.len() == 2,
        "failed to parse bitcoind cookie - missing ':' separator"
    );
    Ok((parts[0].to_owned(), parts[1].to_owned()))
}

fn rpc_connect(config: &Config) -> Result<Client> {
    connect_rpc(
        config.daemon_rpc_addr,
        &config.daemon_auth,
        config.jsonrpc_timeout,
    )
}

/// Direct daemon RPC client build, also used outside the main `Daemon`
/// object: the fx cost-floor fallback gets the network hashrate on its own
/// thread (see src/fx.rs).
pub(crate) fn connect_rpc(
    daemon_rpc_addr: SocketAddr,
    daemon_auth: &SensitiveAuth,
    jsonrpc_timeout: Duration,
) -> Result<Client> {
    let rpc_url = format!("http://{}", daemon_rpc_addr);
    // Allow `wait_for_new_block` to take a bit longer before timing out.
    // See https://github.com/romanz/electrs/issues/495 for more details.
    let builder = jsonrpc::simple_http::SimpleHttpTransport::builder()
        .url(&rpc_url)?
        .timeout(jsonrpc_timeout);
    let builder = match daemon_auth.get_auth() {
        Auth::None => builder,
        Auth::UserPass(user, pass) => builder.auth(user, Some(pass)),
        Auth::CookieFile(path) => {
            let (user, pass) = read_cookie(&path)?;
            builder.auth(user, Some(pass))
        }
    };
    Ok(Client::from_jsonrpc(jsonrpc::Client::with_transport(
        builder.build(),
    )))
}

pub struct Daemon {
    p2p: Mutex<Connection>,
    rpc: Client,
}

impl Daemon {
    pub(crate) fn connect(
        config: &Config,
        exit_flag: &ExitFlag,
        metrics: &Metrics,
    ) -> Result<Self> {
        let mut rpc = rpc_connect(config)?;

        loop {
            exit_flag
                .poll()
                .context("bitcoin RPC polling interrupted")?;
            match rpc_poll(&mut rpc, config.skip_block_download_wait) {
                PollResult::Done(result) => {
                    result.context("bitcoind RPC polling failed")?;
                    break; // on success, finish polling
                }
                PollResult::Retry => {
                    std::thread::sleep(std::time::Duration::from_secs(1)); // wait a bit before polling
                }
            }
        }

        let network_info = rpc.get_network_info()?;
        // Elektron Net restarted the daemon version scheme at 4.0.0 (=40000),
        // far below every Bitcoin Core release number, so upstream's
        // "bitcoind 0.21+" floor (210000) would reject every elektrond. Use a
        // floor that accepts elektrond (>= 4.0.0) and still passes the CI
        // integration test, which runs against a recent Bitcoin Core.
        if network_info.version < 4_00_00 {
            bail!("electrs requires elektrond 4.0+");
        }
        if !network_info.network_active {
            bail!("electrs requires active bitcoind p2p network");
        }
        let info = get_blockchain_info(&rpc)?;
        if info.pruned {
            // Elektron Net enforces mandatory pruning on every node
            // (MandatoryPruneDepth = 197,280 blocks ~ 137 days); a non-pruned
            // node does not exist on this network, so a pruned daemon is the
            // expected steady state, not an error. Block bodies older than
            // the retention window are unavailable by design (headers are
            // always retained).
            info!("connected to a pruned node (mandatory on Elektron Net)");
        }

        let p2p = Mutex::new(Connection::connect(
            config.network,
            config.daemon_p2p_addr,
            metrics,
            config.signet_magic,
        )?);
        Ok(Self { p2p, rpc })
    }

    pub(crate) fn estimate_fee(&self, nblocks: u16) -> Result<Option<Amount>> {
        let res = self.rpc.estimate_smart_fee(nblocks, None);
        if let Err(bitcoincore_rpc::Error::JsonRpc(jsonrpc::Error::Rpc(RpcError {
            code: -32603,
            ..
        }))) = res
        {
            return Ok(None); // don't fail when fee estimation is disabled (e.g. with `-blocksonly=1`)
        }
        Ok(res.context("failed to estimate fee")?.fee_rate)
    }

    pub(crate) fn get_relay_fee(&self) -> Result<Amount> {
        Ok(self
            .rpc
            .get_network_info()
            .context("failed to get relay fee")?
            .relay_fee)
    }

    pub(crate) fn broadcast(&self, tx: &Transaction) -> Result<Txid> {
        self.rpc
            .send_raw_transaction(tx)
            .context("failed to broadcast transaction")
    }

    pub(crate) fn get_transaction_info(
        &self,
        txid: &Txid,
        blockhash: Option<BlockHash>,
    ) -> Result<Value> {
        // No need to parse the resulting JSON, just return it as-is to the client.
        self.rpc
            .call(
                "getrawtransaction",
                &[json!(txid), json!(true), json!(blockhash)],
            )
            .context("failed to get transaction info")
    }

    pub(crate) fn get_transaction_hex(
        &self,
        txid: &Txid,
        blockhash: Option<BlockHash>,
    ) -> Result<Value> {
        use bitcoin::consensus::serde::{hex::Lower, Hex, With};

        let tx = self.get_transaction(txid, blockhash)?;
        #[derive(serde::Serialize)]
        #[serde(transparent)]
        struct TxAsHex(#[serde(with = "With::<Hex<Lower>>")] Transaction);
        serde_json::to_value(TxAsHex(tx)).map_err(Into::into)
    }

    pub(crate) fn get_transaction(
        &self,
        txid: &Txid,
        blockhash: Option<BlockHash>,
    ) -> Result<Transaction> {
        self.rpc
            .get_raw_transaction(txid, blockhash.as_ref())
            .context("failed to get transaction")
    }

    pub(crate) fn get_block_txids(&self, blockhash: BlockHash) -> Result<Vec<Txid>> {
        Ok(self
            .rpc
            .get_block_info(&blockhash)
            .context("failed to get block txids")?
            .tx)
    }

    pub(crate) fn get_mempool_info(&self) -> Result<json::GetMempoolInfoResult> {
        self.rpc
            .get_mempool_info()
            .context("failed to get mempool info")
    }

    pub(crate) fn get_mempool_txids(&self) -> Result<Vec<Txid>> {
        self.rpc
            .get_raw_mempool()
            .context("failed to get mempool txids")
    }

    pub(crate) fn get_mempool_entries(
        &self,
        txids: &[Txid],
    ) -> Result<Vec<Option<json::GetMempoolEntryResult>>> {
        let results = batch_request(self.rpc.get_jsonrpc_client(), "getmempoolentry", txids)?;
        Ok(results
            .into_iter()
            .map(|r| match r?.result::<json::GetMempoolEntryResult>() {
                Ok(entry) => Some(entry),
                Err(err) => {
                    debug!("failed to get mempool entry: {}", err); // probably due to RBF
                    None
                }
            })
            .collect())
    }

    pub(crate) fn get_mempool_transactions(
        &self,
        txids: &[Txid],
    ) -> Result<Vec<Option<Transaction>>> {
        let results = batch_request(self.rpc.get_jsonrpc_client(), "getrawtransaction", txids)?;
        Ok(results
            .into_iter()
            .map(|r| -> Option<Transaction> {
                let tx_hex = match r?.result::<String>() {
                    Ok(tx_hex) => Some(tx_hex),
                    Err(err) => {
                        debug!("failed to get mempool tx: {}", err); // probably due to RBF
                        None
                    }
                }?;
                let tx_bytes = match Vec::from_hex(&tx_hex) {
                    Ok(tx_bytes) => Some(tx_bytes),
                    Err(err) => {
                        warn!("got non-hex transaction {}: {}", tx_hex, err);
                        None
                    }
                }?;
                match deserialize(&tx_bytes) {
                    Ok(tx) => Some(tx),
                    Err(err) => {
                        warn!("got invalid tx {}: {}", tx_hex, err);
                        None
                    }
                }
            })
            .collect())
    }

    pub(crate) fn get_new_headers(&self, chain: &Chain) -> Result<Vec<NewHeader>> {
        self.p2p.lock().get_new_headers(chain)
    }

    /// The daemon's current prune floor (`getblockchaininfo`'s `pruneheight`):
    /// block bodies at or below this height are gone, network-wide. `None`
    /// when the daemon isn't pruned at all (never true on Elektron Net, but
    /// checked rather than assumed).
    pub(crate) fn get_prune_height(&self) -> Result<Option<u32>> {
        let info = get_blockchain_info(&self.rpc).context("getblockchaininfo failed")?;
        Ok(info.pruneheight)
    }

    /// Calls `dumptxoutset` on the daemon to produce a UTXO-snapshot file at
    /// `path` (§3.2 bootstrap, see `doc/utxo-snapshot-bootstrap-plan.md`).
    /// `path` is resolved from the *daemon's* filesystem -- it must be
    /// reachable there for the RPC to succeed, and the same path (or an
    /// equivalent one through a shared mount) must be readable from here
    /// afterwards for `crate::snapshot` to parse the result. Returns the
    /// snapshot's base block height and hash, straight from the RPC
    /// response, for the caller to cross-check against the file's own
    /// metadata header.
    pub(crate) fn dump_txoutset(&self, path: &Path) -> Result<(u32, BlockHash)> {
        let path_str = path.to_str().context("non-UTF8 snapshot path")?;
        // "latest": snapshot the UTXO set as of the current tip. The other
        // accepted value, "rollback", would roll the node's state back to
        // an older block first -- not what bootstrap needs, and considerably
        // slower on a real chain.
        let response: Value = self
            .rpc
            .call("dumptxoutset", &[json!(path_str), json!("latest")])
            .context("dumptxoutset RPC failed")?;
        let base_height = response
            .get("base_height")
            .and_then(Value::as_u64)
            .context("dumptxoutset response missing base_height")?;
        let base_height = u32::try_from(base_height).context("base_height out of range")?;
        let base_hash: BlockHash = response
            .get("base_hash")
            .and_then(Value::as_str)
            .context("dumptxoutset response missing base_hash")?
            .parse()
            .context("dumptxoutset response has invalid base_hash")?;
        Ok((base_height, base_hash))
    }

    pub(crate) fn for_blocks<B, F>(&self, blockhashes: B, func: F) -> Result<()>
    where
        B: IntoIterator<Item = BlockHash>,
        F: FnMut(BlockHash, SerBlock),
    {
        self.p2p.lock().for_blocks(blockhashes, func)
    }

    pub(crate) fn new_block_notification(&self) -> Receiver<()> {
        self.p2p.lock().new_block_notification()
    }
}

pub(crate) type RpcError = bitcoincore_rpc::jsonrpc::error::RpcError;

pub(crate) fn extract_bitcoind_error(err: &bitcoincore_rpc::Error) -> Option<&RpcError> {
    use bitcoincore_rpc::{
        jsonrpc::error::Error::Rpc as ServerError, Error::JsonRpc as JsonRpcError,
    };
    match err {
        JsonRpcError(ServerError(e)) => Some(e),
        _ => None,
    }
}

fn batch_request<T>(
    client: &jsonrpc::Client,
    name: &str,
    items: &[T],
) -> Result<Vec<Option<jsonrpc::Response>>>
where
    T: Serialize,
{
    debug!("calling {} on {} items", name, items.len());
    let args: Vec<Box<RawValue>> = items
        .iter()
        .map(|item| jsonrpc::try_arg([item]).context("failed to serialize into JSON"))
        .collect::<Result<Vec<_>>>()?;
    let reqs: Vec<jsonrpc::Request> = args
        .iter()
        .map(|arg| client.build_request(name, Some(arg)))
        .collect();
    match client.send_batch(&reqs) {
        Ok(values) => {
            assert_eq!(items.len(), values.len());
            Ok(values)
        }
        Err(err) => bail!("batch {} request failed: {}", name, err),
    }
}
