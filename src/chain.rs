use anyhow::{Context, Result};
use jsonrpsee::{
    core::client::ClientT,
    http_client::{HttpClient, HttpClientBuilder},
    rpc_params,
};

/// JSON-RPC client for the Zcash full chain.
#[derive(Clone)]
pub struct Chain(HttpClient);

impl Chain {
    pub fn connect(url: &str) -> Result<Self> {
        Ok(Self(
            HttpClientBuilder::default()
                .build(url)
                .context("failed to build RPC client")?,
        ))
    }

    pub async fn block_count(&self) -> Result<u32> {
        self.0
            .request("getblockcount", rpc_params![])
            .await
            .context("getblockcount failed")
    }

    pub async fn block_hash(&self, height: u32) -> Result<String> {
        self.0
            .request("getblockhash", rpc_params![height])
            .await
            .context("getblockhash failed")
    }

    pub async fn raw_block(&self, hash: &str) -> Result<Vec<u8>> {
        let encoded: String = self
            .0
            .request("getblock", rpc_params![hash, 0])
            .await
            .context("getblock failed")?;
        hex::decode(encoded).context("chain returned invalid block hex")
    }

    pub async fn raw_transaction(&self, txid: &str) -> Result<Vec<u8>> {
        let encoded: String = self
            .0
            .request("getrawtransaction", rpc_params![txid, 0])
            .await
            .context("getrawtransaction failed")?;
        hex::decode(encoded).context("chain returned invalid transaction hex")
    }
}
