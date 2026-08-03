use jsonrpsee::{
    core::client::ClientT,
    http_client::{HttpClient, HttpClientBuilder},
    rpc_params,
};

use crate::error::Result;

/// Thin JSON-RPC client for the Zcash full node.
#[derive(Clone)]
pub struct NodeRpc(HttpClient);

impl NodeRpc {
    pub fn connect(url: &str) -> Result<Self> {
        Ok(Self(HttpClientBuilder::default().build(url)?))
    }

    pub async fn block_count(&self) -> Result<u32> {
        Ok(self.0.request("getblockcount", rpc_params![]).await?)
    }

    pub async fn block_hash(&self, height: u32) -> Result<String> {
        Ok(self.0.request("getblockhash", rpc_params![height]).await?)
    }

    pub async fn raw_block(&self, hash: &str) -> Result<Vec<u8>> {
        let encoded: String = self.0.request("getblock", rpc_params![hash, 0]).await?;
        hex::decode(encoded).map_err(|e| {
            crate::error::Error::Transaction(format!("node returned invalid block hex: {e}"))
        })
    }

    pub async fn raw_transaction(&self, txid: &str) -> Result<Vec<u8>> {
        let encoded: String = self
            .0
            .request("getrawtransaction", rpc_params![txid, 0])
            .await?;
        hex::decode(encoded).map_err(|e| {
            crate::error::Error::Transaction(format!("node returned invalid transaction hex: {e}"))
        })
    }

    pub async fn mempool(&self) -> Result<Vec<String>> {
        Ok(self.0.request("getrawmempool", rpc_params![]).await?)
    }
}
