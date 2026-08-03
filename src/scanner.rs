use std::{collections::HashMap, io::Cursor};

use zcash_client_backend::{TransferType, decrypt_transaction};
use zcash_keys::keys::UnifiedFullViewingKey;
use zcash_primitives::{block::Block, transaction::Transaction};
use zcash_protocol::{
    ShieldedPool,
    consensus::{BlockHeight, BranchId, MainNetwork},
};

use crate::{
    error::{Error, Result},
    model::{DetectedOutput, KeySet, Status, ViewKeyConfig},
    rpc::NodeRpc,
};

/// Scans a raw block for outputs decryptable by any active view key.
pub async fn scan_block(
    bytes: &[u8],
    keys: &KeySet,
    height: u32,
    chain_tip: u32,
    rpc: &NodeRpc,
) -> Result<Vec<DetectedOutput>> {
    let block = Block::read(Cursor::new(bytes), &MainNetwork)?;
    let (_, transactions) = block.into_parts();
    let mut outputs = Vec::new();
    for transaction in transactions {
        outputs.extend(
            scan_transaction(
                &transaction,
                keys,
                Status::Confirmed,
                Some(height),
                chain_tip,
                rpc,
            )
            .await?,
        );
    }
    Ok(outputs)
}

/// Scans a raw mempool transaction for outputs decryptable by any active view key.
pub async fn scan_mempool_transaction(
    bytes: &[u8],
    keys: &KeySet,
    chain_tip: u32,
    rpc: &NodeRpc,
) -> Result<Vec<DetectedOutput>> {
    let transaction = Transaction::read(Cursor::new(bytes), BranchId::Nu6_3)
        .map_err(|e| Error::Transaction(e.to_string()))?;
    scan_transaction(&transaction, keys, Status::Mempool, None, chain_tip, rpc).await
}

/// Internal: maps a numeric index to a `ViewKeyConfig` so that `decrypt_transaction`
/// (which requires `AccountId: Copy`) can be called with `usize` keys.
fn build_index(keys: &KeySet) -> (Vec<&ViewKeyConfig>, HashMap<usize, UnifiedFullViewingKey>) {
    let configs: Vec<&ViewKeyConfig> = keys.keys.values().collect();
    let ufvks: HashMap<usize, UnifiedFullViewingKey> = configs
        .iter()
        .enumerate()
        .map(|(i, cfg)| (i, cfg.ufvk.clone()))
        .collect();
    (configs, ufvks)
}

async fn scan_transaction(
    transaction: &Transaction,
    keys: &KeySet,
    status: Status,
    height: Option<u32>,
    chain_tip: u32,
    rpc: &NodeRpc,
) -> Result<Vec<DetectedOutput>> {
    if keys.keys.is_empty() {
        return Ok(Vec::new());
    }

    let (configs, ufvks) = build_index(keys);
    if ufvks.is_empty() {
        return Ok(Vec::new());
    }

    let decrypted = decrypt_transaction(
        &MainNetwork,
        height.map(BlockHeight::from),
        Some(BlockHeight::from(chain_tip)),
        transaction,
        &ufvks,
    );

    let txid = transaction.txid().to_string();
    let blockheight = height.map(i64::from);
    let mut found = Vec::new();

    // Sapling outputs
    for output in decrypted.sapling_outputs() {
        if output.transfer_type() != TransferType::Incoming {
            continue;
        }
        let idx = *output.account();
        if let Some(cfg) = configs.get(idx) {
            let address = hex::encode(output.note().recipient().to_bytes());
            found.push(make_output(
                cfg,
                &txid,
                "sapling",
                output.index(),
                output.note().value().inner(),
                output.memo().as_slice(),
                &address,
                status,
                blockheight,
            ));
        }
    }

    // Orchard outputs
    for output in decrypted.orchard_outputs() {
        if output.transfer_type() != TransferType::Incoming {
            continue;
        }
        let idx = *output.account();
        if let Some(cfg) = configs.get(idx) {
            let address = hex::encode(output.note().0.recipient().to_raw_address_bytes());
            found.push(make_output(
                cfg,
                &txid,
                shielded_pool_name(output.value_pool()),
                output.index(),
                output.note().0.value().inner(),
                output.memo().as_slice(),
                &address,
                status,
                blockheight,
            ));
        }
    }

    // Ironwood outputs
    for output in decrypted.ironwood_outputs() {
        if output.transfer_type() != TransferType::Incoming {
            continue;
        }
        let idx = *output.account();
        if let Some(cfg) = configs.get(idx) {
            let address = hex::encode(output.note().0.recipient().to_raw_address_bytes());
            found.push(make_output(
                cfg,
                &txid,
                shielded_pool_name(output.value_pool()),
                output.index(),
                output.note().0.value().inner(),
                output.memo().as_slice(),
                &address,
                status,
                blockheight,
            ));
        }
    }

    // Transparent outputs — match against script_pubkey derived from UFVK
    if let Some(bundle) = transaction.transparent_bundle() {
        for (index, tx_out) in bundle.vout.iter().enumerate() {
            let script = &tx_out.script_pubkey().0.0;
            for cfg in configs.iter() {
                if let Some(ref our_script) = cfg.transparent_script
                    && script.as_slice() == our_script.as_slice()
                {
                    // Fetch the sender address from the first input's previous output.
                    let sender_address = fetch_sender_address(transaction, rpc).await;
                    found.push(DetectedOutput {
                        recipient_address: cfg.recipient_address.clone(),
                        reserve_table_name: cfg.reserve_table_name.clone(),
                        txid: txid.clone(),
                        pool: "transparent",
                        output_index: index as i32,
                        amount_zats: tx_out.value().into_u64() as i64,
                        sender_address,
                        memo: None,
                        status,
                        blockheight,
                    });
                    break; // one match per output
                }
            }
        }
    }

    Ok(found)
}

/// Fetches the sender address for a transparent transaction by looking up the
/// first input's previous output via the node's JSON-RPC.
async fn fetch_sender_address(transaction: &Transaction, rpc: &NodeRpc) -> String {
    let Some(bundle) = transaction.transparent_bundle() else {
        return String::new();
    };
    if bundle.is_coinbase() {
        return String::new();
    }
    let Some(first_input) = bundle.vin.first() else {
        return String::new();
    };
    let prevout = first_input.prevout();
    let txid_hex = hex::encode(prevout.hash());
    let vout_index = prevout.n();

    // Fetch the previous transaction, parse it, extract the script_pubkey.
    match rpc.raw_transaction(&txid_hex).await {
        Ok(bytes) => match Transaction::read(Cursor::new(bytes), BranchId::Nu6_3) {
            Ok(prev_tx) => {
                if let Some(prev_bundle) = prev_tx.transparent_bundle()
                    && let Some(prev_out) = prev_bundle.vout.get(vout_index as usize)
                {
                    return hex::encode(prev_out.script_pubkey().0.0.as_slice());
                }
            }
            Err(e) => {
                tracing::warn!(%e, %txid_hex, "could not parse previous transaction for sender address")
            }
        },
        Err(e) => {
            tracing::warn!(%e, %txid_hex, "could not fetch previous transaction for sender address")
        }
    }

    String::new()
}

#[allow(clippy::too_many_arguments)]
fn make_output(
    cfg: &ViewKeyConfig,
    txid: &str,
    pool: &'static str,
    index: usize,
    amount_zats: u64,
    memo: &[u8],
    address: &str,
    status: Status,
    blockheight: Option<i64>,
) -> DetectedOutput {
    DetectedOutput {
        recipient_address: cfg.recipient_address.clone(),
        reserve_table_name: cfg.reserve_table_name.clone(),
        txid: txid.to_owned(),
        pool,
        output_index: index as i32,
        amount_zats: amount_zats as i64,
        sender_address: address.to_owned(),
        memo: std::str::from_utf8(memo).ok().map(str::to_owned),
        status,
        blockheight,
    }
}

fn shielded_pool_name(pool: ShieldedPool) -> &'static str {
    match pool {
        ShieldedPool::Sapling => "sapling",
        ShieldedPool::Orchard => "orchard",
        ShieldedPool::Ironwood => "ironwood",
    }
}
