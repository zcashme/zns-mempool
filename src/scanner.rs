use std::collections::{HashMap, HashSet};
use std::io::Cursor;

use anyhow::{Context, Result};
use zcash_keys::keys::UnifiedFullViewingKey;
use zcash_primitives::{block::Block, transaction::Transaction};
use zcash_protocol::{
    consensus::{BranchId, MainNetwork},
    memo::MemoBytes,
};
use zcash_transparent::keys::IncomingViewingKey;

use crate::{decrypt::DecryptedShieldedOutput, decrypt::Wallet};

// ── Scanner output types ───────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    Mempool,
    Confirmed,
    Expired,
}

impl Status {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mempool => "mempool",
            Self::Confirmed => "confirmed",
            Self::Expired => "expired",
        }
    }
}

#[derive(Clone, Debug)]
pub struct DetectedOutput {
    pub recipient_address: String,
    pub txid: String,
    pub pool: String,
    pub amount_zats: i64,
    pub is_outgoing: bool,
    pub memo: Option<MemoBytes>,
    pub status: Status,
    pub blockheight: Option<i64>,
}

// ── Public entry points ────────────────────────────────────────────────────

/// Scans a raw block for outputs decryptable by any view key.
pub fn scan_block(bytes: &[u8], view_keys: &[Wallet], height: u32) -> Result<Vec<DetectedOutput>> {
    let block = Block::read(Cursor::new(bytes), &MainNetwork).context("failed to parse block")?;
    let (_, transactions) = block.into_parts();

    let t_index = transparent_index(view_keys);

    let mut outputs = Vec::new();
    for tx in transactions {
        outputs.extend(scan_transaction(
            &tx,
            view_keys,
            &t_index,
            Status::Confirmed,
            height,
            Some(i64::from(height)),
        ));
    }
    Ok(outputs)
}

/// Scans a raw mempool transaction for outputs decryptable by any view key.
pub fn scan_mempool_tx(
    bytes: &[u8],
    view_keys: &[Wallet],
    chain_tip: u32,
) -> Result<Vec<DetectedOutput>> {
    let branch_id = BranchId::for_height(&MainNetwork, chain_tip.into());
    let tx = Transaction::read(Cursor::new(bytes), branch_id)
        .context("failed to parse mempool transaction")?;

    // "Mempool height" used for trial decryption is chain_tip + 1.
    let effective_height = chain_tip.saturating_add(1);
    let t_index = transparent_index(view_keys);

    Ok(scan_transaction(
        &tx,
        view_keys,
        &t_index,
        Status::Mempool,
        effective_height,
        None,
    ))
}

// ── Internal helpers ───────────────────────────────────────────────────────

fn scan_transaction(
    tx: &Transaction,
    view_keys: &[Wallet],
    t_index: &[(String, Vec<u8>)],
    status: Status,
    effective_height: u32,
    blockheight: Option<i64>,
) -> Vec<DetectedOutput> {
    if view_keys.is_empty() {
        return Vec::new();
    }

    let txid = tx.txid().to_string();
    let mut found =
        scan_shielded_outputs(tx, view_keys, effective_height, &txid, status, blockheight);
    found.extend(scan_transparent_outputs(
        tx,
        t_index,
        &txid,
        status,
        blockheight,
    ));
    aggregate_outputs(found)
}

/// Aggregates detected outputs by (recipient_address, txid, is_outgoing),
/// summing amounts and joining pool names with "+".
fn aggregate_outputs(outputs: Vec<DetectedOutput>) -> Vec<DetectedOutput> {
    let mut groups: HashMap<(String, String, bool), Vec<DetectedOutput>> = HashMap::new();
    for output in outputs {
        let key = (output.recipient_address.clone(), output.txid.clone(), output.is_outgoing);
        groups.entry(key).or_default().push(output);
    }

    let mut result = Vec::with_capacity(groups.len());
    for (_, group) in groups {
        let amount_zats: i64 = group.iter().map(|o| o.amount_zats).sum();
        let pools: Vec<String> = {
            let set: HashSet<String> = group.iter().map(|o| o.pool.clone()).collect();
            let mut v: Vec<String> = set.into_iter().collect();
            v.sort();
            v
        };
        let pool = pools.join("+");
        let memo = group.iter().find_map(|o| o.memo.clone());
        let first = group.into_iter().next().unwrap();
        result.push(DetectedOutput {
            recipient_address: first.recipient_address,
            txid: first.txid,
            pool,
            amount_zats,
            is_outgoing: first.is_outgoing,
            memo,
            status: first.status,
            blockheight: first.blockheight,
        });
    }
    result
}

fn scan_shielded_outputs(
    tx: &Transaction,
    view_keys: &[Wallet],
    effective_height: u32,
    txid: &str,
    status: Status,
    blockheight: Option<i64>,
) -> Vec<DetectedOutput> {
    crate::decrypt::decrypt_transaction(tx, view_keys, effective_height)
        .into_iter()
        .map(|output| shielded_to_detected_output(output, txid, status, blockheight))
        .collect()
}

fn scan_transparent_outputs(
    tx: &Transaction,
    t_index: &[(String, Vec<u8>)],
    txid: &str,
    status: Status,
    blockheight: Option<i64>,
) -> Vec<DetectedOutput> {
    let bundle = match tx.transparent_bundle() {
        Some(bundle) => bundle,
        None => return Vec::new(),
    };

    if t_index.is_empty() {
        return Vec::new();
    }

    let mut found = Vec::new();
    for tx_out in bundle.vout.iter() {
        let script = &tx_out.script_pubkey().0.0;
        for (recipient_address, our_script) in t_index {
            if script.as_slice() == our_script.as_slice() {
                found.push(DetectedOutput {
                    recipient_address: recipient_address.clone(),
                    txid: txid.to_owned(),
                    pool: "transparent".to_string(),
                    amount_zats: tx_out.value().into_u64() as i64,
                    is_outgoing: false,
                    memo: None,
                    status,
                    blockheight,
                });
                break;
            }
        }
    }
    found
}

fn shielded_to_detected_output(
    output: DecryptedShieldedOutput,
    txid: &str,
    status: Status,
    blockheight: Option<i64>,
) -> DetectedOutput {
    DetectedOutput {
        recipient_address: output.recipient_address,
        txid: txid.to_owned(),
        pool: output.pool.to_string(),
        amount_zats: output.amount_zats as i64,
        is_outgoing: output.is_outgoing,
        memo: Some(output.memo),
        status,
        blockheight,
    }
}

/// Pre-derived transparent scripts for each view key, used for fast matching.
fn transparent_index(view_keys: &[Wallet]) -> Vec<(String, Vec<u8>)> {
    view_keys
        .iter()
        .filter_map(|wallet| {
            derive_transparent_script(&wallet.ufvk)
                .map(|script| (wallet.recipient_address.clone(), script))
        })
        .collect()
}

fn derive_transparent_script(ufvk: &UnifiedFullViewingKey) -> Option<Vec<u8>> {
    let account_pubkey = ufvk.transparent()?;
    let external_ivk = account_pubkey.derive_external_ivk().ok()?;
    let (address, _index) = external_ivk.default_address();
    Some(transparent_script_bytes(&address))
}

fn transparent_script_bytes(addr: &zcash_transparent::address::TransparentAddress) -> Vec<u8> {
    use zcash_transparent::address::TransparentAddress;
    match addr {
        TransparentAddress::PublicKeyHash(hash) => {
            let mut script = Vec::with_capacity(25);
            script.push(0x76); // OP_DUP
            script.push(0xa9); // OP_HASH160
            script.push(0x14); // push 20 bytes
            script.extend_from_slice(hash);
            script.push(0x88); // OP_EQUALVERIFY
            script.push(0xac); // OP_CHECKSIG
            script
        }
        TransparentAddress::ScriptHash(hash) => {
            let mut script = Vec::with_capacity(23);
            script.push(0xa9); // OP_HASH160
            script.push(0x14); // push 20 bytes
            script.extend_from_slice(hash);
            script.push(0x87); // OP_EQUAL
            script
        }
    }
}
