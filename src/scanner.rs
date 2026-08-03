use std::io::Cursor;

use anyhow::{Context, Result};
use zcash_keys::keys::UnifiedFullViewingKey;
use zcash_primitives::{block::Block, transaction::Transaction};
use zcash_protocol::consensus::{BranchId, MainNetwork};
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
    pub pool: &'static str,
    pub output_index: i32,
    pub amount_zats: i64,
    pub memo: Option<String>,
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
    found
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
    for (output_index, tx_out) in bundle.vout.iter().enumerate() {
        let script = &tx_out.script_pubkey().0.0;
        for (recipient_address, our_script) in t_index {
            if script.as_slice() == our_script.as_slice() {
                found.push(DetectedOutput {
                    recipient_address: recipient_address.clone(),
                    txid: txid.to_owned(),
                    pool: "transparent",
                    output_index: output_index as i32,
                    amount_zats: tx_out.value().into_u64() as i64,
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
        pool: output.pool,
        output_index: output.output_index as i32,
        amount_zats: output.amount_zats as i64,
        memo: std::str::from_utf8(&output.memo).ok().map(str::to_owned),
        status,
        blockheight,
    }
}

/// Pre-derived transparent scripts for each view key, used for fast matching.
fn transparent_index(view_keys: &[Wallet]) -> Vec<(String, Vec<u8>)> {
    view_keys
        .iter()
        .filter_map(|vk| {
            derive_transparent_script(&vk.ufvk).map(|script| (vk.recipient_address.clone(), script))
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
