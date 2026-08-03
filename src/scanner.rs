use std::io::Cursor;

use anyhow::{Context, Result};
use zcash_keys::keys::UnifiedFullViewingKey;
use zcash_primitives::{block::Block, transaction::Transaction};
use zcash_protocol::consensus::{BranchId, MainNetwork};
use zcash_transparent::keys::IncomingViewingKey;

use crate::{DetectedOutput, Status, decrypt::DecryptedShieldedOutput, decrypt::ViewKey};

/// Scans a raw block for outputs decryptable by any view key.
pub fn scan_block(bytes: &[u8], view_keys: &[ViewKey], height: u32) -> Result<Vec<DetectedOutput>> {
    let block = Block::read(Cursor::new(bytes), &MainNetwork).context("failed to parse block")?;
    let (_, transactions) = block.into_parts();
    let mut outputs = Vec::new();
    for tx in transactions {
        outputs.extend(scan_transaction(
            &tx,
            view_keys,
            Status::Confirmed,
            height,
            height,
        )?);
    }
    Ok(outputs)
}

/// Scans a raw mempool transaction for outputs decryptable by any view key.
pub fn scan_mempool_tx(
    bytes: &[u8],
    view_keys: &[ViewKey],
    chain_tip: u32,
) -> Result<Vec<DetectedOutput>> {
    let tx = Transaction::read(Cursor::new(bytes), BranchId::Nu6_3)
        .context("failed to parse mempool transaction")?;
    // "Mempool height" used for trial decryption is chain_tip + 1.
    let effective_height = chain_tip.saturating_add(1);
    scan_transaction(&tx, view_keys, Status::Mempool, effective_height, chain_tip)
}

fn scan_transaction(
    tx: &Transaction,
    view_keys: &[ViewKey],
    status: Status,
    effective_height: u32,
    chain_tip_or_height: u32,
) -> Result<Vec<DetectedOutput>> {
    if view_keys.is_empty() {
        return Ok(Vec::new());
    }

    let txid = tx.txid().to_string();
    let blockheight = if status == Status::Confirmed {
        Some(i64::from(chain_tip_or_height))
    } else {
        None
    };

    let shielded = crate::decrypt::decrypt_transaction(tx, view_keys, effective_height);
    let mut found: Vec<DetectedOutput> = shielded
        .into_iter()
        .map(|output| shielded_to_detected_output(output, &txid, status, blockheight))
        .collect();

    // Transparent — match script_pubkey derived from UFVK at index 0.
    // sender_address is empty because resolving it requires an RPC lookup.
    if let Some(bundle) = tx.transparent_bundle() {
        for (index, tx_out) in bundle.vout.iter().enumerate() {
            let script = &tx_out.script_pubkey().0.0;
            for vk in view_keys {
                if let Some(our_script) = derive_transparent_script(&vk.ufvk)
                    && script.as_slice() == our_script.as_slice()
                {
                    found.push(DetectedOutput {
                        recipient_address: vk.recipient_address.clone(),
                        txid: txid.clone(),
                        pool: "transparent",
                        output_index: index as i32,
                        amount_zats: tx_out.value().into_u64() as i64,
                        sender_address: String::new(),
                        memo: None,
                        status,
                        blockheight,
                    });
                    break;
                }
            }
        }
    }

    Ok(found)
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
        sender_address: output.sender_address,
        memo: std::str::from_utf8(&output.memo).ok().map(str::to_owned),
        status,
        blockheight,
    }
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
