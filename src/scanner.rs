use std::collections::HashMap;
use std::io::Cursor;

use anyhow::{Context, Result};
use zcash_client_backend::{TransferType, decrypt_transaction};
use zcash_keys::keys::UnifiedFullViewingKey;
use zcash_primitives::{block::Block, transaction::Transaction};
use zcash_protocol::{
    ShieldedPool,
    consensus::{BlockHeight, BranchId, MainNetwork},
};
use zcash_transparent::keys::IncomingViewingKey;

use crate::{DetectedOutput, Status, ViewKey};

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
            Some(height),
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
    scan_transaction(&tx, view_keys, Status::Mempool, None, chain_tip)
}

fn scan_transaction(
    tx: &Transaction,
    view_keys: &[ViewKey],
    status: Status,
    height: Option<u32>,
    chain_tip: u32,
) -> Result<Vec<DetectedOutput>> {
    if view_keys.is_empty() {
        return Ok(Vec::new());
    }

    let ufvks: HashMap<usize, UnifiedFullViewingKey> = view_keys
        .iter()
        .enumerate()
        .map(|(i, vk)| (i, vk.ufvk.clone()))
        .collect();

    let decrypted = decrypt_transaction(
        &MainNetwork,
        height.map(BlockHeight::from),
        Some(BlockHeight::from(chain_tip)),
        tx,
        &ufvks,
    );

    let txid = tx.txid().to_string();
    let blockheight = height.map(i64::from);
    let mut found = Vec::new();

    for output in decrypted.sapling_outputs() {
        if output.transfer_type() != TransferType::Incoming {
            continue;
        }
        if let Some(vk) = view_keys.get(*output.account()) {
            found.push(make_output(
                vk,
                &txid,
                "sapling",
                output.index(),
                output.note().value().inner(),
                output.memo().as_slice(),
                &hex::encode(output.note().recipient().to_bytes()),
                status,
                blockheight,
            ));
        }
    }

    for output in decrypted.orchard_outputs() {
        if output.transfer_type() != TransferType::Incoming {
            continue;
        }
        if let Some(vk) = view_keys.get(*output.account()) {
            found.push(make_output(
                vk,
                &txid,
                shielded_pool_name(output.value_pool()),
                output.index(),
                output.note().0.value().inner(),
                output.memo().as_slice(),
                &hex::encode(output.note().0.recipient().to_raw_address_bytes()),
                status,
                blockheight,
            ));
        }
    }

    for output in decrypted.ironwood_outputs() {
        if output.transfer_type() != TransferType::Incoming {
            continue;
        }
        if let Some(vk) = view_keys.get(*output.account()) {
            found.push(make_output(
                vk,
                &txid,
                shielded_pool_name(output.value_pool()),
                output.index(),
                output.note().0.value().inner(),
                output.memo().as_slice(),
                &hex::encode(output.note().0.recipient().to_raw_address_bytes()),
                status,
                blockheight,
            ));
        }
    }

    // Transparent — match script_pubkey derived from UFVK at index 0.
    // sender_address is empty because the scanner is pure (no RPC).
    if let Some(bundle) = tx.transparent_bundle() {
        for (index, tx_out) in bundle.vout.iter().enumerate() {
            let script = &tx_out.script_pubkey().0.0;
            for vk in view_keys {
                if let Some(our_script) = derive_transparent_script(&vk.ufvk)
                    && script.as_slice() == our_script.as_slice()
                {
                    found.push(DetectedOutput {
                        recipient_address: vk.recipient_address.clone(),
                        reserve_table_name: vk.reserve_table_name.clone(),
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

#[allow(clippy::too_many_arguments)]
fn make_output(
    vk: &ViewKey,
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
        recipient_address: vk.recipient_address.clone(),
        reserve_table_name: vk.reserve_table_name.clone(),
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
