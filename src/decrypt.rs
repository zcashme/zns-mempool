use orchard::{
    bundle::{Authorized, Bundle},
    keys::PreparedIncomingViewingKey as PreparedOrchardIvk,
    note_encryption::{DomainVersion, IronwoodVersion, NoteEncryptionDomain, OrchardVersion},
};
use sapling_crypto::{
    bundle::Bundle as SaplingBundle,
    keys::PreparedIncomingViewingKey as PreparedSaplingIvk,
    note_encryption::{SaplingDomain, Zip212Enforcement},
};
use zcash_keys::keys::UnifiedFullViewingKey;
use zcash_note_encryption::try_note_decryption;
use zcash_primitives::transaction::Transaction;
use zcash_protocol::{consensus::MainNetwork, value::ZatBalance};
use zip32::Scope;

/// A shielded output decrypted by one of the scanner's view keys.
///
/// `sender_address` is not populated because an IVK-only scanner cannot
/// determine the external sender of a shielded payment. A sender address may be
/// recoverable for transparent inputs via RPC, but this module does not do that.
#[derive(Clone, Debug)]
pub struct DecryptedShieldedOutput {
    pub recipient_address: String,
    pub pool: &'static str,
    pub output_index: usize,
    pub amount_zats: u64,
    pub memo: [u8; 512],
}

/// A view key loaded from the database, ready for trial decryption.
#[derive(Clone)]
pub struct ViewKey {
    pub ufvk: UnifiedFullViewingKey,
    pub recipient_address: String,
}

/// Trial-decrypts every shielded output in `tx` against the supplied view keys.
///
/// `effective_height` is the block height for confirmed transactions, or
/// `chain_tip + 1` for mempool transactions (matching upstream's "mempool height").
pub fn decrypt_transaction(
    tx: &Transaction,
    view_keys: &[ViewKey],
    effective_height: u32,
) -> Vec<DecryptedShieldedOutput> {
    let mut found = Vec::new();

    if let Some(bundle) = tx.sapling_bundle() {
        decrypt_sapling(bundle, view_keys, effective_height, &mut found);
    }

    if let Some(bundle) = tx.orchard_bundle() {
        decrypt_orchard_pool::<OrchardVersion>(bundle, view_keys, "orchard", &mut found);
    }

    if let Some(bundle) = tx.ironwood_bundle() {
        decrypt_orchard_pool::<IronwoodVersion>(bundle, view_keys, "ironwood", &mut found);
    }

    found
}

fn decrypt_sapling(
    bundle: &SaplingBundle<sapling_crypto::bundle::Authorized, ZatBalance>,
    view_keys: &[ViewKey],
    effective_height: u32,
    found: &mut Vec<DecryptedShieldedOutput>,
) {
    let zip212 = sapling_zip212_enforcement(effective_height);
    let domain = SaplingDomain::new(zip212);
    let ivks = sapling_ivks(view_keys);

    for (index, output) in bundle.shielded_outputs().iter().enumerate() {
        for (recipient_address, ivk) in &ivks {
            if let Some((note, _address, memo)) = try_note_decryption(&domain, ivk, output) {
                found.push(DecryptedShieldedOutput {
                    recipient_address: recipient_address.clone(),
                    pool: "sapling",
                    output_index: index,
                    amount_zats: note.value().inner(),
                    memo,
                });
                break;
            }
        }
    }
}

fn decrypt_orchard_pool<V: DomainVersion>(
    bundle: &Bundle<Authorized, ZatBalance>,
    view_keys: &[ViewKey],
    pool_name: &'static str,
    found: &mut Vec<DecryptedShieldedOutput>,
) {
    let ivks = orchard_ivks(view_keys);

    for (index, action) in bundle.actions().iter().enumerate() {
        let domain = NoteEncryptionDomain::<V>::for_action(action);
        for (recipient_address, ivk) in &ivks {
            if let Some((note, _address, memo)) = try_note_decryption(&domain, ivk, action) {
                found.push(DecryptedShieldedOutput {
                    recipient_address: recipient_address.clone(),
                    pool: pool_name,
                    output_index: index,
                    amount_zats: note.value().inner(),
                    memo,
                });
                break;
            }
        }
    }
}

fn sapling_zip212_enforcement(height: u32) -> Zip212Enforcement {
    zcash_primitives::transaction::components::sapling::zip212_enforcement(
        &MainNetwork,
        height.into(),
    )
}

fn sapling_ivks(view_keys: &[ViewKey]) -> Vec<(String, PreparedSaplingIvk)> {
    view_keys
        .iter()
        .filter_map(|vk| {
            vk.ufvk.sapling().map(|dfvk| {
                (
                    vk.recipient_address.clone(),
                    PreparedSaplingIvk::new(&dfvk.to_ivk(Scope::External)),
                )
            })
        })
        .collect()
}

fn orchard_ivks(view_keys: &[ViewKey]) -> Vec<(String, PreparedOrchardIvk)> {
    view_keys
        .iter()
        .filter_map(|vk| {
            vk.ufvk.orchard().map(|fvk| {
                (
                    vk.recipient_address.clone(),
                    fvk.to_ivk(Scope::External).prepare(),
                )
            })
        })
        .collect()
}
