use orchard::{
    bundle::{Authorized, Bundle},
    keys::PreparedIncomingViewingKey as PreparedOrchardIvk,
    note_encryption::{DomainVersion, IronwoodVersion, NoteEncryptionDomain, OrchardVersion},
};
use sapling_crypto::{
    bundle::Bundle as SaplingBundle,
    keys::{OutgoingViewingKey as SaplingOvk, PreparedIncomingViewingKey as PreparedSaplingIvk},
    note_encryption::{SaplingDomain, Zip212Enforcement},
};
use zcash_keys::keys::UnifiedFullViewingKey;
use zcash_note_encryption::{try_note_decryption, try_output_recovery_with_ovk};
use zcash_primitives::transaction::Transaction;
use zcash_protocol::{consensus::MainNetwork, memo::MemoBytes, value::ZatBalance};
use zip32::Scope;

/// A shielded output decrypted by one of the scanner's view keys.
#[derive(Clone, Debug)]
pub struct DecryptedShieldedOutput {
    pub recipient_address: String,
    pub pool: &'static str,
    pub amount_zats: u64,
    pub is_outgoing: bool,
    pub is_change: bool,
    pub memo: MemoBytes,
}

/// One row from `zn_view_keys`: a wallet's view key plus scanner config.
#[derive(Clone)]
pub struct Wallet {
    pub ufvk: UnifiedFullViewingKey,
    pub recipient_address: String,
    pub reserve_table_name: String,
    pub birthday_height: Option<i64>,
}

/// Trial-decrypts every shielded output in `tx` against the supplied view keys.
///
/// `effective_height` is the block height for confirmed transactions, or
/// `chain_tip + 1` for mempool transactions (matching upstream's "mempool height").
pub fn decrypt_transaction(
    tx: &Transaction,
    view_keys: &[Wallet],
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

// ── Sapling ────────────────────────────────────────────────────────────────

struct SaplingKeys {
    recipient_address: String,
    ivk_external: PreparedSaplingIvk,
    ivk_internal: PreparedSaplingIvk,
    ovk: SaplingOvk,
}

fn decrypt_sapling(
    bundle: &SaplingBundle<sapling_crypto::bundle::Authorized, ZatBalance>,
    view_keys: &[Wallet],
    effective_height: u32,
    found: &mut Vec<DecryptedShieldedOutput>,
) {
    let zip212 = sapling_zip212_enforcement(effective_height);
    let domain = SaplingDomain::new(zip212);
    let keys = sapling_keys(view_keys);

    for output in bundle.shielded_outputs().iter() {
        for key in &keys {
            // External IVK → incoming payment
            if let Some((note, _, memo)) = try_note_decryption(&domain, &key.ivk_external, output) {
                found.push(DecryptedShieldedOutput {
                    recipient_address: key.recipient_address.clone(),
                    pool: "sapling",
                    amount_zats: note.value().inner(),
                    is_outgoing: false,
                    is_change: false,
                    memo: MemoBytes::from_bytes(&memo)
                        .expect("decryption always returns a 512-byte memo"),
                });
                continue;
            }
            // Internal IVK → change (stays in wallet, not sent out)
            if let Some((note, _, memo)) = try_note_decryption(&domain, &key.ivk_internal, output) {
                found.push(DecryptedShieldedOutput {
                    recipient_address: key.recipient_address.clone(),
                    pool: "sapling",
                    amount_zats: note.value().inner(),
                    is_outgoing: true,
                    is_change: true,
                    memo: MemoBytes::from_bytes(&memo)
                        .expect("decryption always returns a 512-byte memo"),
                });
                continue;
            }
            // OVK → outgoing spend
            if let Some((note, _, memo)) = try_output_recovery_with_ovk(
                &domain,
                &key.ovk,
                output,
                output.cv(),
                output.out_ciphertext(),
            ) {
                found.push(DecryptedShieldedOutput {
                    recipient_address: key.recipient_address.clone(),
                    pool: "sapling",
                    amount_zats: note.value().inner(),
                    is_outgoing: true,
                    is_change: false,
                    memo: MemoBytes::from_bytes(&memo)
                        .expect("decryption always returns a 512-byte memo"),
                });
                continue;
            }
        }
    }
}

fn sapling_keys(view_keys: &[Wallet]) -> Vec<SaplingKeys> {
    view_keys
        .iter()
        .filter_map(|vk| {
            let dfvk = vk.ufvk.sapling()?;
            Some(SaplingKeys {
                recipient_address: vk.recipient_address.clone(),
                ivk_external: PreparedSaplingIvk::new(&dfvk.to_ivk(Scope::External)),
                ivk_internal: PreparedSaplingIvk::new(&dfvk.to_ivk(Scope::Internal)),
                ovk: dfvk.fvk().ovk,
            })
        })
        .collect()
}

// ── Orchard / Ironwood ─────────────────────────────────────────────────────

struct OrchardKeys {
    recipient_address: String,
    ivk_external: PreparedOrchardIvk,
    ivk_internal: PreparedOrchardIvk,
    ovk: orchard::keys::OutgoingViewingKey,
}

fn decrypt_orchard_pool<V: DomainVersion>(
    bundle: &Bundle<Authorized, ZatBalance>,
    view_keys: &[Wallet],
    pool_name: &'static str,
    found: &mut Vec<DecryptedShieldedOutput>,
) {
    let keys = orchard_keys(view_keys);

    for action in bundle.actions().iter() {
        let domain = NoteEncryptionDomain::<V>::for_action(action);
        for key in &keys {
            // External IVK → incoming payment
            if let Some((note, _, memo)) = try_note_decryption(&domain, &key.ivk_external, action) {
                found.push(DecryptedShieldedOutput {
                    recipient_address: key.recipient_address.clone(),
                    pool: pool_name,
                    amount_zats: note.value().inner(),
                    is_outgoing: false,
                    is_change: false,
                    memo: MemoBytes::from_bytes(&memo)
                        .expect("decryption always returns a 512-byte memo"),
                });
                continue;
            }
            // Internal IVK → change (stays in wallet, not sent out)
            if let Some((note, _, memo)) = try_note_decryption(&domain, &key.ivk_internal, action) {
                found.push(DecryptedShieldedOutput {
                    recipient_address: key.recipient_address.clone(),
                    pool: pool_name,
                    amount_zats: note.value().inner(),
                    is_outgoing: true,
                    is_change: true,
                    memo: MemoBytes::from_bytes(&memo)
                        .expect("decryption always returns a 512-byte memo"),
                });
                continue;
            }
            // OVK → outgoing spend
            if let Some((note, _, memo)) = try_output_recovery_with_ovk(
                &domain,
                &key.ovk,
                action,
                action.cv_net(),
                &action.encrypted_note().out_ciphertext,
            ) {
                found.push(DecryptedShieldedOutput {
                    recipient_address: key.recipient_address.clone(),
                    pool: pool_name,
                    amount_zats: note.value().inner(),
                    is_outgoing: true,
                    is_change: false,
                    memo: MemoBytes::from_bytes(&memo)
                        .expect("decryption always returns a 512-byte memo"),
                });
                continue;
            }
        }
    }
}

fn orchard_keys(view_keys: &[Wallet]) -> Vec<OrchardKeys> {
    view_keys
        .iter()
        .filter_map(|vk| {
            let fvk = vk.ufvk.orchard()?;
            Some(OrchardKeys {
                recipient_address: vk.recipient_address.clone(),
                ivk_external: fvk.to_ivk(Scope::External).prepare(),
                ivk_internal: fvk.to_ivk(Scope::Internal).prepare(),
                ovk: fvk.to_ovk(Scope::External),
            })
        })
        .collect()
}

// ── Helpers ────────────────────────────────────────────────────────────────

fn sapling_zip212_enforcement(height: u32) -> Zip212Enforcement {
    zcash_primitives::transaction::components::sapling::zip212_enforcement(
        &MainNetwork,
        height.into(),
    )
}
