use std::collections::HashMap;

use zcash_keys::keys::UnifiedFullViewingKey;
use zcash_protocol::consensus::MainNetwork;
use zcash_transparent::keys::IncomingViewingKey;

use crate::{
    error::{Error, Result},
    model::{KeySet, ViewKeyConfig},
};

#[derive(sqlx::FromRow)]
struct ViewKeyRow {
    recipient_address: String,
    view_key: String,
    reserve_table_name: String,
    birthday_height: Option<i64>,
}

/// Loads all active view keys from `zn_view_keys`.
///
/// For each UFVK that has a transparent component, the default transparent
/// address (index 0) is derived and its script_pubkey is stored for matching.
pub async fn load(pool: &sqlx::PgPool) -> Result<KeySet> {
    let rows: Vec<ViewKeyRow> = sqlx::query_as(
        "select recipient_address, view_key, reserve_table_name, birthday_height
         from public.zn_view_keys
         where is_active = true",
    )
    .fetch_all(pool)
    .await?;

    let mut keys = HashMap::with_capacity(rows.len());
    for row in rows {
        let ufvk =
            UnifiedFullViewingKey::decode(&MainNetwork, &row.view_key).map_err(|reason| {
                Error::ViewKey {
                    recipient_address: row.recipient_address.clone(),
                    reason,
                }
            })?;
        let birthday_height = row
            .birthday_height
            .map(|height| {
                u32::try_from(height).map_err(|_| Error::ViewKey {
                    recipient_address: row.recipient_address.clone(),
                    reason: "birthday_height is outside u32 range".to_owned(),
                })
            })
            .transpose()?;

        // Derive the transparent script_pubkey from the UFVK at index 0.
        let transparent_script = derive_transparent_script(&ufvk);

        keys.insert(
            row.recipient_address.clone(),
            ViewKeyConfig {
                recipient_address: row.recipient_address,
                ufvk,
                reserve_table_name: row.reserve_table_name,
                birthday_height,
                transparent_script,
            },
        );
    }

    Ok(KeySet { keys })
}

/// Derives the script_pubkey for the default external transparent address (index 0)
/// from the UFVK's transparent component. Returns None if the UFVK has no
/// transparent component or derivation fails.
fn derive_transparent_script(ufvk: &UnifiedFullViewingKey) -> Option<Vec<u8>> {
    let account_pubkey = ufvk.transparent()?;
    let external_ivk = account_pubkey.derive_external_ivk().ok()?;
    let (address, _index) = external_ivk.default_address();
    Some(transparent_script_bytes(&address))
}

/// Constructs the raw script_pubkey bytes for a P2PKH or P2SH transparent address.
///
/// P2PKH: OP_DUP OP_HASH160 <20 bytes> OP_EQUALVERIFY OP_CHECKSIG
/// P2SH:  OP_HASH160 <20 bytes> OP_EQUAL
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
