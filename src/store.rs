use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use sqlx::PgPool;
use zcash_keys::keys::UnifiedFullViewingKey;
use zcash_protocol::consensus::MainNetwork;

use crate::{decrypt::Wallet, scanner::DetectedOutput, scanner::Status};

pub async fn load_view_keys(pool: &PgPool) -> Result<Vec<Wallet>> {
    let rows = sqlx::query!(
        "select recipient_address, view_key, reserve_table_name, birthday_height
         from public.zn_view_keys
         where is_active = true"
    )
    .fetch_all(pool)
    .await
    .context("failed to load view keys")?;

    let mut wallets = Vec::with_capacity(rows.len());
    for row in rows {
        let ufvk = UnifiedFullViewingKey::decode(&MainNetwork, &row.view_key).map_err(|e| {
            anyhow::anyhow!(
                "invalid view key for {recipient}: {e}",
                recipient = row.recipient_address
            )
        })?;
        wallets.push(Wallet {
            recipient_address: row.recipient_address,
            ufvk,
            reserve_table_name: row.reserve_table_name,
            birthday_height: row.birthday_height,
        });
    }
    Ok(wallets)
}

/// Returns the maximum confirmed block height in the reserve table, if any.
pub async fn max_confirmed_height(pool: &PgPool, reserve_table_name: &str) -> Result<Option<i64>> {
    let max_height: Option<i64> = sqlx::query_scalar(&format!(
        "select max(blockheight) from public.{reserve_table_name} where status = '{}'",
        Status::Confirmed.as_str()
    ))
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to query max blockheight in {reserve_table_name}"))?;
    Ok(max_height)
}

pub async fn write_outputs(
    pool: &PgPool,
    outputs: &[DetectedOutput],
    wallets: &[Wallet],
) -> Result<()> {
    let table_map: HashMap<&str, &str> = wallets
        .iter()
        .map(|w| (w.recipient_address.as_str(), w.reserve_table_name.as_str()))
        .collect();

    for output in outputs {
        let reserve_table_name =
            table_map
                .get(output.recipient_address.as_str())
                .context(format!(
                    "no reserve table configured for {}",
                    output.recipient_address
                ))?;

        let sql = format!(
            "insert into public.{tbl} (
                recipient_address, txid, pool, output_index, amount_zats,
                status, blockheight, detected_at, memo
             ) values ($1, $2, $3, $4, $5, $6, $7, now(), $8)
             on conflict (recipient_address, txid, pool, output_index) do update
             set status = case
                    when excluded.status = 'confirmed' then 'confirmed'
                    else {tbl}.status
                 end,
                 blockheight = coalesce(excluded.blockheight, {tbl}.blockheight),
                 memo = coalesce({tbl}.memo, excluded.memo)",
            tbl = reserve_table_name,
        );

        sqlx::query(&sql)
            .bind(&output.recipient_address)
            .bind(&output.txid)
            .bind(output.pool)
            .bind(output.output_index)
            .bind(output.amount_zats)
            .bind(output.is_outgoing)
            .bind(output.status.as_str())
            .bind(output.blockheight)
            .bind(&output.memo)
            .execute(pool)
            .await
            .with_context(|| format!("failed to write output {} {}", output.txid, output.pool))?;
    }
    Ok(())
}

pub async fn expire_mempool(pool: &PgPool, expiry_seconds: i64, wallets: &[Wallet]) -> Result<u64> {
    let mut total = 0u64;
    let unique_names: HashSet<&str> = wallets
        .iter()
        .map(|w| w.reserve_table_name.as_str())
        .collect();
    for name in unique_names {
        let result = sqlx::query(&format!(
            "update public.{name}
             set status = '{}'
             where status = '{}'
               and detected_at < now() - make_interval(secs => $1)",
            Status::Expired.as_str(),
            Status::Mempool.as_str(),
        ))
        .bind(expiry_seconds)
        .execute(pool)
        .await
        .with_context(|| format!("failed to expire mempool in {name}"))?;
        total += result.rows_affected();
    }
    Ok(total)
}
