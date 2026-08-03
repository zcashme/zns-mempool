use std::collections::HashMap;

use anyhow::{Context, Result};
use sqlx::PgPool;
use zcash_keys::keys::UnifiedFullViewingKey;
use zcash_protocol::consensus::MainNetwork;

use crate::{decrypt::ViewKey, scanner::DetectedOutput, scanner::Status};

/// Maps `recipient_address` to the physical table where that account's outputs are written.
pub type OutputIndex = HashMap<String, String>;

pub async fn load_view_keys(pool: &PgPool) -> Result<(Vec<ViewKey>, OutputIndex)> {
    let rows = sqlx::query!(
        "select recipient_address, view_key, reserve_table_name
         from public.zn_view_keys
         where is_active = true"
    )
    .fetch_all(pool)
    .await
    .context("failed to load view keys")?;

    let mut keys = Vec::with_capacity(rows.len());
    let mut output_index = OutputIndex::with_capacity(rows.len());
    for row in rows {
        let ufvk = UnifiedFullViewingKey::decode(&MainNetwork, &row.view_key).map_err(|e| {
            anyhow::anyhow!(
                "invalid view key for {recipient}: {e}",
                recipient = row.recipient_address
            )
        })?;
        output_index.insert(row.recipient_address.clone(), row.reserve_table_name);
        keys.push(ViewKey {
            recipient_address: row.recipient_address,
            ufvk,
        });
    }
    Ok((keys, output_index))
}

/// `max(blockheight) - 100` from the reserve table, falling back to
/// `birthday_height` from `zn_view_keys`, then to 0.
pub async fn resume_height(
    pool: &PgPool,
    recipient_address: &str,
    output_index: &OutputIndex,
) -> Result<u32> {
    let reserve_table_name = output_index.get(recipient_address).context(format!(
        "no reserve table configured for {recipient_address}"
    ))?;

    let max_height: Option<i64> = sqlx::query_scalar(&format!(
        "select max(blockheight) from public.{reserve_table_name} where status = 'confirmed'"
    ))
    .fetch_one(pool)
    .await
    .context("failed to query max blockheight")?;

    if let Some(max) = max_height {
        return Ok((max - 100).max(0) as u32);
    }

    let birthday: Option<i64> = sqlx::query_scalar(
        "select birthday_height from public.zn_view_keys where recipient_address = $1",
    )
    .bind(recipient_address)
    .fetch_one(pool)
    .await
    .context("failed to query birthday_height")?;

    Ok(birthday.and_then(|h| u32::try_from(h).ok()).unwrap_or(0))
}

pub async fn write_outputs(
    pool: &PgPool,
    outputs: &[DetectedOutput],
    output_index: &OutputIndex,
) -> Result<()> {
    for output in outputs {
        let reserve_table_name = output_index
            .get(&output.recipient_address)
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
            .bind(output.status.as_str())
            .bind(output.blockheight)
            .bind(&output.memo)
            .execute(pool)
            .await
            .with_context(|| format!("failed to write output {} {}", output.txid, output.pool))?;
    }
    Ok(())
}

pub async fn expire_mempool(
    pool: &PgPool,
    expiry_seconds: i64,
    output_index: &OutputIndex,
) -> Result<u64> {
    use std::collections::HashSet;

    let mut total = 0u64;
    let unique_names: HashSet<&String> = output_index.values().collect();
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
