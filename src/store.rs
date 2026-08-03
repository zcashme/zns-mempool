use anyhow::{Context, Result};
use sqlx::{PgPool, postgres::PgPoolOptions};
use zcash_keys::keys::UnifiedFullViewingKey;
use zcash_protocol::consensus::MainNetwork;

use crate::{DetectedOutput, ViewKey};

pub async fn connect(database_url: &str) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
        .context("failed to connect to Postgres")
}

#[derive(sqlx::FromRow)]
struct ViewKeyRow {
    recipient_address: String,
    view_key: String,
    reserve_table_name: String,
}

pub async fn load_view_keys(pool: &PgPool) -> Result<Vec<ViewKey>> {
    let rows: Vec<ViewKeyRow> = sqlx::query_as(
        "select recipient_address, view_key, reserve_table_name
         from public.zn_view_keys
         where is_active = true",
    )
    .fetch_all(pool)
    .await
    .context("failed to load view keys")?;

    let mut keys = Vec::with_capacity(rows.len());
    for row in rows {
        let ufvk = UnifiedFullViewingKey::decode(&MainNetwork, &row.view_key).map_err(|e| {
            anyhow::anyhow!(
                "invalid view key for {recipient}: {e}",
                recipient = row.recipient_address
            )
        })?;
        keys.push(ViewKey {
            recipient_address: row.recipient_address,
            ufvk,
            reserve_table_name: row.reserve_table_name,
        });
    }
    Ok(keys)
}

/// `max(blockheight) - 100` from the reserve table, falling back to
/// `birthday_height` from `zn_view_keys`, then to 0.
pub async fn resume_height(
    pool: &PgPool,
    recipient_address: &str,
    reserve_table_name: &str,
) -> Result<u32> {
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

pub async fn write_outputs(pool: &PgPool, outputs: &[DetectedOutput]) -> Result<()> {
    for output in outputs {
        let sql = format!(
            "insert into public.{tbl} (
                recipient_address, txid, pool, output_index, amount_zats,
                sender_address, status, blockheight, detected_at, memo
             ) values ($1, $2, $3, $4, $5, $6, $7, $8, now(), $9)
             on conflict (recipient_address, txid, pool, output_index) do update
             set status = case
                    when excluded.status = 'confirmed' then 'confirmed'
                    else {tbl}.status
                 end,
                 blockheight = coalesce(excluded.blockheight, {tbl}.blockheight),
                 memo = coalesce({tbl}.memo, excluded.memo)",
            tbl = output.reserve_table_name,
        );

        sqlx::query(&sql)
            .bind(&output.recipient_address)
            .bind(&output.txid)
            .bind(output.pool)
            .bind(output.output_index)
            .bind(output.amount_zats)
            .bind(&output.sender_address)
            .bind(output.status.as_str())
            .bind(output.blockheight)
            .bind(&output.memo)
            .execute(pool)
            .await
            .with_context(|| format!("failed to write output {} {}", output.txid, output.pool))?;
    }
    Ok(())
}

pub async fn expire_mempool(pool: &PgPool, expiry_seconds: i64) -> Result<u64> {
    let table_names: Vec<String> = sqlx::query_scalar(
        "select reserve_table_name from public.zn_view_keys where is_active = true",
    )
    .fetch_all(pool)
    .await
    .context("failed to load table names for expiry")?;

    let mut total = 0u64;
    for name in &table_names {
        let result = sqlx::query(&format!(
            "update public.{name}
             set status = 'expired'
             where status = 'mempool'
               and detected_at < now() - make_interval(secs => $1)"
        ))
        .bind(expiry_seconds)
        .execute(pool)
        .await
        .with_context(|| format!("failed to expire mempool in {name}"))?;
        total += result.rows_affected();
    }
    Ok(total)
}
