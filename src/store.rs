use std::collections::HashMap;

use anyhow::{Context, Result};
use sqlx::PgPool;
use zcash_keys::keys::UnifiedFullViewingKey;
use zcash_protocol::{consensus::MainNetwork, memo::Memo};

use crate::{decrypt::Wallet, scanner::DetectedOutput, scanner::Status};

pub async fn ensure_table_exists(db: &PgPool, reserve_table_name: &str) -> Result<()> {
    if reserve_table_name.is_empty()
        || reserve_table_name.len() > 63
        || !reserve_table_name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_')
    {
        anyhow::bail!("invalid table name: {}", reserve_table_name);
    }

    sqlx::query(&format!(
        r#"CREATE TABLE IF NOT EXISTS public."{tbl}" (
            recipient_address text NOT NULL,
            txid text NOT NULL,
            pool text NOT NULL,
            amount_zats bigint NOT NULL,
            is_outgoing boolean NOT NULL DEFAULT false,
            status text NOT NULL DEFAULT 'mempool',
            blockheight bigint,
            detected_at timestamptz NOT NULL DEFAULT now(),
            memo text,
            fee_zats bigint DEFAULT 0,
            PRIMARY KEY (recipient_address, txid, is_outgoing)
        )"#,
        tbl = reserve_table_name,
    ))
    .execute(db)
    .await?;

    sqlx::query(&format!(
        r#"ALTER TABLE public."{tbl}" ADD COLUMN IF NOT EXISTS fee_zats bigint DEFAULT 0"#,
        tbl = reserve_table_name,
    ))
    .execute(db)
    .await?;

    if let Err(e) = sqlx::query(&format!(
        r#"ALTER PUBLICATION supabase_realtime ADD TABLE public."{tbl}""#,
        tbl = reserve_table_name,
    ))
    .execute(db)
    .await
    {
        tracing::warn!(%e, table = reserve_table_name, "failed to add table to supabase_realtime (may already be a member)");
    }

    Ok(())
}

pub async fn load_view_keys(db: &PgPool) -> Result<Vec<Wallet>> {
    let rows = sqlx::query!(
        "select recipient_address, view_key, reserve_table_name, birthday_height
         from public.zn_view_keys
         where is_active = true"
    )
    .fetch_all(db)
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

        ensure_table_exists(db, &row.reserve_table_name).await?;

        wallets.push(Wallet {
            recipient_address: row.recipient_address,
            ufvk,
            reserve_table_name: row.reserve_table_name,
            birthday_height: row.birthday_height,
        });
    }
    Ok(wallets)
}

/// Returns the maximum confirmed block height in the transactions table, if any.
pub async fn max_confirmed_height(db: &PgPool, reserve_table_name: &str) -> Result<Option<i64>> {
    let max_height: Option<i64> = sqlx::query_scalar(&format!(
        "select max(blockheight) from public.{reserve_table_name} where status = '{}'",
        Status::Confirmed.as_str()
    ))
    .fetch_one(db)
    .await
    .with_context(|| format!("failed to query max blockheight in {reserve_table_name}"))?;
    Ok(max_height)
}

pub async fn write_outputs(db: &PgPool, outputs: &[DetectedOutput]) -> Result<()> {
    if outputs.is_empty() {
        return Ok(());
    }

    let recipient_addresses: Vec<String> = outputs
        .iter()
        .map(|output| output.recipient_address.clone())
        .collect();
    let routes: HashMap<String, String> = sqlx::query!(
        "select recipient_address, reserve_table_name
         from public.zn_view_keys
         where recipient_address = any($1)",
        &recipient_addresses,
    )
    .fetch_all(db)
    .await
    .context("failed to resolve output tables")?
    .into_iter()
    .map(|row| (row.recipient_address, row.reserve_table_name))
    .collect();

    for output in outputs {
        let reserve_table_name = routes.get(&output.recipient_address).context(format!(
            "no reserve table configured for {}",
            output.recipient_address
        ))?;
        let sql = format!(
            "insert into public.{tbl} (
                recipient_address, txid, pool, amount_zats,
                is_outgoing, status, blockheight, detected_at, memo, fee_zats
             ) values ($1, $2, $3, $4, $5, $6, $7, now(), $8, $9)
             on conflict (recipient_address, txid, is_outgoing) do update
             set status = case
                    when excluded.status = 'confirmed' then 'confirmed'
                    else {tbl}.status
                 end,
                 pool = excluded.pool,
                 amount_zats = excluded.amount_zats,
                 blockheight = coalesce(excluded.blockheight, {tbl}.blockheight),
                 memo = coalesce({tbl}.memo, excluded.memo),
                 fee_zats = excluded.fee_zats",
            tbl = reserve_table_name,
        );
        let memo: Option<String> = output
            .memo
            .as_ref()
            .and_then(|memo| Memo::try_from(memo).ok())
            .and_then(|memo| match memo {
                Memo::Text(text) => Some(text.replace('\0', "")),
                _ => None,
            });

        sqlx::query(&sql)
            .bind(&output.recipient_address)
            .bind(&output.txid)
            .bind(&output.pool)
            .bind(output.amount_zats)
            .bind(output.is_outgoing)
            .bind(output.status.as_str())
            .bind(output.blockheight)
            .bind(memo)
            .bind(output.fee_zats)
            .execute(db)
            .await
            .with_context(|| format!("failed to write output {} {}", output.txid, output.pool))?;
    }
    Ok(())
}

pub async fn expire_mempool(db: &PgPool, expiry_seconds: i64) -> Result<u64> {
    let mut total = 0u64;
    let reserve_table_names =
        sqlx::query_scalar!("select distinct reserve_table_name from public.zn_view_keys")
            .fetch_all(db)
            .await
            .context("failed to load reserve tables for mempool expiry")?;

    for name in reserve_table_names {
        let result = sqlx::query(&format!(
            "update public.{name}
             set status = '{}'
             where status = '{}'
               and detected_at < now() - make_interval(secs => $1)",
            Status::Expired.as_str(),
            Status::Mempool.as_str(),
        ))
        .bind(expiry_seconds)
        .execute(db)
        .await
        .with_context(|| format!("failed to expire mempool in {name}"))?;
        total += result.rows_affected();
    }
    Ok(total)
}
