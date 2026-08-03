use sqlx::{PgPool, postgres::PgPoolOptions};

use crate::error::Result;

pub async fn connect(database_url: &str) -> Result<PgPool> {
    Ok(PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await?)
}

/// Validates that `table_name` is a simple SQL identifier (letters, digits, underscores).
/// This is required because the scanner interpolates the reserve table name into queries.
pub fn validate_table_name(table_name: &str) -> Result<()> {
    if table_name.is_empty()
        || !table_name.chars().all(|c| c.is_alphanumeric() || c == '_')
        || table_name
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_digit())
    {
        return Err(crate::error::Error::InvalidTableName(table_name.to_owned()));
    }
    Ok(())
}

/// Returns the resume height for a given reserve table.
///
/// Per the schema: `max(blockheight) - 100` to tolerate short reorgs.
/// Falls back to `birthday_height` if the table is empty, then to `initial_height`.
pub async fn resume_height(
    pool: &PgPool,
    table_name: &str,
    birthday_height: Option<u32>,
    initial_height: u32,
) -> Result<u32> {
    validate_table_name(table_name)?;

    let max_height: Option<i64> = sqlx::query_scalar(&format!(
        "select max(blockheight) from public.{table_name} where status = 'confirmed'"
    ))
    .fetch_one(pool)
    .await?;

    if let Some(max) = max_height {
        let resume = (max - 100).max(0) as u32;
        return Ok(resume);
    }

    Ok(birthday_height.unwrap_or(initial_height))
}

/// Writes detected outputs into their respective reserve tables.
///
/// Each output carries its own `reserve_table_name`, so outputs for different
/// accounts can be written in a single batch.
pub async fn write_outputs(pool: &PgPool, outputs: &[crate::model::DetectedOutput]) -> Result<()> {
    for output in outputs {
        write_single_output(pool, output).await?;
    }
    Ok(())
}

async fn write_single_output(pool: &PgPool, output: &crate::model::DetectedOutput) -> Result<()> {
    validate_table_name(&output.reserve_table_name)?;

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
        .await?;

    Ok(())
}

/// Marks mempool outputs older than `expiry_seconds` as expired in every reserve table.
pub async fn expire_mempool(pool: &PgPool, expiry_seconds: i64) -> Result<u64> {
    let table_names: Vec<String> =
        sqlx::query_scalar("select reserve_table_name from public.zn_view_keys")
            .fetch_all(pool)
            .await?;

    let mut total = 0u64;
    for name in &table_names {
        validate_table_name(name)?;
        let result = sqlx::query(&format!(
            "update public.{name}
             set status = 'expired'
             where status = 'mempool'
               and detected_at < now() - make_interval(secs => $1)"
        ))
        .bind(expiry_seconds)
        .execute(pool)
        .await?;
        total += result.rows_affected();
    }
    Ok(total)
}
