mod chain;
mod decrypt;
mod scanner;
mod store;

use std::{collections::HashSet, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use sqlx::PgPool;
use tokio::{sync::RwLock, time};
use tonic::Request;
use tracing::{error, info, warn};
use zebra_indexer_proto::{Empty, ZebraClient, mempool_change_message::ChangeType};

use crate::chain::Node;
use crate::decrypt::ViewKey;
use crate::store::OutputIndex;

// ── Constants ──────────────────────────────────────────────────────────────

const BLOCK_POLL: Duration = Duration::from_secs(3);
const KEY_REFRESH: Duration = Duration::from_secs(60);
const MEMPOOL_EXPIRY_SECS: i64 = 21_600;

// ── Entry point ────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL is required")?;
    let rpc_url = std::env::var("NODE_RPC_URL").context("NODE_RPC_URL is required")?;

    let pool = sqlx::PgPool::connect(&database_url)
        .await
        .context("failed to connect to Postgres")?;
    let node = Node::connect(&rpc_url)?;

    let (keys, output_index) = store::load_view_keys(&pool).await?;
    let view_keys = Arc::new(RwLock::new(keys));
    let output_index = Arc::new(RwLock::new(output_index));

    let key_task = tokio::spawn(refresh_keys(
        pool.clone(),
        view_keys.clone(),
        output_index.clone(),
    ));
    let expiry_task = tokio::spawn(expire_mempool(pool.clone(), output_index.clone()));
    let scanner_task = tokio::spawn(run_scanner(pool, view_keys, output_index, node));

    tokio::select! {
        result = key_task => result.context("key-refresh task panicked")??,
        result = expiry_task => result.context("expiry task panicked")??,
        result = scanner_task => result.context("scanner task panicked")??,
        signal = tokio::signal::ctrl_c() => signal.context("unable to listen for shutdown signal")?,
    }
    Ok(())
}

// ── Scanner loop: catch up to tip, then watch mempool ──────────────────────

async fn run_scanner(
    pool: PgPool,
    view_keys: Arc<RwLock<Vec<ViewKey>>>,
    output_index: Arc<RwLock<OutputIndex>>,
    node: Node,
) -> Result<()> {
    loop {
        catch_up(&pool, &view_keys, &output_index, &node).await?;
        watch_mempool(&pool, &view_keys, &output_index, &node).await?;
    }
}

/// Scans blocks from the lowest resume height across all view keys until at chain tip.
async fn catch_up(
    pool: &PgPool,
    view_keys: &Arc<RwLock<Vec<ViewKey>>>,
    output_index: &Arc<RwLock<OutputIndex>>,
    node: &Node,
) -> Result<()> {
    loop {
        let tip = node.block_count().await?;
        let keys = view_keys.read().await.clone();
        let index = output_index.read().await.clone();

        if keys.is_empty() {
            time::sleep(BLOCK_POLL).await;
            continue;
        }

        let mut min_resume = u32::MAX;
        for vk in &keys {
            let resume = store::resume_height(pool, &vk.recipient_address, &index).await?;
            min_resume = min_resume.min(resume);
        }

        if min_resume > tip {
            return Ok(()); // at tip
        }

        for height in min_resume..=tip {
            let hash = node.block_hash(height).await?;
            let bytes = node.raw_block(&hash).await?;
            let outputs = scanner::scan_block(&bytes, &keys, height)?;
            store::write_outputs(pool, &outputs, &index).await?;

            if !outputs.is_empty() {
                info!(height, outputs = outputs.len(), "scanned block");
            }
        }

        let new_tip = node.block_count().await?;
        if new_tip <= tip {
            return Ok(()); // still at tip
        }
        // more blocks arrived during scan, loop again
    }
}

/// Watches mempool via gRPC stream. Returns when new blocks are detected.
async fn watch_mempool(
    pool: &PgPool,
    view_keys: &Arc<RwLock<Vec<ViewKey>>>,
    output_index: &Arc<RwLock<OutputIndex>>,
    node: &Node,
) -> Result<()> {
    let indexer_url = std::env::var("NODE_INDEXER_GRPC_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8230".to_owned());
    let mut seen = HashSet::new();
    let mut block_check = time::interval(BLOCK_POLL);

    loop {
        let mut client = ZebraClient::connect(indexer_url.clone())
            .await
            .context("failed to connect to node indexer")?;

        info!("connected to node indexer mempool stream");

        let mut stream = client
            .mempool_change(Request::new(Empty {}))
            .await
            .context("failed to subscribe to mempool stream")?
            .into_inner();

        loop {
            tokio::select! {
                event = stream.message() => {
                    match event {
                        Err(err) => {
                            warn!(%err, "mempool stream failed; reconnecting");
                            break;
                        }
                        Ok(Some(event)) => match ChangeType::try_from(event.change_type) {
                            Ok(ChangeType::Added) => {
                                let txid = hex::encode(event.tx_hash);
                                let keys = view_keys.read().await.clone();
                                let index = output_index.read().await.clone();
                                let tip = node.block_count().await?;
                                if let Err(err) = scan_one_mempool_tx(
                                    pool, &keys, &index, node, &txid, &mut seen, tip,
                                ).await {
                                    warn!(%err, %txid, "could not scan mempool transaction");
                                }
                            }
                            Ok(ChangeType::Invalidated) | Ok(ChangeType::Mined) => {
                                seen.remove(&hex::encode(event.tx_hash));
                            }
                            Err(_) => warn!(change_type = event.change_type, "unknown mempool event"),
                        },
                        Ok(None) => break,
                    }
                }
                _ = block_check.tick() => {
                    if new_blocks_available(pool, view_keys, output_index, node).await? {
                        return Ok(());
                    }
                }
            }
        }

        warn!("mempool stream ended; reconnecting");
        time::sleep(Duration::from_secs(5)).await;
    }
}

async fn new_blocks_available(
    pool: &PgPool,
    view_keys: &Arc<RwLock<Vec<ViewKey>>>,
    output_index: &Arc<RwLock<OutputIndex>>,
    node: &Node,
) -> Result<bool> {
    let keys = view_keys.read().await.clone();
    let index = output_index.read().await.clone();
    if keys.is_empty() {
        return Ok(false);
    }
    let tip = node.block_count().await?;
    for vk in &keys {
        let resume = store::resume_height(pool, &vk.recipient_address, &index).await?;
        if resume <= tip {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn scan_one_mempool_tx(
    pool: &PgPool,
    keys: &[ViewKey],
    output_index: &OutputIndex,
    node: &Node,
    txid: &str,
    seen: &mut HashSet<String>,
    tip: u32,
) -> Result<()> {
    if seen.contains(txid) {
        return Ok(());
    }
    let bytes = node.raw_transaction(txid).await?;
    let outputs = scanner::scan_mempool_tx(&bytes, keys, tip)?;
    store::write_outputs(pool, &outputs, output_index).await?;
    seen.insert(txid.to_owned());
    if !outputs.is_empty() {
        info!(%txid, outputs = outputs.len(), "detected mempool payment");
    }
    Ok(())
}

// ── Background tasks ───────────────────────────────────────────────────────

async fn refresh_keys(
    pool: PgPool,
    view_keys: Arc<RwLock<Vec<ViewKey>>>,
    output_index: Arc<RwLock<OutputIndex>>,
) -> Result<()> {
    let mut interval = time::interval(KEY_REFRESH);
    loop {
        interval.tick().await;
        match store::load_view_keys(&pool).await {
            Ok((next_keys, next_index)) => {
                *view_keys.write().await = next_keys;
                *output_index.write().await = next_index;
            }
            Err(err) => error!(%err, "could not refresh view keys; retaining last good set"),
        }
    }
}

async fn expire_mempool(pool: PgPool, output_index: Arc<RwLock<OutputIndex>>) -> Result<()> {
    let mut interval = time::interval(Duration::from_secs(60));
    loop {
        interval.tick().await;
        let index = output_index.read().await;
        match store::expire_mempool(&pool, MEMPOOL_EXPIRY_SECS, &index).await {
            Ok(expired) if expired > 0 => info!(expired, "marked stale mempool payments expired"),
            Ok(_) => {}
            Err(err) => warn!(%err, "mempool expiry pass failed"),
        }
    }
}
