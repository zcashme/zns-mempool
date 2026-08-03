mod chain;
mod decrypt;
mod scanner;
mod store;

use std::{collections::HashSet, time::Duration};

use anyhow::{Context, Result};
use sqlx::{PgPool, postgres::PgListener};
use tokio::{sync::watch, time};
use tonic::Request;
use tracing::{error, info, warn};
use zebra_indexer_proto::{Empty, ZebraClient, mempool_change_message::ChangeType};

use crate::chain::Node;
use crate::decrypt::Wallet;

// ── Constants ──────────────────────────────────────────────────────────────

const BLOCK_POLL: Duration = Duration::from_secs(10);
const MEMPOOL_EXPIRY_SECS: i64 = 3_600;

// ── Entry point ────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL is required")?;
    let rpc_url = std::env::var("NODE_RPC_URL").context("NODE_RPC_URL is required")?;

    let db = sqlx::PgPool::connect(&database_url)
        .await
        .context("failed to connect to Postgres")?;
    let node = Node::connect(&rpc_url)?;

    // Subscribe before reading the first snapshot, so a configuration change made
    // during startup is queued and causes a harmless reload afterwards.
    let listener = subscribe_to_wallet_changes(&database_url).await?;
    let (wallet_updates, wallets) = watch::channel(store::load_view_keys(&db).await?);

    let wallet_task = tokio::spawn(refresh_wallets(
        listener,
        database_url.clone(),
        db.clone(),
        wallet_updates,
    ));

    // Sync to chain tip before starting mempool watcher.
    sync(&db, &wallets, &node).await?;
    info!("synced to chain tip");

    let expiry_task = tokio::spawn(expire_mempool(db.clone()));
    let block_task = tokio::spawn(watch_blocks(db.clone(), wallets.clone(), node.clone()));
    let mempool_task = tokio::spawn(watch_mempool(db, wallets, node));

    tokio::select! {
        result = wallet_task => result.context("wallet-config task panicked")??,
        result = expiry_task => result.context("expiry task panicked")??,
        result = block_task => result.context("block watcher panicked")??,
        result = mempool_task => result.context("mempool watcher panicked")??,
        signal = tokio::signal::ctrl_c() => signal.context("unable to listen for shutdown signal")?,
    }
    Ok(())
}

// ── Sync: scan blocks from resume height to chain tip ───────────────────────

async fn sync(db: &PgPool, wallets: &watch::Receiver<Vec<Wallet>>, node: &Node) -> Result<()> {
    loop {
        let tip = node.block_count().await?;
        let wallets = wallets.borrow().clone();

        if wallets.is_empty() {
            time::sleep(BLOCK_POLL).await;
            continue;
        }

        let mut min_resume = u32::MAX;
        for w in &wallets {
            let max_confirmed = store::max_confirmed_height(db, &w.reserve_table_name).await?;
            let resume = compute_resume_height(max_confirmed, w.birthday_height);
            min_resume = min_resume.min(resume);
        }

        if min_resume > tip {
            return Ok(()); // at tip
        }

        for height in min_resume..=tip {
            let hash = node.block_hash(height).await?;
            let bytes = node.raw_block(&hash).await?;
            let outputs = scanner::scan_block(&bytes, &wallets, height)?;
            store::write_outputs(db, &outputs).await?;

            if !outputs.is_empty() {
                info!(height, outputs = outputs.len(), "scanned block");
            }
        }

        let new_tip = node.block_count().await?;
        if new_tip <= tip {
            return Ok(()); // still at tip
        }
    }
}

// ── Watch blocks: continuously scan new blocks ─────────────────────────────

async fn watch_blocks(db: PgPool, wallets: watch::Receiver<Vec<Wallet>>, node: Node) -> Result<()> {
    loop {
        time::sleep(BLOCK_POLL).await;
        sync(&db, &wallets, &node).await?;
    }
}

// ── Watch mempool: gRPC stream, runs forever ───────────────────────────────

async fn watch_mempool(
    db: PgPool,
    wallets: watch::Receiver<Vec<Wallet>>,
    node: Node,
) -> Result<()> {
    let indexer_url = std::env::var("NODE_INDEXER_GRPC_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8230".to_owned());
    let mut seen = HashSet::new();

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
            match stream.message().await {
                Err(err) => {
                    warn!(%err, "mempool stream failed; reconnecting");
                    break;
                }
                Ok(Some(event)) => match ChangeType::try_from(event.change_type) {
                    Ok(ChangeType::Added) => {
                        let txid = hex::encode(event.tx_hash);
                        let wallet_snapshot = wallets.borrow().clone();
                        let tip = node.block_count().await?;
                        if seen.contains(&txid) {
                            continue;
                        }
                        match mempool_outputs(&wallet_snapshot, &node, &txid, tip).await {
                            Ok(outputs) => match store::write_outputs(&db, &outputs).await {
                                Ok(()) => {
                                    seen.insert(txid.clone());
                                    if !outputs.is_empty() {
                                        info!(%txid, outputs = outputs.len(), "detected mempool payment");
                                    }
                                }
                                Err(err) => {
                                    warn!(%err, %txid, "could not persist mempool transaction")
                                }
                            },
                            Err(err) => warn!(%err, %txid, "could not scan mempool transaction"),
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

        warn!("mempool stream ended; reconnecting");
        time::sleep(Duration::from_secs(5)).await;
    }
}

async fn mempool_outputs(
    wallets: &[Wallet],
    node: &Node,
    txid: &str,
    tip: u32,
) -> Result<Vec<scanner::DetectedOutput>> {
    let bytes = node.raw_transaction(txid).await?;
    scanner::scan_mempool_tx(&bytes, wallets, tip)
}

/// Computes the height at which scanning should resume.
///
/// Uses `max(blockheight) - 100` from the reserve table if available, otherwise
/// falls back to the per-view-key `birthday_height`, and finally to 0.
fn compute_resume_height(max_confirmed: Option<i64>, birthday_height: Option<i64>) -> u32 {
    max_confirmed
        .map(|h| (h - 100).max(0) as u32)
        .or_else(|| birthday_height.and_then(|h| u32::try_from(h).ok()))
        .unwrap_or(0)
}

// ── Background tasks ───────────────────────────────────────────────────────

async fn subscribe_to_wallet_changes(database_url: &str) -> Result<PgListener> {
    let mut listener = PgListener::connect(database_url)
        .await
        .context("failed to connect PgListener")?;
    listener
        .listen("zn_view_keys_changed")
        .await
        .context("failed to LISTEN on zn_view_keys_changed")?;
    Ok(listener)
}

/// Reloads and publishes a complete wallet snapshot after every Postgres notification.
async fn refresh_wallets(
    mut listener: PgListener,
    database_url: String,
    db: PgPool,
    updates: watch::Sender<Vec<Wallet>>,
) -> Result<()> {
    info!("listening for zn_view_keys changes");

    loop {
        match listener.recv().await {
            Ok(_) => match store::load_view_keys(&db).await {
                Ok(next) => {
                    updates.send_replace(next);
                    info!("wallets refreshed via NOTIFY");
                }
                Err(err) => error!(%err, "could not refresh wallets; retaining last good snapshot"),
            },
            Err(err) => {
                warn!(%err, "wallet-config listener failed; reconnecting");
                loop {
                    time::sleep(Duration::from_secs(5)).await;
                    match subscribe_to_wallet_changes(&database_url).await {
                        Ok(next) => {
                            listener = next;
                            info!("reconnected to zn_view_keys notifications");
                            break;
                        }
                        Err(err) => warn!(%err, "could not reconnect wallet-config listener"),
                    }
                }
            }
        }
    }
}

async fn expire_mempool(db: PgPool) -> Result<()> {
    let mut interval = time::interval(Duration::from_secs(60));
    loop {
        interval.tick().await;
        match store::expire_mempool(&db, MEMPOOL_EXPIRY_SECS).await {
            Ok(expired) if expired > 0 => info!(expired, "marked stale mempool payments expired"),
            Ok(_) => {}
            Err(err) => warn!(%err, "mempool expiry pass failed"),
        }
    }
}
