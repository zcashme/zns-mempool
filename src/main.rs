mod chain;
mod scanner;
mod store;

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use axum::{Json, Router, extract::State, routing::get};
use serde::Serialize;
use sqlx::PgPool;
use tokio::{net::TcpListener, sync::RwLock, time};
use tonic::Request;
use tracing::{error, info, warn};
use zcash_keys::keys::UnifiedFullViewingKey;
use zebra_indexer_proto::{Empty, ZebraClient, mempool_change_message::ChangeType};

use crate::chain::Node;

// ── Types ──────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct ViewKey {
    pub ufvk: UnifiedFullViewingKey,
    pub recipient_address: String,
    pub reserve_table_name: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    Mempool,
    Confirmed,
}

impl Status {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mempool => "mempool",
            Self::Confirmed => "confirmed",
        }
    }
}

#[derive(Clone, Debug)]
pub struct DetectedOutput {
    pub recipient_address: String,
    pub reserve_table_name: String,
    pub txid: String,
    pub pool: &'static str,
    pub output_index: i32,
    pub amount_zats: i64,
    pub sender_address: String,
    pub memo: Option<String>,
    pub status: Status,
    pub blockheight: Option<i64>,
}

// ── Constants ──────────────────────────────────────────────────────────────

const BLOCK_POLL: Duration = Duration::from_secs(3);
const KEY_REFRESH: Duration = Duration::from_secs(60);
const MEMPOOL_EXPIRY_SECS: i64 = 21_600;
const HEALTH_BIND: &str = "127.0.0.1:9090";

// ── Health ─────────────────────────────────────────────────────────────────

#[derive(Clone, Default)]
struct Health {
    keys_loaded: Arc<AtomicUsize>,
    last_scanned_height: Arc<AtomicU64>,
    mempool_events: Arc<AtomicU64>,
}

#[derive(Serialize)]
struct HealthResponse {
    ok: bool,
    keys_loaded: usize,
    last_scanned_height: u64,
    mempool_events: u64,
}

// ── Entry point ────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL is required")?;
    let rpc_url = std::env::var("NODE_RPC_URL").context("NODE_RPC_URL is required")?;

    let pool = store::connect(&database_url).await?;
    let node = Node::connect(&rpc_url)?;

    let view_keys = Arc::new(RwLock::new(store::load_view_keys(&pool).await?));
    let health = Health::default();
    health
        .keys_loaded
        .store(view_keys.read().await.len(), Ordering::Relaxed);

    let health_task = tokio::spawn(run_health(health.clone()));
    let key_task = tokio::spawn(refresh_keys(
        pool.clone(),
        view_keys.clone(),
        health.clone(),
    ));
    let expiry_task = tokio::spawn(expire_mempool(pool.clone()));
    let scanner_task = tokio::spawn(run_scanner(pool, view_keys, node, health.clone()));

    tokio::select! {
        result = health_task => result.context("health task panicked")??,
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
    node: Node,
    health: Health,
) -> Result<()> {
    loop {
        catch_up(&pool, &view_keys, &node, &health).await?;
        watch_mempool(&pool, &view_keys, &node, &health).await?;
    }
}

/// Scans blocks from the lowest resume height across all view keys until at chain tip.
async fn catch_up(
    pool: &PgPool,
    view_keys: &Arc<RwLock<Vec<ViewKey>>>,
    node: &Node,
    health: &Health,
) -> Result<()> {
    loop {
        let tip = node.block_count().await?;
        let keys = view_keys.read().await.clone();

        if keys.is_empty() {
            time::sleep(BLOCK_POLL).await;
            continue;
        }

        let mut min_resume = u32::MAX;
        for vk in &keys {
            let resume =
                store::resume_height(pool, &vk.recipient_address, &vk.reserve_table_name).await?;
            min_resume = min_resume.min(resume);
        }

        if min_resume > tip {
            return Ok(()); // at tip
        }

        for height in min_resume..=tip {
            let hash = node.block_hash(height).await?;
            let bytes = node.raw_block(&hash).await?;
            let outputs = scanner::scan_block(&bytes, &keys, height)?;
            store::write_outputs(pool, &outputs).await?;

            health
                .last_scanned_height
                .store(u64::from(height), Ordering::Relaxed);

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
    node: &Node,
    health: &Health,
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
                                let tip = node.block_count().await?;
                                if let Err(err) = scan_one_mempool_tx(
                                    pool, &keys, node, &txid, &mut seen, tip, health,
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
                    if new_blocks_available(pool, view_keys, node).await? {
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
    node: &Node,
) -> Result<bool> {
    let keys = view_keys.read().await.clone();
    if keys.is_empty() {
        return Ok(false);
    }
    let tip = node.block_count().await?;
    for vk in &keys {
        let resume =
            store::resume_height(pool, &vk.recipient_address, &vk.reserve_table_name).await?;
        if resume <= tip {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn scan_one_mempool_tx(
    pool: &PgPool,
    keys: &[ViewKey],
    node: &Node,
    txid: &str,
    seen: &mut HashSet<String>,
    tip: u32,
    health: &Health,
) -> Result<()> {
    if seen.contains(txid) {
        return Ok(());
    }
    let bytes = node.raw_transaction(txid).await?;
    let outputs = scanner::scan_mempool_tx(&bytes, keys, tip)?;
    store::write_outputs(pool, &outputs).await?;
    seen.insert(txid.to_owned());
    health.mempool_events.fetch_add(1, Ordering::Relaxed);
    if !outputs.is_empty() {
        info!(%txid, outputs = outputs.len(), "detected mempool payment");
    }
    Ok(())
}

// ── Background tasks ───────────────────────────────────────────────────────

async fn run_health(health: Health) -> Result<()> {
    let app = Router::new()
        .route("/health", get(health_handler))
        .with_state(health);
    let listener = TcpListener::bind(HEALTH_BIND).await?;
    info!(%HEALTH_BIND, "health endpoint listening");
    axum::serve(listener, app)
        .await
        .context("health server failed")
}

async fn health_handler(State(health): State<Health>) -> Json<HealthResponse> {
    Json(HealthResponse {
        ok: true,
        keys_loaded: health.keys_loaded.load(Ordering::Relaxed),
        last_scanned_height: health.last_scanned_height.load(Ordering::Relaxed),
        mempool_events: health.mempool_events.load(Ordering::Relaxed),
    })
}

async fn refresh_keys(
    pool: PgPool,
    view_keys: Arc<RwLock<Vec<ViewKey>>>,
    health: Health,
) -> Result<()> {
    let mut interval = time::interval(KEY_REFRESH);
    loop {
        interval.tick().await;
        match store::load_view_keys(&pool).await {
            Ok(next) => {
                health.keys_loaded.store(next.len(), Ordering::Relaxed);
                *view_keys.write().await = next;
            }
            Err(err) => error!(%err, "could not refresh view keys; retaining last good set"),
        }
    }
}

async fn expire_mempool(pool: PgPool) -> Result<()> {
    let mut interval = time::interval(Duration::from_secs(60));
    loop {
        interval.tick().await;
        match store::expire_mempool(&pool, MEMPOOL_EXPIRY_SECS).await {
            Ok(expired) if expired > 0 => info!(expired, "marked stale mempool payments expired"),
            Ok(_) => {}
            Err(err) => warn!(%err, "mempool expiry pass failed"),
        }
    }
}
