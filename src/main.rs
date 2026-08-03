mod db;
mod error;
mod keys;
mod model;
mod rpc;
mod scanner;

use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{Json, Router, extract::State, routing::get};
use serde::Serialize;
use sqlx::PgPool;
use tokio::{net::TcpListener, sync::RwLock, time};
use tonic::Request;
use tracing::{error, info, warn};
use zebra_indexer_proto::{Empty, ZebraClient, mempool_change_message::ChangeType};

use crate::{error::Result, model::KeySet, rpc::NodeRpc};

// ── Constants ──────────────────────────────────────────────────────────────
const BLOCK_POLL: Duration = Duration::from_secs(3);
const MEMPOOL_RECONCILE: Duration = Duration::from_secs(30);
const KEY_REFRESH: Duration = Duration::from_secs(60);
const MEMPOOL_EXPIRY_SECS: i64 = 21_600; // 6 hours
const HEALTH_BIND: &str = "127.0.0.1:9090";

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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let database_url =
        std::env::var("DATABASE_URL").map_err(|_| error::Error::MissingEnv("DATABASE_URL"))?;
    let rpc_url =
        std::env::var("NODE_RPC_URL").map_err(|_| error::Error::MissingEnv("NODE_RPC_URL"))?;

    let pool = db::connect(&database_url).await?;
    let initial_keys = keys::load(&pool).await?;
    let health = Health::default();
    health
        .keys_loaded
        .store(initial_keys.keys.len(), Ordering::Relaxed);
    let keys = Arc::new(RwLock::new(initial_keys));
    let rpc = NodeRpc::connect(&rpc_url)?;

    let health_task = tokio::spawn(run_health(health.clone()));
    let key_task = tokio::spawn(refresh_keys(pool.clone(), keys.clone(), health.clone()));
    let block_task = tokio::spawn(scan_blocks(
        pool.clone(),
        keys.clone(),
        rpc.clone(),
        health.clone(),
    ));
    let mempool_task = tokio::spawn(scan_mempool(
        pool.clone(),
        keys.clone(),
        rpc,
        health.clone(),
    ));
    let expiry_task = tokio::spawn(expire_mempool(pool.clone()));

    tokio::select! {
        result = health_task => result.expect("health task panicked")?,
        result = key_task => result.expect("key-refresh task panicked")?,
        result = block_task => result.expect("block scanner task panicked")?,
        result = mempool_task => result.expect("mempool scanner task panicked")?,
        result = expiry_task => result.expect("expiry task panicked")?,
        signal = tokio::signal::ctrl_c() => signal.expect("unable to listen for shutdown signal"),
    }
    Ok(())
}

async fn expire_mempool(pool: PgPool) -> Result<()> {
    let mut interval = time::interval(Duration::from_secs(60));
    loop {
        interval.tick().await;
        match db::expire_mempool(&pool, MEMPOOL_EXPIRY_SECS).await {
            Ok(expired) if expired > 0 => info!(expired, "marked stale mempool payments expired"),
            Ok(_) => {}
            Err(err) => warn!(%err, "mempool expiry pass failed"),
        }
    }
}

async fn run_health(health: Health) -> Result<()> {
    let app = Router::new()
        .route("/health", get(health_handler))
        .with_state(health);
    let listener = TcpListener::bind(HEALTH_BIND).await?;
    let bind: SocketAddr = HEALTH_BIND.parse().expect("valid HEALTH_BIND");
    info!(%bind, "health endpoint listening");
    axum::serve(listener, app).await.map_err(error::Error::Io)
}

async fn health_handler(State(health): State<Health>) -> Json<HealthResponse> {
    Json(HealthResponse {
        ok: true,
        keys_loaded: health.keys_loaded.load(Ordering::Relaxed),
        last_scanned_height: health.last_scanned_height.load(Ordering::Relaxed),
        mempool_events: health.mempool_events.load(Ordering::Relaxed),
    })
}

async fn refresh_keys(pool: PgPool, keys: Arc<RwLock<KeySet>>, health: Health) -> Result<()> {
    let mut interval = time::interval(KEY_REFRESH);
    loop {
        interval.tick().await;
        match crate::keys::load(&pool).await {
            Ok(next) => {
                health.keys_loaded.store(next.keys.len(), Ordering::Relaxed);
                *keys.write().await = next;
            }
            Err(err) => error!(%err, "could not refresh view keys; retaining last good set"),
        }
    }
}

async fn scan_blocks(
    pool: PgPool,
    keys: Arc<RwLock<KeySet>>,
    rpc: NodeRpc,
    health: Health,
) -> Result<()> {
    loop {
        if let Err(err) = scan_block_pass(&pool, &keys, &rpc, &health).await {
            error!(%err, "block scanner pass failed; retrying");
        }
        time::sleep(BLOCK_POLL).await;
    }
}

/// One pass of block scanning.
///
/// For each active view key, derives a resume height from its reserve table
/// (`max(blockheight) - 100`), then scans forward from the lowest resume height
/// to the current chain tip. All keys are tried on every block in the range,
/// so blocks are fetched only once.
async fn scan_block_pass(
    pool: &PgPool,
    keys: &Arc<RwLock<KeySet>>,
    rpc: &NodeRpc,
    health: &Health,
) -> Result<()> {
    let tip = rpc.block_count().await?;
    let key_snapshot = keys.read().await.clone();

    if key_snapshot.keys.is_empty() {
        return Ok(());
    }

    // Determine the lowest resume height across all accounts.
    let mut min_resume = u32::MAX;
    for cfg in key_snapshot.keys.values() {
        let resume =
            db::resume_height(pool, &cfg.reserve_table_name, cfg.birthday_height, 0).await?;
        min_resume = min_resume.min(resume);
    }

    if min_resume > tip {
        return Ok(());
    }

    for height in min_resume..=tip {
        let block_hash = rpc.block_hash(height).await?;
        let bytes = rpc.raw_block(&block_hash).await?;
        let outputs = scanner::scan_block(&bytes, &key_snapshot, height, tip, rpc).await?;
        db::write_outputs(pool, &outputs).await?;

        health
            .last_scanned_height
            .store(u64::from(height), Ordering::Relaxed);

        if !outputs.is_empty() {
            info!(height, outputs = outputs.len(), "scanned block");
        }
    }

    Ok(())
}

async fn scan_mempool(
    pool: PgPool,
    keys: Arc<RwLock<KeySet>>,
    rpc: NodeRpc,
    health: Health,
) -> Result<()> {
    let indexer_grpc_url = std::env::var("NODE_INDEXER_GRPC_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8230".to_owned());
    let mut seen = HashSet::new();
    loop {
        match ZebraClient::connect(indexer_grpc_url.clone()).await {
            Ok(mut client) => {
                info!("connected to node indexer mempool stream");
                let mut stream = match client.mempool_change(Request::new(Empty {})).await {
                    Ok(response) => response.into_inner(),
                    Err(err) => {
                        warn!(%err, "could not subscribe to mempool stream");
                        reconcile_mempool(&pool, &keys, &rpc, &mut seen, &health).await?;
                        time::sleep(MEMPOOL_RECONCILE).await;
                        continue;
                    }
                };
                let mut reconcile = time::interval(MEMPOOL_RECONCILE);
                loop {
                    tokio::select! {
                        event = stream.message() => match event {
                            Err(err) => {
                                warn!(%err, "mempool stream failed; reconnecting");
                                break;
                            }
                            Ok(Some(event)) => match ChangeType::try_from(event.change_type) {
                                Ok(ChangeType::Added) => {
                                    let txid = hex::encode(event.tx_hash);
                                    if let Err(err) = scan_one_mempool_tx(
                                        &pool, &keys, &rpc, &txid, &mut seen, &health,
                                    ).await {
                                        warn!(%err, %txid, "could not scan mempool transaction");
                                    }
                                }
                                Ok(ChangeType::Invalidated) | Ok(ChangeType::Mined) => {
                                    seen.remove(&hex::encode(event.tx_hash));
                                }
                                Err(_) => warn!(
                                    change_type = event.change_type,
                                    "unknown mempool event"
                                ),
                            },
                            Ok(None) => break,
                        },
                        _ = reconcile.tick() => {
                            if let Err(err) = reconcile_mempool(
                                &pool, &keys, &rpc, &mut seen, &health,
                            ).await {
                                warn!(%err, "mempool reconciliation failed");
                            }
                        },
                    }
                }
            }
            Err(err) => {
                warn!(%err, "node indexer unavailable; using JSON-RPC mempool reconciliation")
            }
        }
        if let Err(err) = reconcile_mempool(&pool, &keys, &rpc, &mut seen, &health).await {
            warn!(%err, "mempool reconciliation failed");
        }
        time::sleep(MEMPOOL_RECONCILE).await;
    }
}

async fn reconcile_mempool(
    pool: &PgPool,
    keys: &Arc<RwLock<KeySet>>,
    rpc: &NodeRpc,
    seen: &mut HashSet<String>,
    health: &Health,
) -> Result<()> {
    let current: HashSet<String> = rpc.mempool().await?.into_iter().collect();
    seen.retain(|txid| current.contains(txid));
    for txid in &current {
        scan_one_mempool_tx(pool, keys, rpc, txid, seen, health).await?;
    }
    Ok(())
}

async fn scan_one_mempool_tx(
    pool: &PgPool,
    keys: &Arc<RwLock<KeySet>>,
    rpc: &NodeRpc,
    txid: &str,
    seen: &mut HashSet<String>,
    health: &Health,
) -> Result<()> {
    if seen.contains(txid) {
        return Ok(());
    }
    let tip = rpc.block_count().await?;
    let bytes = rpc.raw_transaction(txid).await?;
    let key_snapshot = keys.read().await.clone();
    let outputs = scanner::scan_mempool_transaction(&bytes, &key_snapshot, tip, rpc).await?;
    db::write_outputs(pool, &outputs).await?;
    seen.insert(txid.to_owned());
    health.mempool_events.fetch_add(1, Ordering::Relaxed);
    if !outputs.is_empty() {
        info!(%txid, outputs = outputs.len(), "detected mempool payment");
    }
    Ok(())
}
