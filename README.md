# zns-mempool

Stateless Zcash payment scanner daemon. Detects incoming and outgoing transactions for configured wallets and writes them to Postgres.

## How it works

On startup the scanner syncs from where it left off to chain tip, then runs two concurrent loops forever:

- **Block watcher** — polls for new blocks, scans them, writes confirmed outputs.
- **Mempool watcher** — subscribes to a gRPC stream, scans each new transaction, writes mempool outputs.

Each shielded output is trial-decrypted with three keys per wallet: external IVK (incoming), internal IVK (change), and OVK (outgoing). Transparent outputs are matched by deriving P2PKH scripts from the UFVK.

Outputs flow: `mempool` → `confirmed` (when mined) or `mempool` → `expired` (after 1 hour unconfirmed).

## Docker build

Build for an x86-64 Linux server, including from an Apple Silicon Mac:

```sh
docker buildx build --platform linux/amd64 --load -t zns-mempool:nu7 .
```

The build uses the committed `.sqlx` query metadata and does not need a database
connection. Supply `DATABASE_URL`, `NODE_RPC_URL`, and optionally
`NODE_INDEXER_GRPC_URL` when running the container (see `.env.example`).

After changing SQL queries or the configuration schema, regenerate `.sqlx`
against a development database with that schema before rebuilding:

```sh
touch src/store.rs
SQLX_OFFLINE=false SQLX_OFFLINE_DIR=.sqlx DATABASE_URL="$BUILD_DATABASE_URL" cargo check --locked
```
