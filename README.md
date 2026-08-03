# zns-scanner

A stateless Rust daemon that trial-decrypts shielded Zcash transactions and writes detected payments into per-account reserve tables in Supabase Postgres.

## What it does

- Reads view keys from `public.zn_view_keys`
- Trial-decrypts every block and mempool transaction
- Writes matches to the per-account reserve table named in `reserve_table_name`
- Derives resume height from `max(blockheight) - 100` in each reserve table (no cursor table)
- Streams mempool events from the node indexer gRPC (falls back to JSON-RPC polling)
- Exposes a `/health` endpoint

## Env vars

| Variable | Required | Default |
|---|---|---|
| `NODE_RPC_URL` | yes | — |
| `DATABASE_URL` | yes | — |
| `NODE_INDEXER_GRPC_URL` | no | `http://127.0.0.1:8230` |

All other timing values are compile-time constants in `main.rs`.

## Build & run

```sh
export NODE_RPC_URL=http://user:pass@127.0.0.1:8232
export DATABASE_URL=postgresql://scanner:password@db.example.com:5432/postgres
cargo run --release
```

## Docker

```sh
docker build -t zns-scanner .
docker run --env-file .env zns-scanner
```