# zns-scanner architecture

## What it is

A stateless Rust daemon that sits between a Zcash full node and Supabase Postgres. It watches one or more Zcash view keys, detects incoming payments by trial-decrypting blocks and mempool transactions, and writes those payments into per-account reserve tables in Postgres.

Supabase Realtime observes the writes and pushes them to the frontend. The scanner does not use the Supabase browser SDK, Realtime SDK, or any wallet-spend logic.

## Product: real-time Zcash payment notifications

A user pays a recipient that accepts Zcash. The recipient wants immediate feedback:

- "We see your payment in the mempool."
- "Your payment is confirmed."

Because Zcash shielded transactions are private, no third party can simply query "did address X get paid." Only the holder of the view key can recognize incoming payments. The scanner is that holder: it watches the chain, decrypts what belongs to the recipient, and emits events into Postgres.

## High-level flow

```text
user pays recipient
        ↓
Zcash full node
        ↓
zns-scanner (trial-decrypts with recipient's view key)
        ↓
Supabase Postgres / per-recipient reserve table
        ↓
Supabase Realtime
        ↓
payment status UI / dashboard
```

Example user flow:

1. Recipient shows a Zcash address and amount.
2. User sends payment from their wallet.
3. User clicks "I just paid."
4. UI shows live status:
   - `mempool` → "Payment detected, waiting for confirmation."
   - `confirmed` → "Payment received and confirmed!"

## Statelessness

The scanner keeps no durable local state:

- No SQLite, no local files, no volume mounts.
- No wallet seed, no spend keys.
- No cursor table.
- View keys and table routing come from `zn_view_keys`.
- Resume height comes from `max(blockheight)` in each reserve table.

The only local data is runtime configuration from environment variables.

## Responsibilities

| In scope | Out of scope |
|----------|--------------|
| Trial-decrypt Sapling, Orchard, Ironwood outputs | Spending funds |
| Derive transparent address from UFVK (index 0, no gap limit) | Transparent address gap-limit derivation |
| Write detected outputs to reserve tables | Interpreting memos as business logic |
| Mark mempool outputs confirmed/orphaned/expired | Wallet synchronization |
| Expose a simple `/health` endpoint | Supabase Realtime client logic |

## Subsystems

### Configuration loader

Reads `zn_view_keys` at startup and on a periodic refresh interval. Active rows become the scanner's key set. Inactive rows are ignored.

Each loaded row provides:

- `view_key` — the UFVK to decrypt with.
- `recipient_address` — the FK value written into reserve rows.
- `reserve_table_name` — the physical table to insert into.
- `birthday_height` — fallback start height when a reserve table is empty.

### Block scanner

- Queries the full node for the current tip.
- For each active account, derives the resume height from `max(blockheight) - 100` in its reserve table.
- Fetches raw blocks, trial-decrypts with each UFVK, records matches.
- Updates `status` from `mempool` to `confirmed` when a previously seen mempool tx is mined.

### Mempool scanner

- Subscribes to the full node's mempool stream when available.
- Falls back to periodic JSON-RPC reconciliation if the stream disconnects.
- Writes newly seen mempool outputs with `status = 'mempool'`.

### Lifecycle manager

- Reorg handling: short reorganizations are covered by resuming 100 blocks back. Longer reorgs require deeper rewinds; the current design accepts rescanning from birthday height in extreme cases.
- Expiry: outputs that stay in `mempool` past a configured age are marked `expired`.

## Data model

See `schema.md` for the full table definitions.

At a glance:

- `zn_view_keys` — one row per watched view key; holds `view_key`, `recipient_address`, `reserve_table_name`, `birthday_height`, `is_active`.
- recipient reserve tables — one table per view key, arbitrary name, foreign key on `recipient_address`, one row per detected output.

The scanner does not know about invoices, orders, or recipients. It only says "this address received this amount at this time." The downstream app joins that event with whatever invoice, order, or session was associated with that address.

## Configuration

Environment variables drive runtime behavior:

- `DATABASE_URL` — Supabase/Postgres connection string.
- `NODE_RPC_URL` — full node JSON-RPC endpoint.
- `NODE_INDEXER_GRPC_URL` — full node gRPC indexer endpoint for mempool stream.
- `INITIAL_HEIGHT` — global fallback if a key has no `birthday_height` and an empty reserve table.
- `BLOCK_POLL_SECONDS` — how often to poll for new blocks.
- `MEMPOOL_RECONCILE_SECONDS` — how often to reconcile mempool via RPC.
- `MEMPOOL_EXPIRY_SECONDS` — how long an unmined output stays `mempool` before `expired`.
- `HEALTH_BIND` — address for the `/health` HTTP endpoint.

## Error handling philosophy

- A single failed block pass is logged and retried; the scanner does not crash the whole process.
- A failed mempool stream reconnects and falls back to RPC reconciliation.
- A failed key parse logs the error and skips that key; other keys keep scanning.
- Database write failures are retried with backoff where appropriate.

## Deployment

The daemon is containerized. It connects directly to Postgres with a privileged service role. It does not need browser Supabase credentials.

See `README.md` and `compose.yaml` for build and run instructions.
