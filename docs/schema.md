# Scanner schema

The scanner is stateless. All configuration and all detected outputs live in Supabase/Postgres. There is no local cursor, no local wallet, and no SQLite store.

## Design

- One global config table: `zn_view_keys`.
- One reserve table per view key. The name is arbitrary and human-chosen: `zn_shithole`, `zn_crappoop`, `zn_waitlist_reserves_ops`, or anything else.
- The `zn_view_keys` row stores the exact reserve table name in `reserve_table_name`.
- Each reserve table is linked back to `zn_view_keys` through the stable `recipient_address`.

`recipient_address` is the natural primary key because, by design, each account table contains outputs for exactly one receiving address.

The scanner learns which physical table belongs to which view key by reading `reserve_table_name` directly from `zn_view_keys`.

---

## `zn_view_keys`

Single source of truth for what the scanner watches.

```sql
create table public.zn_view_keys (
    recipient_address   text primary key,
    view_key            text not null unique,
    reserve_table_name  text not null check (reserve_table_name ~ '^[a-zA-Z_][a-zA-Z0-9_]*$'),
    label               text,
    birthday_height     bigint,
    is_active           boolean not null default true,
    updated_at          timestamptz not null default now()
);
```

| Column | Meaning |
|--------|---------|
| `recipient_address` | The Zcash address users pay to. Stable natural key. Also the link to the per-account reserve table. |
| `view_key` | The canonical Unified Full Viewing Key (UFVK) the scanner uses to trial-decrypt. |
| `reserve_table_name` | The exact physical table where outputs for this view key are written, e.g. `zn_shithole`. |
| `label` | Optional human-readable name, e.g. `ops`, `treasury`, `shithole`. Used only for display/logging. |
| `birthday_height` | First block height to scan if the reserve table is empty. |
| `is_active` | If false, the scanner ignores this view key. |
| `updated_at` | Last time this config row changed. |

This table can live in `public`, but consider putting it in a non-browser schema such as `private` or `scanner` if your Supabase policies expose `public` tables to frontend clients.

The scanner reads every `is_active = true` row, uses `view_key` to trial-decrypt, and writes matches to `reserve_table_name`. `recipient_address` is not used during detection; it is only the database link and human identifier.

Operational rule: do not rename a reserve table directly. Instead create a new table and update `reserve_table_name` in `zn_view_keys`.

---

## Per-account reserve tables

One table per active view key. The name is arbitrary and chosen by you. The scanner reads `reserve_table_name` from `zn_view_keys` to know where to insert.

Concrete example:

```sql
create table public.zn_shithole (
    recipient_address text not null
        references public.zn_view_keys(recipient_address),

    txid              text not null,
    pool              text not null
        check (pool in ('sapling', 'orchard', 'ironwood', 'transparent')),
    output_index      int  not null,
    amount_zats       bigint not null,
    sender_address    text not null,

    status            text not null default 'mempool'
        check (status in ('mempool', 'confirmed', 'orphaned', 'expired')),

    blockheight       bigint,
    detected_at       timestamptz not null default now(),

    memo              text,

    primary key (recipient_address, txid, pool, output_index)
);

create index idx_zn_shithole_status_time
    on public.zn_shithole (status, detected_at);

create index idx_zn_shithole_blockheight
    on public.zn_shithole (blockheight desc nulls last);
```

Template for another account:

```sql
create table public.<table_name_you_choose> (
    recipient_address text not null
        references public.zn_view_keys(recipient_address),

    txid              text not null,
    pool              text not null
        check (pool in ('sapling', 'orchard', 'ironwood', 'transparent')),
    output_index      int  not null,
    amount_zats       bigint not null,
    sender_address    text not null,

    status            text not null default 'mempool'
        check (status in ('mempool', 'confirmed', 'orphaned', 'expired')),

    blockheight       bigint,
    detected_at       timestamptz not null default now(),

    memo              text,

    primary key (recipient_address, txid, pool, output_index)
);

create index idx_<table_name_you_choose>_status_time
    on public.<table_name_you_choose> (status, detected_at);

create index idx_<table_name_you_choose>_blockheight
    on public.<table_name_you_choose> (blockheight desc nulls last);
```

| Column | Meaning |
|--------|---------|
| `recipient_address` | Foreign key to `zn_view_keys`. Every row in this table has the same value. |
| `txid` | Transaction id, hex-encoded. |
| `pool` | Shielded pool or transparent. |
| `output_index` | Output/action index within the transaction. |
| `amount_zats` | Incoming amount in integer zatoshis. |
| `sender_address` | The address that sent the payment. Always available, including for transparent UTXOs. |
| `status` | `mempool`, `confirmed`, `orphaned`, or `expired`. |
| `blockheight` | Block height where this output was mined. Null for mempool outputs. |
| `detected_at` | When the scanner's own full node first saw the transaction. |
| `memo` | Decoded payment memo, if any. |

Primary key identity: one transaction can pay the same account multiple times across different pools or output indices, so `(recipient_address, txid, pool, output_index)` is the smallest unique key.

---

## Mapping view keys to reserve tables

The scanner joins the two pieces of information at startup:

1. `view_key` from `zn_view_keys` — used to trial-decrypt.
2. `reserve_table_name` from `zn_view_keys` — used to decide where to write detected outputs.

Example scanner query:

```sql
select recipient_address, view_key, birthday_height, reserve_table_name
from public.zn_view_keys
where is_active = true;
```

For each returned row, the scanner inserts into `reserve_table_name` with the same `recipient_address` value. The foreign key inside the reserve table validates that the address exists in `zn_view_keys`.

---

## Resume logic

No separate cursor table. The scanner derives its resume height from the reserve table itself:

```sql
select coalesce(max(blockheight) - 100, birthday_height, initial_height)
from public.<table_name_you_choose>
where status = 'confirmed';
```

- Start from `max(blockheight) - 100` to handle short reorganizations without a stored block hash.
- If the table is empty, fall back to `birthday_height`.
- If `birthday_height` is null, fall back to the daemon's `initial_height` config.

Worst case: the reserve table is truncated or missing and the scanner rescans from `birthday_height`. That is slower but still correct.

---

## What the scanner does not store

- No wallet state.
- No spend authority.
- No parsed claim or memo interpretation.
- No local SQLite, local files, or Kubernetes volumes beyond the binary.
- No separate cursor table.

The scanner only writes detected outputs and updates `zn_view_keys.updated_at` when config changes are observed.
