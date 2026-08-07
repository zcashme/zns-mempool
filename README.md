# zns-mempool

Stateless Zcash payment scanner daemon. Detects incoming and outgoing transactions for configured wallets and writes them to Postgres.

## How it works

On startup the scanner syncs from where it left off to chain tip, then runs two concurrent loops forever:

- **Block watcher** — polls for new blocks, scans them, writes confirmed outputs.
- **Mempool watcher** — subscribes to a gRPC stream, scans each new transaction, writes mempool outputs.

Each shielded output is trial-decrypted with three keys per wallet: external IVK (incoming), internal IVK (change), and OVK (outgoing). Transparent outputs are matched by deriving P2PKH scripts from the UFVK.

Outputs flow: `mempool` → `confirmed` (when mined) or `mempool` → `expired` (after 1 hour unconfirmed).
