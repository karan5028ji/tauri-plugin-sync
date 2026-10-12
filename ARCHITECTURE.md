# Technical Architecture Document

## System Architecture: `tauri-plugin-sync`

---

## 1. High-Level Architecture Diagram

```
+---------------------------------------------------------------+
|                      Tauri v2 Frontend                        |
|  (React / Vue / Svelte / Vanilla TS - Guest-JS SDK)           |
+---------------------------------------------------------------+
                               |
                               | IPC Commands & Event Emitter
                               v
+---------------------------------------------------------------+
|                   tauri-plugin-sync (Rust Core)               |
|                                                               |
|  +---------------------+           +------------------------+  |
|  |  Sync Engine Actor  | <-------> |  SQLite CDC & Triggers |  |
|  |  (Tokio Background) |           |  (_sync_changes log)   |  |
|  +---------------------+           +------------------------+  |
|             |                                   |             |
|             | HTTP / WebSocket                  | rusqlite /  |
|             v                                   | sqlx        |
|  +---------------------+                        v             |
|  | Remote Sync Server  |           +------------------------+  |
|  | (Supabase / Custom) |           |   Local SQLite DB      |  |
|  +---------------------+           +------------------------+  |
+---------------------------------------------------------------+
```

---

## 2. Core Components

### 2.1 SQLite Change Data Capture (CDC)
- **Table Shadowing & Triggers:**
  When a table (e.g. `todos`) is registered for sync, the plugin automatically creates:
  1. A shadow metadata table `_sync_meta` storing schema versions, client device ID, and synchronization watermarks.
  2. A journal table `_sync_changes`:
     ```sql
     CREATE TABLE IF NOT EXISTS _sync_changes (
       id INTEGER PRIMARY KEY AUTOINCREMENT,
       table_name TEXT NOT NULL,
       row_id TEXT NOT NULL,
       operation TEXT NOT NULL, -- 'INSERT', 'UPDATE', 'DELETE'
       data JSON,               -- Row snapshot or delta
       timestamp INTEGER NOT NULL,
       synced INTEGER DEFAULT 0 -- 0: pending, 1: synced
     );
     ```
  3. Triggers on the user table (`AFTER INSERT`, `AFTER UPDATE`, `AFTER DELETE`) that record changes into `_sync_changes` automatically without requiring the developer to modify their application SQL queries.

### 2.2 Sync Engine (Rust Background Worker)
- Runs as an asynchronous Tokio task inside Tauri's managed state.
- **Push Pipeline (Bounded Chunking):**
  1. Reads all rows from `_sync_changes WHERE synced = 0 ORDER BY id ASC LIMIT ?1` bounded by `batch_size` (default: 500 rows) to prevent Out-Of-Memory (OOM) spikes during initial or heavy synchronization.
  2. Batches records into a canonical Changeset payload:
     ```json
     {
       "client_id": "uuid-v4",
       "watermark": 1728734000,
       "changes": [
         {
           "table": "todos",
           "row_id": "item-101",
           "op": "INSERT",
           "data": { "title": "Buy milk", "completed": false },
           "timestamp": 1728734000
         }
       ]
     }
     ```
  3. Sends HTTP POST / WebSocket message to the configured remote endpoint. Redacts all authorization headers and bearer tokens from error traces.
  4. Handles HTTP 401 Unauthorized by transitioning to `SyncState::Error` and broadcasting `sync://unauthorized`.
  5. Upon remote acknowledgement (`200 OK`), marks changesets as `synced = 1` and loops until all pending chunks are pushed.
  6. Automatically prunes synced journal entries older than `prune_days` (default: 30 days) to prevent unbound disk growth.
- **Pull Pipeline (Deferred Foreign Keys & Chunking):**
  1. Requests changes from the remote endpoint with `since = last_sync_watermark` and `limit = batch_size` (default: 500).
  2. Receives incoming changesets from other devices or the server in bounded chunks.
  3. Executes inside an atomic SQLite transaction with:
     - `PRAGMA defer_foreign_keys = ON;`: Defers FK constraint validation until transaction `COMMIT`. Parent and child records (e.g. `categories` and `tasks`) arriving in arbitrary or reversed batch order apply smoothly without foreign key constraint violations.
     - `_sync_suppress = 1`: Disables local CDC triggers temporarily to prevent re-logging remote updates as new local mutations.
  4. **Schema Evolution & Column Filtering:** Before inserting/updating rows, inspects local table columns via `PRAGMA table_info(...)`. Extra/unknown columns sent by newer v2 servers are cleanly dropped, preventing forward-compatibility crashes on older clients.
  5. Advances `last_sync_watermark` and loops until all chunks are pulled.

### 2.3 Conflict Resolution Strategy & CRDT Evolution
- **v1.0 Production Implementation: Row-Level Deterministic Last-Write-Wins (LWW) with Tombstones:**
  - Every mutation carries a high-precision UTC millisecond timestamp.
  - If a row was modified locally at `T1` and remotely at `T2`, the change with the higher timestamp takes precedence.
  - **Tombstone Protection:** Deleted rows are tracked in `_sync_tombstones (table_name, row_id, deleted_at)`. Stale remote updates ($T_{\text{remote}} \le T_{\text{deleted}}$) are rejected, guaranteeing deleted records are never resurrected by network reordering.
  - **Recreation Safety:** If a remote creation occurs with a timestamp strictly greater than the tombstone ($T_{\text{remote}} > T_{\text{deleted}}$), the tombstone is cleared, allowing intentional recreations.
- **v2.0 Roadmap Evolution: Column-Level Merging & CRDT Integration:**
  - Row-level LWW is optimal for discrete documents and record-level edits.
  - v2.0 introduces field-level JSON-Patch merge / State-based CRDTs (LWW-Element-Set per column) for concurrent multi-user editing of shared text and collaborative fields without overwriting adjacent column modifications.

### 2.4 Guest-JS SDK & IPC Interface
- Exposes standard Tauri commands via IPC:
  - `plugin:sync|init`: Initializes database tracking, registers tables, and configures WAL/busy timeout.
  - `plugin:sync|sync_now`: Manually forces a push/pull sync pass.
  - `plugin:sync|get_status`: Returns current sync status (`idle`, `syncing`, `offline`, `error`).
  - `plugin:sync|configure`: Updates endpoint URL, authentication headers, batch size, or intervals dynamically.
- Emits Tauri frontend events:
  - `sync://status`: Emitted on state changes.
  - `sync://progress`: Emitted with count of pushed and pulled changesets.
  - `sync://unauthorized`: Emitted upon HTTP 401, triggering automated `onTokenExpired()` token refresh and automatic re-sync without user disruption.

---

## 3. Directory Layout (Monorepo Standard)

```
tauri-plugin-sync/
├── Cargo.toml                    # Workspace root Cargo.toml
├── README.md
├── PRD.md
├── ARCHITECTURE.md
├── ROADMAP.md
├── crates/
│   └── tauri-plugin-sync/        # Main Rust crate
│       ├── Cargo.toml
│       ├── build.rs              # Tauri plugin build script
│       └── src/
│           ├── lib.rs            # Plugin entrypoint & Tauri Builder hooks
│           ├── commands.rs       # IPC command handlers
│           ├── engine.rs         # Background Tokio sync worker
│           ├── cdc.rs            # SQLite trigger injection & change log
│           ├── models.rs         # Changeset and configuration structs
│           └── error.rs          # Plugin error types
├── guest-js/                     # TypeScript SDK
│   ├── package.json
│   ├── tsconfig.json
│   ├── rollup.config.js
│   └── index.ts                  # Public JS/TS API
└── examples/
    └── vanilla/                  # Working test Tauri app demonstrating sync
```
