# tauri-plugin-sync

> **Zero-Configuration Local-First SQLite Replication & Sync Engine for Tauri v2.**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tauri v2](https://img.shields.io/badge/Tauri-v2-blue.svg)](https://v2.tauri.app)
[![CI](https://github.com/karan5028ji/tauri-plugin-sync/actions/workflows/ci.yml/badge.svg)](https://github.com/karan5028ji/tauri-plugin-sync/actions)

`tauri-plugin-sync` enables developers to build real-time, offline-first desktop applications with Tauri v2. Write to local SQLite with 0ms latency, and let the background sync engine seamlessly replicate changesets to your remote cloud backend (Supabase, Postgres, or custom REST/WebSocket APIs) with automatic conflict resolution.

---

## Features

- ⚡ **Zero-Latency Local Writes:** Query local SQLite directly at native speeds.
- 🔄 **Automatic Change Data Capture (CDC):** Shadow triggers track inserts, updates, and deletes without changing existing application SQL.
- 📴 **Offline-First Resilience:** Network disconnects queue mutations locally; auto-sync resumes instantly when back online.
- 🛡️ **Conflict Resolution:** Deterministic Last-Write-Wins (LWW) with Lamport timestamps and tombstone deletion support.
- 🌐 **Universal Backend Adapters:** Protocol-agnostic changeset exchange format.
- 📦 **Modern TypeScript SDK:** Full TypeScript definitions and reactive status listeners.

---

## Installation

> [!TIP]
> **Pre-Release Review:** Ahead of locking the immutable `v0.1.0` release on crates.io and npm, early testers and maintainers can install directly via Git dependencies.

### 1. Add Rust Dependency
In your `src-tauri/Cargo.toml`:

```toml
# Git dependency (Pre-release testing)
[dependencies]
tauri-plugin-sync = { git = "https://github.com/karan5028ji/tauri-plugin-sync", branch = "main" }

# Or from crates.io (Once v0.1.0 is published)
# tauri-plugin-sync = "0.1"
```

### 2. Initialize in Tauri Builder
Configure database, endpoints, and credentials securely in `src-tauri/src/lib.rs` (or `main.rs`):

```rust
use tauri_plugin_sync::Builder as SyncBuilder;

pub fn run() {
    tauri::Builder::default()
        .plugin(
            SyncBuilder::new()
                .database("app.db")
                .endpoint("https://api.example.com/sync")
                .bearer_token("YOUR_AUTH_TOKEN") // Securely managed in Rust backend (XSS-safe)
                .tables(vec!["notes".into(), "todos".into()])
                .sync_interval(std::time::Duration::from_secs(5))
                .build()
        )
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
```

### 3. Add Guest-JS SDK
In your frontend project:

```bash
# Git dependency (Pre-release testing)
npm install "https://github.com/karan5028ji/tauri-plugin-sync#main"
# or
pnpm add "github:karan5028ji/tauri-plugin-sync#main"

# Or from npm registry (Once published)
# npm install tauri-plugin-sync
```

---

## Quick Start

```typescript
import { SyncManager, syncNow, getStatus } from 'tauri-plugin-sync';

// Zero-configuration: connects directly to the Rust-configured sync engine
const sync = await SyncManager.init();

// Listen to real-time sync status events
sync.onStatusChange((status) => {
  console.log('Current status:', status); // 'idle' | 'syncing' | 'offline' | 'error'
});

// Trigger an immediate sync pass
await sync.syncNow();
```

---

## Architecture & Documentation

- [Product Requirements Document (PRD)](./PRD.md)
- [Technical Architecture](./ARCHITECTURE.md)
- [Roadmap](./ROADMAP.md)

---

## 🚀 Ecosystem Extensions & Upcoming Adapters

`tauri-plugin-sync` core is **100% Free & Open Source under the MIT License**. To support complex production setups, we are developing turnkey backend adapters and enterprise capabilities:

- 🟣 **Supabase & PostgreSQL Cloud Adapter:** Pre-built SQL migrations, Edge Functions, and Row Level Security (RLS) integration.
- 🟡 **Gupta Ventures Managed Sync Engine:** Zero-backend hosted synchronization endpoint for rapid prototyping.
- 🔒 **Enterprise E2EE (Zero-Knowledge):** Client-side AES-256-GCM encryption for privacy, healthcare, and finance apps.

👉 **[Join the Early Access Waitlist](https://karan5028ji.github.io/tauri-plugin-sync/#waitlist)** to get notified when turnkey adapters launch or to request specific backend connectors.

---

## Authors & Maintainers
- **Karan Gupta** ([@karan5028ji](https://github.com/karan5028ji)) - Founder, Gupta Ventures
- Open-source contributors welcome!

---

## License
MIT License. See [LICENSE](./LICENSE) for details.
