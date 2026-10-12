# Tauri Plugin Sync — Todo Example App

A complete, working offline-first desktop Todo application demonstrating **0ms local SQLite latency**, simulated offline network disconnection, transparent SQLite CDC shadow triggers, and background replication.

---

## ⚡ Quickstart (Run in 60 Seconds)

### Prerequisites
- [Rust toolchain](https://www.rust-lang.org/tools/install) installed (`rustc`, `cargo`).
- Tauri CLI installed: `cargo install tauri-cli --version "^2.0"` (or use `cargo tauri`).

### Launch the App
From the root of this repository:

```bash
cd examples/todo/src-tauri
cargo tauri dev
```

---

## 🎮 What This Demo Shows

1. **0ms Local UI Latency:**
   - Every Todo item you add, toggle, or delete writes directly to a local embedded SQLite database (`todos.db`).
   - The UI responds with **zero network latency** and instant optimistic updates.

2. **Offline Simulation Toggle:**
   - Click the **"Simulate Offline"** button in the top-right corner.
   - Continue adding, editing, and deleting items while offline.
   - Inspect the SQLite database — all mutations are safely stored locally in `todos` and queued in `_sync_changes` via SQLite shadow triggers.

3. **Instant Auto-Replication on Reconnect:**
   - Toggle **"Go Online"**.
   - The background Tokio sync actor automatically flushes the local changeset queue to the cloud endpoint with bounded chunking and LWW conflict resolution.

---

## 📂 Project Structure

```text
examples/todo/
├── src-tauri/
│   ├── Cargo.toml          # Rust dependencies (tauri v2, rusqlite, tauri-plugin-sync)
│   ├── tauri.conf.json     # Tauri v2 desktop window & bundle configuration
│   └── src/
│       └── main.rs         # SQLite initialization, schema setup, and plugin attachment
└── ui/
    └── index.html          # Lightweight modern dark-mode frontend with offline toggle
```
