# Development Roadmap: `tauri-plugin-sync`

---

## Phase 1: Project Scaffolding & Core CDC Engine (Complete)
- [x] Initialize Cargo workspace and crate `crates/tauri-plugin-sync`.
- [x] Set up `guest-js` package with Rollup, TypeScript, and pnpm build tooling.
- [x] Implement `models.rs` (Changeset, SyncConfig, SyncStatus, SyncOperation).
- [x] Implement `cdc.rs` (SQLite shadow table creation, trigger generator for tracked tables).
- [x] Implement `error.rs` with `thiserror` and `serde::Serialize` support for IPC.

## Phase 2: Engine Implementation & Network Pipeline (Complete)
- [x] Implement Tokio background worker in `engine.rs` with configurable polling interval.
- [x] Implement `push` pipeline (read unsynced changesets, serialize to JSON, send HTTP POST).
- [x] Implement `pull` pipeline (request server changes since watermark, suppress trigger feedback loop, apply in atomic transaction).
- [x] Implement basic LWW (Last-Write-Wins) timestamp-based conflict resolution.
- [x] Add network retry logic with exponential backoff on HTTP/TCP errors.

## Phase 3: Guest-JS SDK & IPC Commands (Complete)
- [x] Wire IPC commands in `commands.rs` (`init`, `sync_now`, `get_status`, `configure`).
- [x] Build TypeScript SDK in `guest-js/index.ts` with typed promises and event listeners.
- [x] Configure Tauri v2 permissions (`permissions/default.json`, `build.rs` code generation).

## Phase 4: Test Suite & Example App (Complete)
- [x] Comprehensive Rust unit tests for CDC triggers and changeset serialization.
- [x] In-memory SQLite mock test for concurrent push/pull conflict handling.
- [x] Create `examples/todo/` Tauri app with a todo list demo showing offline writes and cloud sync.
- [x] Enterprise Hardening: Foreign key deferral (`PRAGMA defer_foreign_keys = ON;`).
- [x] Enterprise Hardening: 500-record chunking & pagination preventing initial sync OOM.
- [x] Enterprise Hardening: Auth token lifecycle (HTTP 401 handling, `sync://unauthorized` event, and auto-refresh).
- [x] Enterprise Hardening: Forward schema evolution (auto-filtering & dropping unknown server columns).

## Phase 5: Community Launch & Virality
- [ ] Publish crates to `crates.io` and npm package to `@gupta-ventures/tauri-plugin-sync`.
- [ ] Submit PR to `tauri-apps/awesome-tauri`.
- [ ] Publish launch blog post on Dev.to, Medium, and Reddit (`r/rust`, `r/tauri`).
- [ ] Launch on Hacker News ("Show HN: tauri-plugin-sync").

## Phase 6: v2.0 Vision & Collaborative CRDTs (Scheduled)
- [ ] Field-level JSON Patch delta generation.
- [ ] State-based CRDTs (LWW-Element-Set per column) for concurrent multi-user collaborative editing.
- [ ] WebRTC / P2P direct synchronization mesh.
