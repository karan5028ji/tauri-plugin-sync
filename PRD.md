# Product Requirements Document (PRD)

## Project: `tauri-plugin-sync`
**Subtitle:** Zero-Configuration Local-First SQLite Replication & Sync Engine for Tauri v2  
**Author:** Karan Gupta (Founder, Gupta Ventures)  
**Status:** In Development (v0.1.0 Scaffolding)  
**Target Ecosystem:** Tauri v2, Rust, TypeScript, Cross-Platform Desktop (Windows, macOS, Linux)

---

## 1. Executive Summary & Vision

### 1.1 The Problem
Building offline-first desktop applications with Tauri today is needlessly painful. While `@tauri-apps/plugin-sql` provides local SQLite query execution, desktop apps (such as Notion, Linear, Obsidian, and SaaS dashboards) require two-way data replication between the local SQLite database and remote cloud backends (Supabase, Postgres, SQLite Cloud, custom REST/WebSocket servers).
Currently, developers must manually invent custom sync protocols, write complex delta-tracking logic, handle connection drops, resolve conflicts, and manage offline queues. This barrier prevents widespread adoption of local-first desktop apps.

### 1.2 The Solution
`tauri-plugin-sync` is an official-grade, drop-in Tauri v2 plugin that provides:
1. **Automatic Change Data Capture (CDC):** Transparently monitors SQLite database mutations (INSERT, UPDATE, DELETE) via high-performance SQLite shadow triggers and a compact `_sync_changes` journal.
2. **Offline-Resilient Replication Engine:** Queues mutations when offline, and automatically syncs changesets with exponential backoff and batching when internet connectivity resumes.
3. **Pluggable Conflict Resolution:** Deterministic Last-Write-Wins (LWW) with Lamport timestamps and tombstone deletions, with an extensible hook for custom business logic.
4. **Universal Backend Adapters:** Protocol-agnostic changeset exchange format compatible with Supabase, PostgreSQL, HTTP/REST endpoints, and WebSockets.
5. **Ergonomic TypeScript / Guest-JS SDK:** Zero-boilerplate frontend API with reactive sync status hooks (`idle`, `syncing`, `offline`, `error`).

---

## 2. Target Audience & Developer Persona
- **Tauri v2 Desktop Developers:** Developers building productivity software, note-taking apps, CRMs, personal finance tools, and offline-capable utilities.
- **Enterprise / B2B SaaS Teams:** Teams seeking instant UI responsiveness (<16ms 60fps local queries) without waiting for network latency.
- **Indie Hackers & Founders:** Creators looking for drop-in offline sync without having to implement complex CRDT engines from scratch.

---

## 3. Product Scope & Functional Requirements

### 3.1 Core Features (P0 - MVP)
- **FR-01: Auto-Track Tables:** Developers can specify one or more tables to track (e.g. `['todos', 'notes', 'users']`). The plugin automatically attaches triggers and tracks primary keys, column deltas, and timestamps.
- **FR-02: Local Changeset Log (`_sync_log`):** Tracks operation type (`INSERT`, `UPDATE`, `DELETE`), table name, row primary key, JSON payload or delta, Lamport/wall timestamp, and sync status (`pending`, `synced`).
- **FR-03: Soft Deletes / Tombstones:** Automatic handling of record deletions so delete operations propagate across clients without losing sync state.
- **FR-04: Bidirectional Sync Pipeline:**
  - **Push:** Batches pending local changesets and sends them to the remote sync endpoint.
  - **Pull:** Fetches remote changesets created after the last known sync watermark and applies them to local SQLite inside an atomic transaction.
- **FR-05: Network State Detection & Auto-Retry:** Seamless transition between online and offline states, queuing all writes and auto-triggering sync on reconnect.
- **FR-06: Guest-JS / TS SDK:** Idiomatic TypeScript client package with full typings and event listeners for real-time status.

### 3.2 Advanced Features (P1 - Fast Follow)
- **FR-07: Selective Sync & Scopes:** Sync only data belonging to the authenticated user ID (`tenant_id` / `user_id` filtering).
- **FR-08: Real-Time Sync via WebSocket:** Instant push notifications when remote changes occur rather than polling.
- **FR-09: Built-in Supabase / Postgres Adapter:** First-class connector for Supabase Realtime / Postgres change streams.

---

## 4. Non-Functional Requirements

- **Performance:**
  - Local query execution overhead caused by triggers must be `< 2%`.
  - Push/pull operations must run in background Rust threads via Tokio, never blocking the main Tauri IPC or WebView UI thread.
- **Reliability & Idempotency:**
  - All changeset applications must be idempotent. Re-applying the same changeset must not corrupt state.
  - SQLite transactions (`BEGIN IMMEDIATE ... COMMIT`) must guarantee local ACID safety.
- **Memory Footprint:**
  - Idle RAM consumption of the sync background worker must stay below **10 MB**.
- **Security:**
  - Follow Tauri v2 permission system guidelines with default capabilities and granular command permissions.
  - Support authentication headers (Bearer tokens / API keys) passed securely via Rust config or IPC.

---

## 5. Developer Experience (DX) & Example Usage

```typescript
import { SyncManager } from 'tauri-plugin-sync';

// 1. Initialize Sync on an existing SQLite database
const sync = await SyncManager.init({
  database: 'myapp.db',
  endpoint: 'https://api.myapp.com/sync',
  tables: ['tasks', 'categories'],
  authHeader: 'Bearer eyJhbGciOi...',
  syncIntervalMs: 5000, // automatic sync interval (or manual trigger)
});

// 2. Listen to sync status
sync.onStatusChange((status) => {
  console.log('Sync status:', status); // 'idle' | 'syncing' | 'offline' | 'error'
});

// 3. Trigger manual sync or let it run automatically
await sync.syncNow();
```

---

## 6. Success Metrics & KPIs
1. **GitHub Stars:** 500+ within 30 days of community release.
2. **Official Tauri Recognition:** Listed on `tauri-apps/awesome-tauri` and featured in official newsletter.
3. **npm & Crates.io Adoption:** 2,000+ monthly downloads within 60 days.
4. **Community Feedback:** Mentioned across X/Twitter and Reddit as the de-facto sync solution for Tauri v2.
