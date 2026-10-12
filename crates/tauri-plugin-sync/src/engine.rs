use std::sync::Arc;
use std::time::Duration;
use log::{error, info, warn};
use reqwest::header::{HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use reqwest::StatusCode;
use rusqlite::{params, Connection};
use serde_json::Value;
use tokio::sync::{broadcast, Mutex, RwLock};

use crate::cdc::CdcManager;
use crate::error::{Error, Result};
use crate::models::{
    ChangeRecord, ClientChangeset, ServerChangeset, SyncConfig, SyncOperation, SyncProgressEvent,
    SyncState, SyncStatus,
};

pub struct SyncEngine {
    config: Arc<RwLock<SyncConfig>>,
    status: Arc<RwLock<SyncStatus>>,
    db_conn: Arc<Mutex<Connection>>,
    client_id: String,
    http_client: reqwest::Client,
    status_tx: broadcast::Sender<SyncStatus>,
    progress_tx: broadcast::Sender<SyncProgressEvent>,
    unauthorized_tx: broadcast::Sender<()>,
}

pub type SyncReceivers = (
    broadcast::Receiver<SyncStatus>,
    broadcast::Receiver<SyncProgressEvent>,
    broadcast::Receiver<()>,
);

impl SyncEngine {
    pub fn new(
        conn: Connection,
        config: SyncConfig,
    ) -> Result<(Self, SyncReceivers)> {
        // Initialize shadow tables & concurrency PRAGMAs (WAL, busy_timeout)
        CdcManager::init_shadow_tables(&conn)?;

        // Attach triggers for all tracked tables
        for table in &config.tables {
            CdcManager::setup_triggers(&conn, table)?;
        }

        let client_id = CdcManager::get_or_create_client_id(&conn)?;
        let pending = CdcManager::count_pending_changes(&conn)?;
        let watermark = CdcManager::get_watermark(&conn)?;

        let initial_status = SyncStatus {
            state: SyncState::Idle,
            last_sync_at: if watermark > 0 { Some(watermark) } else { None },
            pending_changes: pending,
            last_error: None,
        };

        let (status_tx, status_rx) = broadcast::channel(32);
        let (progress_tx, progress_rx) = broadcast::channel(32);
        let (unauthorized_tx, unauthorized_rx) = broadcast::channel(16);

        let engine = Self {
            config: Arc::new(RwLock::new(config)),
            status: Arc::new(RwLock::new(initial_status)),
            db_conn: Arc::new(Mutex::new(conn)),
            client_id,
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default(),
            status_tx,
            progress_tx,
            unauthorized_tx,
        };

        Ok((engine, (status_rx, progress_rx, unauthorized_rx)))
    }

    pub fn subscribe_status(&self) -> broadcast::Receiver<SyncStatus> {
        self.status_tx.subscribe()
    }

    pub fn subscribe_progress(&self) -> broadcast::Receiver<SyncProgressEvent> {
        self.progress_tx.subscribe()
    }

    pub fn subscribe_unauthorized(&self) -> broadcast::Receiver<()> {
        self.unauthorized_tx.subscribe()
    }

    pub async fn get_status(&self) -> SyncStatus {
        let mut status = self.status.read().await.clone();
        // Update pending changes count live from db
        let conn = self.db_conn.lock().await;
        if let Ok(count) = CdcManager::count_pending_changes(&conn) {
            status.pending_changes = count;
        }
        status
    }

    pub async fn update_config(&self, new_config: SyncConfig) -> Result<()> {
        let conn = self.db_conn.lock().await;
        // Check new tables and attach triggers
        for table in &new_config.tables {
            CdcManager::setup_triggers(&conn, table)?;
        }
        let mut cfg = self.config.write().await;
        *cfg = new_config;
        Ok(())
    }

    async fn set_state(&self, state: SyncState, last_error: Option<String>) {
        let mut status = self.status.write().await;
        status.state = state;
        if last_error.is_some() {
            status.last_error = last_error;
        }
        let _ = self.status_tx.send(status.clone());
    }

    /// Perform a single complete sync pass (Push, Pull, and Journal Prune)
    pub async fn sync_pass(&self) -> Result<SyncProgressEvent> {
        self.set_state(SyncState::Syncing, None).await;

        let push_result = self.push_changes().await;
        if let Err(err) = push_result {
            let err_msg = Self::sanitize_error_message(&err.to_string());
            warn!("Push phase failed: {}", err_msg);

            let state = match &err {
                Error::Unauthorized(_) => {
                    let _ = self.unauthorized_tx.send(());
                    SyncState::Error
                }
                _ if err_msg.contains("Network") || err_msg.contains("connection") => SyncState::Offline,
                _ => SyncState::Error,
            };

            self.set_state(state, Some(err_msg.clone())).await;
            return Err(err);
        }
        let pushed_count = push_result?;

        let pull_result = self.pull_changes().await;
        if let Err(err) = pull_result {
            let err_msg = Self::sanitize_error_message(&err.to_string());
            warn!("Pull phase failed: {}", err_msg);

            let state = match &err {
                Error::Unauthorized(_) => {
                    let _ = self.unauthorized_tx.send(());
                    SyncState::Error
                }
                _ if err_msg.contains("Network") || err_msg.contains("connection") => SyncState::Offline,
                _ => SyncState::Error,
            };

            self.set_state(state, Some(err_msg.clone())).await;
            return Err(err);
        }
        let (pulled_count, new_watermark) = pull_result?;

        // Automatic pruning of old synced journal entries to protect disk space
        self.prune_old_journal_entries().await;

        // Update successful status
        {
            let mut status = self.status.write().await;
            status.state = SyncState::Idle;
            status.last_sync_at = Some(chrono::Utc::now().timestamp_millis());
            status.last_error = None;
            let conn = self.db_conn.lock().await;
            if let Ok(count) = CdcManager::count_pending_changes(&conn) {
                status.pending_changes = count;
            }
            let _ = self.status_tx.send(status.clone());
        }

        let progress = SyncProgressEvent {
            pushed_count,
            pulled_count,
            watermark: new_watermark,
        };
        let _ = self.progress_tx.send(progress.clone());

        Ok(progress)
    }

    /// Prune synced entries older than configured `prune_days`
    async fn prune_old_journal_entries(&self) {
        let prune_days = {
            let cfg = self.config.read().await;
            cfg.prune_days
        };

        if prune_days > 0 {
            let cutoff_millis = chrono::Utc::now().timestamp_millis()
                - (prune_days as i64 * 24 * 60 * 60 * 1000);
            let conn = self.db_conn.lock().await;
            if let Ok(deleted) = CdcManager::prune_synced_changes(&conn, cutoff_millis) {
                if deleted > 0 {
                    info!("Pruned {} old synced journal entries older than {} days", deleted, prune_days);
                }
            }
        }
    }

    /// Sanitize error messages to ensure sensitive tokens/headers are never leaked
    fn sanitize_error_message(msg: &str) -> String {
        // Redact any 'Bearer ...' patterns
        let mut sanitized = msg.to_string();
        if let Some(bearer_idx) = sanitized.to_lowercase().find("bearer ") {
            let end_idx = sanitized[bearer_idx..]
                .find(|c: char| c.is_whitespace() || c == '"' || c == '\'')
                .map(|offset| bearer_idx + offset)
                .unwrap_or(sanitized.len());
            sanitized.replace_range(bearer_idx..end_idx, "Bearer [REDACTED]");
        }
        sanitized
    }

    /// Push local changes to the remote sync endpoint in chunked batches
    async fn push_changes(&self) -> Result<usize> {
        let (config, batch_size) = {
            let cfg = self.config.read().await;
            (cfg.clone(), cfg.batch_size)
        };

        let mut total_pushed = 0;

        loop {
            let (changes, watermark) = {
                let conn = self.db_conn.lock().await;
                let pending = CdcManager::get_pending_changes(&conn, batch_size)?;
                let wm = CdcManager::get_watermark(&conn)?;
                (pending, wm)
            };

            if changes.is_empty() {
                break;
            }

            let max_change_id = changes.last().map(|c| c.id).unwrap_or(0);
            let batch_len = changes.len();

            let payload = ClientChangeset {
                client_id: self.client_id.clone(),
                watermark,
                changes,
            };

            let push_url = format!("{}/push", config.endpoint.trim_end_matches('/'));
            let mut request = self
                .http_client
                .post(&push_url)
                .header(CONTENT_TYPE, "application/json");

            if let Some(auth) = &config.auth_header {
                if let Ok(header_val) = HeaderValue::from_str(auth) {
                    request = request.header(AUTHORIZATION, header_val);
                }
            }

            let response = request
                .json(&payload)
                .send()
                .await
                .map_err(Error::Network)?;

            if response.status() == StatusCode::UNAUTHORIZED {
                let err_text = response.text().await.unwrap_or_default();
                let clean_err = Self::sanitize_error_message(&err_text);
                return Err(Error::Unauthorized(clean_err));
            }

            if !response.status().is_success() {
                let status_code = response.status();
                let err_text = response.text().await.unwrap_or_default();
                let clean_err = Self::sanitize_error_message(&err_text);
                return Err(Error::Engine(format!(
                    "Remote push failed with HTTP {}: {}",
                    status_code, clean_err
                )));
            }

            // Mark changes as synced in database
            {
                let conn = self.db_conn.lock().await;
                CdcManager::mark_changes_synced(&conn, max_change_id)?;
            }

            total_pushed += batch_len;

            // If batch was smaller than limit, we're done with pending changes
            if batch_len < batch_size {
                break;
            }
        }

        Ok(total_pushed)
    }

    /// Pull remote changes in chunked batches and apply locally inside deferred foreign key transactions
    async fn pull_changes(&self) -> Result<(usize, i64)> {
        let (config, batch_size) = {
            let cfg = self.config.read().await;
            (cfg.clone(), cfg.batch_size)
        };

        let mut total_pulled = 0;
        let mut last_known_watermark = {
            let conn = self.db_conn.lock().await;
            CdcManager::get_watermark(&conn)?
        };

        loop {
            let pull_url = format!("{}/pull", config.endpoint.trim_end_matches('/'));
            let mut request = self
                .http_client
                .get(&pull_url)
                .query(&[
                    ("since", last_known_watermark.to_string()),
                    ("client_id", self.client_id.clone()),
                    ("limit", batch_size.to_string()),
                ])
                .header(CONTENT_TYPE, "application/json");

            if let Some(auth) = &config.auth_header {
                if let Ok(header_val) = HeaderValue::from_str(auth) {
                    request = request.header(AUTHORIZATION, header_val);
                }
            }

            let response = request.send().await.map_err(Error::Network)?;

            if response.status() == StatusCode::UNAUTHORIZED {
                let err_text = response.text().await.unwrap_or_default();
                let clean_err = Self::sanitize_error_message(&err_text);
                return Err(Error::Unauthorized(clean_err));
            }

            if !response.status().is_success() {
                let status_code = response.status();
                let err_text = response.text().await.unwrap_or_default();
                let clean_err = Self::sanitize_error_message(&err_text);
                return Err(Error::Engine(format!(
                    "Remote pull failed with HTTP {}: {}",
                    status_code, clean_err
                )));
            }

            let server_changeset: ServerChangeset = response
                .json()
                .await
                .map_err(|e| Error::Engine(format!("Failed to parse server changeset: {}", e)))?;

            let batch_count = server_changeset.changes.len();

            if batch_count > 0 || server_changeset.watermark > last_known_watermark {
                let mut conn = self.db_conn.lock().await;

                // Execute inside an atomic transaction with deferred FK constraints and CDC suppression
                let tx = conn.transaction()?;

                // Defer foreign key checks until transaction commit
                tx.execute_batch("PRAGMA defer_foreign_keys = ON;")?;

                // 1. Enable trigger suppression so applied changes aren't logged as new local mutations
                CdcManager::set_suppression(&tx, true)?;

                for record in &server_changeset.changes {
                    Self::apply_remote_record(&tx, record)?;
                }

                // 2. Advance watermark
                CdcManager::set_watermark(&tx, server_changeset.watermark)?;

                // 3. Disable suppression before committing
                CdcManager::set_suppression(&tx, false)?;

                tx.commit()?;
                last_known_watermark = server_changeset.watermark;
            }

            total_pulled += batch_count;

            // If returned fewer records than batch_size, we've pulled all up-to-date changes
            if batch_count < batch_size {
                break;
            }
        }

        Ok((total_pulled, last_known_watermark))
    }

    /// Apply a single remote change record with LWW conflict resolution, tombstone checking,
    /// and schema evolution column filtering (drops unknown server columns cleanly).
    pub fn apply_remote_record(conn: &Connection, record: &ChangeRecord) -> Result<()> {
        let pk = CdcManager::get_primary_key(conn, &record.table)?;

        // Check if there is an un-synced local change for this exact row
        let local_conflict: Option<(i64, i64)> = conn
            .query_row(
                "SELECT id, timestamp FROM _sync_changes 
                 WHERE table_name = ?1 AND row_id = ?2 AND synced = 0
                 ORDER BY id DESC LIMIT 1;",
                params![record.table, record.row_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .ok();

        if let Some((_local_id, local_timestamp)) = local_conflict {
            // Last-Write-Wins: if local timestamp is higher, local wins, skip remote overwrite
            if local_timestamp >= record.timestamp {
                info!(
                    "Conflict resolved: local modification at {} supersedes remote at {} for {}:{}",
                    local_timestamp, record.timestamp, record.table, record.row_id
                );
                return Ok(());
            }
        }

        // Tombstone check: check if row was deleted locally or previously
        if let Ok(Some(deleted_at)) = CdcManager::get_tombstone(conn, &record.table, &record.row_id) {
            // If deletion happened at or after the remote record's timestamp, tombstone wins: do NOT resurrect
            if deleted_at >= record.timestamp {
                info!(
                    "Tombstone wins: row {}:{} was deleted at {}, ignoring older remote mutation at {}",
                    record.table, record.row_id, deleted_at, record.timestamp
                );
                return Ok(());
            } else {
                // Remote mutation is newer than tombstone: resurrect/recreate row and remove tombstone
                let _ = CdcManager::remove_tombstone(conn, &record.table, &record.row_id);
            }
        }

        let quoted_table = CdcManager::quote_ident(&record.table);
        let quoted_pk = CdcManager::quote_ident(&pk);

        match record.op {
            SyncOperation::Delete => {
                let sql = format!(
                    "DELETE FROM {} WHERE {} = ?1;",
                    quoted_table, quoted_pk
                );
                conn.execute(&sql, params![record.row_id])?;
                // Record tombstone for this deletion
                CdcManager::record_tombstone(conn, &record.table, &record.row_id, record.timestamp)?;
            }
            SyncOperation::Insert | SyncOperation::Update => {
                if let Some(Value::Object(map)) = &record.data {
                    if map.is_empty() {
                        return Ok(());
                    }

                    // Query actual existing columns in local table (resilience against server schema additions)
                    let existing_columns = CdcManager::get_table_columns(conn, &record.table)?;

                    // Filter incoming map keys to only those that exist locally
                    let valid_columns: Vec<String> = map
                        .keys()
                        .filter(|k| existing_columns.contains(k))
                        .cloned()
                        .collect();

                    if valid_columns.is_empty() {
                        return Ok(());
                    }

                    let placeholders: Vec<String> = (1..=valid_columns.len()).map(|i| format!("?{}", i)).collect();
                    let col_names = valid_columns
                        .iter()
                        .map(|c| CdcManager::quote_ident(c))
                        .collect::<Vec<_>>()
                        .join(", ");

                    // INSERT OR REPLACE INTO "table" ("col1", "col2") VALUES (?1, ?2)
                    let sql = format!(
                        "INSERT OR REPLACE INTO {} ({}) VALUES ({});",
                        quoted_table,
                        col_names,
                        placeholders.join(", ")
                    );

                    let mut stmt = conn.prepare(&sql)?;
                    let params_vec: Vec<rusqlite::types::Value> = valid_columns
                        .iter()
                        .map(|c| match map.get(c) {
                            Some(Value::Null) | None => rusqlite::types::Value::Null,
                            Some(Value::Bool(b)) => rusqlite::types::Value::Integer(if *b { 1 } else { 0 }),
                            Some(Value::Number(n)) => {
                                if let Some(i) = n.as_i64() {
                                    rusqlite::types::Value::Integer(i)
                                } else if let Some(f) = n.as_f64() {
                                    rusqlite::types::Value::Real(f)
                                } else {
                                    rusqlite::types::Value::Text(n.to_string())
                                }
                            }
                            Some(Value::String(s)) => rusqlite::types::Value::Text(s.clone()),
                            Some(other) => rusqlite::types::Value::Text(other.to_string()),
                        })
                        .collect();

                    let rusqlite_params = rusqlite::params_from_iter(params_vec);
                    stmt.execute(rusqlite_params)?;
                }
            }
        }

        Ok(())
    }

    /// Background worker loop that polls and performs sync passes with exponential backoff
    pub async fn run_loop(self: Arc<Self>, mut stop_rx: tokio::sync::oneshot::Receiver<()>) {
        info!("SyncEngine background loop starting for client: {}", self.client_id);
        let mut backoff = Duration::from_millis(1000);
        let max_backoff = Duration::from_secs(60);

        loop {
            let interval_ms = {
                let cfg = self.config.read().await;
                cfg.sync_interval_ms
            };

            let interval_dur = Duration::from_millis(interval_ms);

            tokio::select! {
                _ = &mut stop_rx => {
                    info!("SyncEngine background loop received stop signal");
                    break;
                }
                _ = tokio::time::sleep(if backoff > Duration::from_millis(1000) { backoff } else { interval_dur }) => {
                    match self.sync_pass().await {
                        Ok(_) => {
                            // Reset backoff on success
                            backoff = Duration::from_millis(1000);
                        }
                        Err(err) => {
                            error!("Sync pass error: {}", err);
                            // Increase backoff
                            backoff = std::cmp::min(backoff * 2, max_backoff);
                        }
                    }
                }
            }
        }
        info!("SyncEngine background loop exited");
    }
}
