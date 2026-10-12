use rusqlite::{params, Connection};
use serde_json::Value;
use std::str::FromStr;

use crate::error::{Error, Result};
use crate::models::{ChangeRecord, SyncOperation};

pub struct CdcManager;

impl CdcManager {
    /// Escape double quotes in SQL identifiers (tables, columns, triggers)
    pub fn quote_ident(ident: &str) -> String {
        format!("\"{}\"", ident.replace('"', "\"\""))
    }

    /// Configure performance & concurrency PRAGMAs: WAL mode and busy_timeout
    pub fn configure_concurrency(conn: &Connection) -> Result<()> {
        conn.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA busy_timeout = 5000;
            PRAGMA synchronous = NORMAL;
            "#,
        )?;
        Ok(())
    }

    /// Initialize shadow tables required for CDC and metadata tracking.
    pub fn init_shadow_tables(conn: &Connection) -> Result<()> {
        Self::configure_concurrency(conn)?;

        conn.execute_batch(
            r#"
            -- Metadata table tracking sync client ID, watermark, and configurations
            CREATE TABLE IF NOT EXISTS _sync_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            -- Table to track CDC changes
            CREATE TABLE IF NOT EXISTS _sync_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT NOT NULL,
                row_id TEXT NOT NULL,
                operation TEXT NOT NULL, -- 'INSERT', 'UPDATE', 'DELETE'
                data TEXT,               -- JSON serialized row snapshot
                timestamp INTEGER NOT NULL,
                synced INTEGER DEFAULT 0 -- 0: pending, 1: synced
            );

            CREATE INDEX IF NOT EXISTS idx_sync_changes_pending 
            ON _sync_changes(synced, id ASC);

            CREATE INDEX IF NOT EXISTS idx_sync_changes_table_row 
            ON _sync_changes(table_name, row_id);

            -- Tombstones table tracking deleted rows to prevent resurrected updates
            CREATE TABLE IF NOT EXISTS _sync_tombstones (
                table_name TEXT NOT NULL,
                row_id TEXT NOT NULL,
                deleted_at INTEGER NOT NULL,
                PRIMARY KEY (table_name, row_id)
            );

            CREATE INDEX IF NOT EXISTS idx_sync_tombstones_deleted_at
            ON _sync_tombstones(deleted_at);

            -- Suppression table / session flag to prevent trigger loop during remote pulls
            CREATE TABLE IF NOT EXISTS _sync_suppress (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                suppressed INTEGER NOT NULL DEFAULT 0
            );

            INSERT OR IGNORE INTO _sync_suppress (id, suppressed) VALUES (1, 0);
            "#,
        )?;
        Ok(())
    }

    /// Set suppression state (1 = suppressed / CDC triggers will not log, 0 = active)
    pub fn set_suppression(conn: &Connection, suppressed: bool) -> Result<()> {
        let flag = if suppressed { 1 } else { 0 };
        conn.execute(
            "UPDATE _sync_suppress SET suppressed = ?1 WHERE id = 1;",
            params![flag],
        )?;
        Ok(())
    }

    /// Check if triggers are currently suppressed
    pub fn is_suppressed(conn: &Connection) -> Result<bool> {
        let flag: i32 = conn.query_row(
            "SELECT suppressed FROM _sync_suppress WHERE id = 1;",
            [],
            |row| row.get(0),
        )?;
        Ok(flag == 1)
    }

    /// Record a tombstone deletion timestamp for a row
    pub fn record_tombstone(conn: &Connection, table_name: &str, row_id: &str, timestamp: i64) -> Result<()> {
        conn.execute(
            "INSERT OR REPLACE INTO _sync_tombstones (table_name, row_id, deleted_at) VALUES (?1, ?2, ?3);",
            params![table_name, row_id, timestamp],
        )?;
        Ok(())
    }

    /// Check if a tombstone exists for a row and return the deletion timestamp
    pub fn get_tombstone(conn: &Connection, table_name: &str, row_id: &str) -> Result<Option<i64>> {
        let res: std::result::Result<i64, rusqlite::Error> = conn.query_row(
            "SELECT deleted_at FROM _sync_tombstones WHERE table_name = ?1 AND row_id = ?2;",
            params![table_name, row_id],
            |row| row.get(0),
        );

        match res {
            Ok(ts) => Ok(Some(ts)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(Error::Database(e)),
        }
    }

    /// Remove a tombstone when a row is recreated with a newer timestamp
    pub fn remove_tombstone(conn: &Connection, table_name: &str, row_id: &str) -> Result<()> {
        conn.execute(
            "DELETE FROM _sync_tombstones WHERE table_name = ?1 AND row_id = ?2;",
            params![table_name, row_id],
        )?;
        Ok(())
    }

    /// Detect primary key column for a table via table_info PRAGMA
    pub fn get_primary_key(conn: &Connection, table_name: &str) -> Result<String> {
        let quoted_tbl = Self::quote_ident(table_name);
        let mut stmt = conn.prepare(&format!("PRAGMA table_info({});", quoted_tbl))?;
        let rows = stmt.query_map([], |row| {
            let col_name: String = row.get(1)?;
            let pk_index: i32 = row.get(5)?;
            Ok((col_name, pk_index))
        })?;

        let mut primary_keys = Vec::new();
        for r in rows {
            let (name, pk) = r?;
            if pk > 0 {
                primary_keys.push((name, pk));
            }
        }

        if primary_keys.is_empty() {
            // Default to 'id' or 'rowid' if no explicit PK defined
            Ok("id".to_string())
        } else {
            // Sort by pk index (for composite keys or single key)
            primary_keys.sort_by_key(|k| k.1);
            Ok(primary_keys[0].0.clone())
        }
    }

    /// Get all column names for a given table
    pub fn get_table_columns(conn: &Connection, table_name: &str) -> Result<Vec<String>> {
        let quoted_tbl = Self::quote_ident(table_name);
        let mut stmt = conn.prepare(&format!("PRAGMA table_info({});", quoted_tbl))?;
        let rows = stmt.query_map([], |row| {
            let col_name: String = row.get(1)?;
            Ok(col_name)
        })?;

        let mut cols = Vec::new();
        for r in rows {
            cols.push(r?);
        }
        Ok(cols)
    }

    /// Attach AFTER INSERT, AFTER UPDATE, AFTER DELETE triggers to user table
    pub fn setup_triggers(conn: &Connection, table_name: &str) -> Result<()> {
        let pk = Self::get_primary_key(conn, table_name)?;
        let cols = Self::get_table_columns(conn, table_name)?;

        if cols.is_empty() {
            return Err(Error::Config(format!(
                "Table '{}' has no columns or does not exist",
                table_name
            )));
        }

        let quoted_table = Self::quote_ident(table_name);
        let quoted_pk = Self::quote_ident(&pk);

        // Escape table name for literal inclusion in SQLite string literals: ' -> ''
        let escaped_table_literal = table_name.replace('\'', "''");

        // Build json_object arguments: 'col1', NEW."col1", 'col2', NEW."col2"...
        let json_new_pairs: Vec<String> = cols
            .iter()
            .map(|c| {
                let escaped_c_lit = c.replace('\'', "''");
                let quoted_c = Self::quote_ident(c);
                format!("'{}', NEW.{}", escaped_c_lit, quoted_c)
            })
            .collect();
        let json_new_expr = format!("json_object({})", json_new_pairs.join(", "));

        let json_old_pairs: Vec<String> = cols
            .iter()
            .map(|c| {
                let escaped_c_lit = c.replace('\'', "''");
                let quoted_c = Self::quote_ident(c);
                format!("'{}', OLD.{}", escaped_c_lit, quoted_c)
            })
            .collect();
        let json_old_expr = format!("json_object({})", json_old_pairs.join(", "));

        let insert_trigger_name = format!("_sync_trg_{}_ai", table_name);
        let update_trigger_name = format!("_sync_trg_{}_au", table_name);
        let delete_trigger_name = format!("_sync_trg_{}_ad", table_name);

        let quoted_insert_trg = Self::quote_ident(&insert_trigger_name);
        let quoted_update_trg = Self::quote_ident(&update_trigger_name);
        let quoted_delete_trg = Self::quote_ident(&delete_trigger_name);

        // Current epoch millisecond timestamp expression:
        let timestamp_expr = "(strftime('%s', 'now') * 1000 + CAST(substr(strftime('%f', 'now'), 4, 3) AS INTEGER))";

        let trigger_sql = format!(
            r#"
            DROP TRIGGER IF EXISTS {quoted_insert_trg};
            CREATE TRIGGER {quoted_insert_trg}
            AFTER INSERT ON {quoted_table}
            WHEN (SELECT suppressed FROM _sync_suppress WHERE id = 1) = 0
            BEGIN
                INSERT INTO _sync_changes (table_name, row_id, operation, data, timestamp, synced)
                VALUES (
                    '{table_literal}',
                    CAST(NEW.{quoted_pk} AS TEXT),
                    'INSERT',
                    {json_new_expr},
                    {timestamp_expr},
                    0
                );
            END;

            DROP TRIGGER IF EXISTS {quoted_update_trg};
            CREATE TRIGGER {quoted_update_trg}
            AFTER UPDATE ON {quoted_table}
            WHEN (SELECT suppressed FROM _sync_suppress WHERE id = 1) = 0
            BEGIN
                INSERT INTO _sync_changes (table_name, row_id, operation, data, timestamp, synced)
                VALUES (
                    '{table_literal}',
                    CAST(NEW.{quoted_pk} AS TEXT),
                    'UPDATE',
                    {json_new_expr},
                    {timestamp_expr},
                    0
                );
            END;

            DROP TRIGGER IF EXISTS {quoted_delete_trg};
            CREATE TRIGGER {quoted_delete_trg}
            AFTER DELETE ON {quoted_table}
            WHEN (SELECT suppressed FROM _sync_suppress WHERE id = 1) = 0
            BEGIN
                INSERT INTO _sync_changes (table_name, row_id, operation, data, timestamp, synced)
                VALUES (
                    '{table_literal}',
                    CAST(OLD.{quoted_pk} AS TEXT),
                    'DELETE',
                    {json_old_expr},
                    {timestamp_expr},
                    0
                );
                INSERT OR REPLACE INTO _sync_tombstones (table_name, row_id, deleted_at)
                VALUES ('{table_literal}', CAST(OLD.{quoted_pk} AS TEXT), {timestamp_expr});
            END;
            "#,
            quoted_insert_trg = quoted_insert_trg,
            quoted_update_trg = quoted_update_trg,
            quoted_delete_trg = quoted_delete_trg,
            quoted_table = quoted_table,
            table_literal = escaped_table_literal,
            quoted_pk = quoted_pk,
            json_new_expr = json_new_expr,
            json_old_expr = json_old_expr,
            timestamp_expr = timestamp_expr,
        );

        conn.execute_batch(&trigger_sql)?;
        Ok(())
    }

    /// Fetch unsynced changes up to `batch_size`
    pub fn get_pending_changes(conn: &Connection, limit: usize) -> Result<Vec<ChangeRecord>> {
        let mut stmt = conn.prepare(
            "SELECT id, table_name, row_id, operation, data, timestamp
             FROM _sync_changes
             WHERE synced = 0
             ORDER BY id ASC
             LIMIT ?1;",
        )?;

        let rows = stmt.query_map(params![limit as i64], |row| {
            let id: i64 = row.get(0)?;
            let table: String = row.get(1)?;
            let row_id: String = row.get(2)?;
            let op_str: String = row.get(3)?;
            let data_str: Option<String> = row.get(4)?;
            let timestamp: i64 = row.get(5)?;

            let op = SyncOperation::from_str(&op_str)
                .unwrap_or(SyncOperation::Insert);
            let data: Option<Value> = data_str.and_then(|s| serde_json::from_str(&s).ok());

            Ok(ChangeRecord {
                id,
                table,
                row_id,
                op,
                data,
                timestamp,
            })
        })?;

        let mut results = Vec::new();
        for r in rows {
            results.push(r?);
        }
        Ok(results)
    }

    /// Count pending changes
    pub fn count_pending_changes(conn: &Connection) -> Result<usize> {
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM _sync_changes WHERE synced = 0;",
            [],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    /// Mark changes as synced up to a given change ID
    pub fn mark_changes_synced(conn: &Connection, max_id: i64) -> Result<()> {
        conn.execute(
            "UPDATE _sync_changes SET synced = 1 WHERE id <= ?1 AND synced = 0;",
            params![max_id],
        )?;
        Ok(())
    }

    /// Compact synced changes older than a specified timestamp (prune journal)
    pub fn prune_synced_changes(conn: &Connection, before_timestamp: i64) -> Result<usize> {
        let deleted = conn.execute(
            "DELETE FROM _sync_changes WHERE synced = 1 AND timestamp < ?1;",
            params![before_timestamp],
        )?;
        Ok(deleted)
    }

    /// Get client ID from `_sync_meta` or generate and save a new one
    pub fn get_or_create_client_id(conn: &Connection) -> Result<String> {
        let existing: std::result::Result<String, rusqlite::Error> = conn.query_row(
            "SELECT value FROM _sync_meta WHERE key = 'client_id';",
            [],
            |row| row.get(0),
        );

        match existing {
            Ok(id) => Ok(id),
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                let new_id = uuid::Uuid::new_v4().to_string();
                conn.execute(
                    "INSERT INTO _sync_meta (key, value) VALUES ('client_id', ?1);",
                    params![new_id],
                )?;
                Ok(new_id)
            }
            Err(e) => Err(Error::Database(e)),
        }
    }

    /// Get last sync watermark timestamp
    pub fn get_watermark(conn: &Connection) -> Result<i64> {
        let watermark: std::result::Result<String, rusqlite::Error> = conn.query_row(
            "SELECT value FROM _sync_meta WHERE key = 'watermark';",
            [],
            |row| row.get(0),
        );

        match watermark {
            Ok(w) => Ok(w.parse::<i64>().unwrap_or(0)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(0),
            Err(e) => Err(Error::Database(e)),
        }
    }

    /// Set sync watermark timestamp
    pub fn set_watermark(conn: &Connection, watermark: i64) -> Result<()> {
        conn.execute(
            "INSERT OR REPLACE INTO _sync_meta (key, value) VALUES ('watermark', ?1);",
            params![watermark.to_string()],
        )?;
        Ok(())
    }
}
