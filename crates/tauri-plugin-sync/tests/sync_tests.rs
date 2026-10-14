#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use tempfile::NamedTempFile;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use tauri_plugin_sync::cdc::CdcManager;
    use tauri_plugin_sync::engine::SyncEngine;
    use tauri_plugin_sync::models::{ChangeRecord, FrontendSyncConfig, SyncConfig, SyncOperation};
    use tauri_plugin_sync::Builder;

    fn setup_test_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE todos (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                completed INTEGER NOT NULL DEFAULT 0
            );
            "#,
        )
        .unwrap();

        CdcManager::init_shadow_tables(&conn).unwrap();
        CdcManager::setup_triggers(&conn, "todos").unwrap();
        conn
    }

    #[test]
    fn test_cdc_triggers_record_insert_update_delete() {
        let conn = setup_test_db();

        // 1. Insert
        conn.execute(
            "INSERT INTO todos (id, title, completed) VALUES ('todo-1', 'Write tests', 0);",
            [],
        )
        .unwrap();

        let pending = CdcManager::get_pending_changes(&conn, 10).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].table, "todos");
        assert_eq!(pending[0].row_id, "todo-1");
        assert_eq!(pending[0].op, SyncOperation::Insert);
        assert_eq!(
            pending[0].data.as_ref().unwrap()["title"],
            json!("Write tests")
        );

        // 2. Update
        conn.execute(
            "UPDATE todos SET completed = 1 WHERE id = 'todo-1';",
            [],
        )
        .unwrap();

        let pending_after_update = CdcManager::get_pending_changes(&conn, 10).unwrap();
        assert_eq!(pending_after_update.len(), 2);
        assert_eq!(pending_after_update[1].op, SyncOperation::Update);
        assert_eq!(
            pending_after_update[1].data.as_ref().unwrap()["completed"],
            json!(1)
        );

        // 3. Delete
        conn.execute("DELETE FROM todos WHERE id = 'todo-1';", [])
            .unwrap();

        let pending_after_delete = CdcManager::get_pending_changes(&conn, 10).unwrap();
        assert_eq!(pending_after_delete.len(), 3);
        assert_eq!(pending_after_delete[2].op, SyncOperation::Delete);
        assert_eq!(pending_after_delete[2].row_id, "todo-1");
    }

    #[test]
    fn test_trigger_suppression_prevents_recursive_cdc() {
        let conn = setup_test_db();

        // Enable suppression
        CdcManager::set_suppression(&conn, true).unwrap();
        assert!(CdcManager::is_suppressed(&conn).unwrap());

        // Perform insert while suppressed
        conn.execute(
            "INSERT INTO todos (id, title, completed) VALUES ('todo-suppressed', 'Ghost task', 0);",
            [],
        )
        .unwrap();

        let pending = CdcManager::get_pending_changes(&conn, 10).unwrap();
        assert_eq!(pending.len(), 0, "Suppression failed: changes were logged");

        // Disable suppression
        CdcManager::set_suppression(&conn, false).unwrap();
        assert!(!CdcManager::is_suppressed(&conn).unwrap());

        // Perform insert when unsuppressed
        conn.execute(
            "INSERT INTO todos (id, title, completed) VALUES ('todo-normal', 'Normal task', 0);",
            [],
        )
        .unwrap();

        let pending_after = CdcManager::get_pending_changes(&conn, 10).unwrap();
        assert_eq!(pending_after.len(), 1);
        assert_eq!(pending_after[0].row_id, "todo-normal");
    }

    #[test]
    fn test_mark_and_prune_changes() {
        let conn = setup_test_db();

        conn.execute(
            "INSERT INTO todos (id, title, completed) VALUES ('t1', 'Task 1', 0);",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO todos (id, title, completed) VALUES ('t2', 'Task 2', 0);",
            [],
        )
        .unwrap();

        let pending = CdcManager::get_pending_changes(&conn, 10).unwrap();
        assert_eq!(pending.len(), 2);

        // Mark first synced
        CdcManager::mark_changes_synced(&conn, pending[0].id).unwrap();

        let remaining = CdcManager::get_pending_changes(&conn, 10).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].row_id, "t2");

        // Prune older than far future
        let pruned = CdcManager::prune_synced_changes(&conn, i64::MAX).unwrap();
        assert_eq!(pruned, 1);
    }

    #[test]
    fn test_apply_remote_record_lww() {
        let conn = setup_test_db();

        // 1. Remote insert record
        let remote_insert = ChangeRecord {
            id: 1,
            table: "todos".to_string(),
            row_id: "rem-1".to_string(),
            op: SyncOperation::Insert,
            data: Some(json!({
                "id": "rem-1",
                "title": "Remote task",
                "completed": 0
            })),
            timestamp: 1000,
        };

        SyncEngine::apply_remote_record(&conn, &remote_insert).unwrap();

        let title: String = conn
            .query_row("SELECT title FROM todos WHERE id = 'rem-1';", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(title, "Remote task");

        // 2. Local change exists with higher timestamp (LWW)
        conn.execute(
            "INSERT INTO _sync_changes (table_name, row_id, operation, data, timestamp, synced)
             VALUES ('todos', 'rem-1', 'UPDATE', '{\"title\":\"Local newer title\"}', 2000, 0);",
            [],
        )
        .unwrap();

        // Stale remote update arrives at t = 1500
        let stale_remote_update = ChangeRecord {
            id: 2,
            table: "todos".to_string(),
            row_id: "rem-1".to_string(),
            op: SyncOperation::Update,
            data: Some(json!({
                "id": "rem-1",
                "title": "Stale remote overwrite",
                "completed": 1
            })),
            timestamp: 1500,
        };

        SyncEngine::apply_remote_record(&conn, &stale_remote_update).unwrap();

        // Should not have overwritten since local change at 2000 > remote at 1500
        let title_after: String = conn
            .query_row("SELECT title FROM todos WHERE id = 'rem-1';", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(title_after, "Remote task");

        // 3. Newer remote update arrives at t = 3000 -> Wins!
        let winning_remote_update = ChangeRecord {
            id: 3,
            table: "todos".to_string(),
            row_id: "rem-1".to_string(),
            op: SyncOperation::Update,
            data: Some(json!({
                "id": "rem-1",
                "title": "Winning remote title",
                "completed": 1
            })),
            timestamp: 3000,
        };

        SyncEngine::apply_remote_record(&conn, &winning_remote_update).unwrap();

        let title_won: String = conn
            .query_row("SELECT title FROM todos WHERE id = 'rem-1';", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(title_won, "Winning remote title");
    }

    #[test]
    fn test_deletion_tombstone_wins_over_stale_update() {
        let conn = setup_test_db();

        conn.execute("INSERT INTO todos (id, title, completed) VALUES ('row-xyz', 'Original', 0);", []).unwrap();
        conn.execute("DELETE FROM todos WHERE id = 'row-xyz';", []).unwrap();

        let count: i64 = conn.query_row("SELECT COUNT(*) FROM todos WHERE id = 'row-xyz';", [], |r| r.get(0)).unwrap();
        assert_eq!(count, 0);

        let stale_update = ChangeRecord {
            id: 10,
            table: "todos".to_string(),
            row_id: "row-xyz".to_string(),
            op: SyncOperation::Update,
            data: Some(json!({
                "id": "row-xyz",
                "title": "Zombie Rebirth",
                "completed": 1
            })),
            timestamp: 1500,
        };

        SyncEngine::apply_remote_record(&conn, &stale_update).unwrap();

        let count_after: i64 = conn.query_row("SELECT COUNT(*) FROM todos WHERE id = 'row-xyz';", [], |r| r.get(0)).unwrap();
        assert_eq!(count_after, 0, "Zombie resurrected! Tombstone failed to protect deletion.");
    }

    #[test]
    fn test_sql_injection_and_reserved_keywords_sanitization() {
        let conn = Connection::open_in_memory().unwrap();

        conn.execute_batch(
            r#"
            CREATE TABLE "user-data order group" (
                "id" TEXT PRIMARY KEY,
                "order" INTEGER,
                "group by" TEXT,
                "user description" TEXT
            );
            "#,
        )
        .unwrap();

        CdcManager::init_shadow_tables(&conn).unwrap();
        CdcManager::setup_triggers(&conn, "user-data order group").unwrap();

        let tricky_title = "🔥 Special 'quotes' \"double\" and \n newline \t tab!";
        conn.execute(
            r#"INSERT INTO "user-data order group" ("id", "order", "group by", "user description")
               VALUES ('user-1', 42, 'admin team', ?1);"#,
            [tricky_title],
        )
        .unwrap();

        let pending = CdcManager::get_pending_changes(&conn, 10).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].table, "user-data order group");
        assert_eq!(pending[0].row_id, "user-1");

        let json_data = pending[0].data.as_ref().unwrap();
        assert_eq!(json_data["order"], json!(42));
        assert_eq!(json_data["group by"], json!("admin team"));
        assert_eq!(json_data["user description"], json!(tricky_title));
    }

    #[test]
    fn test_foreign_key_deferral_reverse_batch_order() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r#"
            PRAGMA foreign_keys = ON;

            CREATE TABLE categories (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL
            );

            CREATE TABLE tasks (
                id TEXT PRIMARY KEY,
                category_id TEXT NOT NULL REFERENCES categories(id),
                title TEXT NOT NULL
            );
            "#,
        )
        .unwrap();

        CdcManager::init_shadow_tables(&conn).unwrap();
        CdcManager::setup_triggers(&conn, "categories").unwrap();
        CdcManager::setup_triggers(&conn, "tasks").unwrap();

        // Create transaction with deferred foreign keys
        let tx = conn.transaction().unwrap();
        tx.execute_batch("PRAGMA defer_foreign_keys = ON;").unwrap();
        CdcManager::set_suppression(&tx, true).unwrap();

        // Intentionally insert CHILD record FIRST, PARENT record SECOND
        let child_task = ChangeRecord {
            id: 1,
            table: "tasks".to_string(),
            row_id: "task-100".to_string(),
            op: SyncOperation::Insert,
            data: Some(json!({
                "id": "task-100",
                "category_id": "cat-urgent",
                "title": "Child record arriving before parent"
            })),
            timestamp: 100,
        };

        let parent_category = ChangeRecord {
            id: 2,
            table: "categories".to_string(),
            row_id: "cat-urgent".to_string(),
            op: SyncOperation::Insert,
            data: Some(json!({
                "id": "cat-urgent",
                "name": "Urgent Work"
            })),
            timestamp: 101,
        };

        // Apply child first
        SyncEngine::apply_remote_record(&tx, &child_task).unwrap();
        // Apply parent second
        SyncEngine::apply_remote_record(&tx, &parent_category).unwrap();

        CdcManager::set_suppression(&tx, false).unwrap();
        // Commit should succeed without FOREIGN KEY constraint failed error
        tx.commit().unwrap();

        let count: i64 = conn.query_row("SELECT COUNT(*) FROM tasks;", [], |r| r.get(0)).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_schema_evolution_drops_unknown_server_columns() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE articles (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL
            );
            "#,
        )
        .unwrap();

        CdcManager::init_shadow_tables(&conn).unwrap();
        CdcManager::setup_triggers(&conn, "articles").unwrap();

        // Server sends payload from newer v2 schema with extra columns ('ai_summary', 'view_count')
        let v2_server_record = ChangeRecord {
            id: 1,
            table: "articles".to_string(),
            row_id: "art-1".to_string(),
            op: SyncOperation::Insert,
            data: Some(json!({
                "id": "art-1",
                "title": "Local First Architecture",
                "ai_summary": "Extra future column that does not exist in local v1 schema",
                "view_count": 99999
            })),
            timestamp: 500,
        };

        // Applying must succeed cleanly without 'table articles has no column named ai_summary' error
        SyncEngine::apply_remote_record(&conn, &v2_server_record).unwrap();

        let title: String = conn.query_row("SELECT title FROM articles WHERE id = 'art-1';", [], |r| r.get(0)).unwrap();
        assert_eq!(title, "Local First Architecture");
    }

    #[tokio::test]
    async fn test_unauthorized_401_triggers_error_and_event() {
        let mock_server = MockServer::start().await;
        let db_file = NamedTempFile::new().unwrap();
        let db_path = db_file.path().to_str().unwrap().to_string();

        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch("CREATE TABLE test_data (id TEXT PRIMARY KEY);").unwrap();
        }

        let config = SyncConfig {
            database: db_path.clone(),
            endpoint: mock_server.uri(),
            tables: vec!["test_data".to_string()],
            auth_header: Some("Bearer expired-token".to_string()),
            sync_interval_ms: 10000,
            batch_size: 100,
            prune_days: 30,
        };

        let conn = Connection::open(&db_path).unwrap();
        let (engine, (_status_rx, _prog_rx, mut unauthorized_rx)) = SyncEngine::new(conn, config).unwrap();

        // Insert pending item so push is attempted
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute("INSERT INTO test_data (id) VALUES ('x1');", []).unwrap();
        }

        Mock::given(method("POST"))
            .and(path("/push"))
            .respond_with(ResponseTemplate::new(401).set_body_string("JWT expired"))
            .mount(&mock_server)
            .await;

        let res = engine.sync_pass().await;
        assert!(res.is_err());

        // Verify unauthorized event receiver fired
        assert!(unauthorized_rx.try_recv().is_ok());

        // Verify status transitioned to Error
        let status = engine.get_status().await;
        assert_eq!(status.state, tauri_plugin_sync::models::SyncState::Error);
    }

    #[test]
    fn test_concurrent_writes_1000_operations_stress() {
        let db_file = NamedTempFile::new().unwrap();
        let db_path = db_file.path().to_str().unwrap().to_string();

        {
            let conn = Connection::open(&db_path).unwrap();
            CdcManager::configure_concurrency(&conn).unwrap();
            conn.execute_batch(
                r#"
                CREATE TABLE stress_items (
                    id TEXT PRIMARY KEY,
                    thread_id INTEGER NOT NULL,
                    seq INTEGER NOT NULL,
                    content TEXT NOT NULL
                );
                "#,
            )
            .unwrap();

            CdcManager::init_shadow_tables(&conn).unwrap();
            CdcManager::setup_triggers(&conn, "stress_items").unwrap();
        }

        let num_threads = 10;
        let ops_per_thread = 100;
        let mut handles = Vec::new();

        let stop_flag = Arc::new(AtomicBool::new(false));

        let bg_path = db_path.clone();
        let bg_stop = stop_flag.clone();
        let bg_handle = thread::spawn(move || {
            let conn = Connection::open(&bg_path).unwrap();
            CdcManager::configure_concurrency(&conn).unwrap();
            let mut reads = 0;
            while !bg_stop.load(Ordering::Relaxed) {
                let _pending = CdcManager::get_pending_changes(&conn, 50).unwrap();
                reads += 1;
                thread::sleep(Duration::from_millis(5));
            }
            reads
        });

        for thread_idx in 0..num_threads {
            let path = db_path.clone();
            let handle = thread::spawn(move || {
                let conn = Connection::open(&path).unwrap();
                CdcManager::configure_concurrency(&conn).unwrap();

                for seq in 0..ops_per_thread {
                    let id = format!("th-{}-seq-{}", thread_idx, seq);
                    let content = format!("Payload from thread {} at sequence {} 🚀", thread_idx, seq);

                    conn.execute(
                        "INSERT INTO stress_items (id, thread_id, seq, content) VALUES (?1, ?2, ?3, ?4);",
                        rusqlite::params![id, thread_idx, seq, content],
                    )
                    .unwrap();

                    let updated_content = format!("Updated payload {} 🚀", seq);
                    conn.execute(
                        "UPDATE stress_items SET content = ?1 WHERE id = ?2;",
                        rusqlite::params![updated_content, id],
                    )
                    .unwrap();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        stop_flag.store(true, Ordering::Relaxed);
        let total_reads = bg_handle.join().unwrap();
        assert!(total_reads > 0);

        let conn = Connection::open(&db_path).unwrap();
        let total_items: i64 = conn.query_row("SELECT COUNT(*) FROM stress_items;", [], |r| r.get(0)).unwrap();
        assert_eq!(total_items, (num_threads * ops_per_thread) as i64);

        let total_changes: i64 = conn.query_row("SELECT COUNT(*) FROM _sync_changes;", [], |r| r.get(0)).unwrap();
        assert_eq!(total_changes, (num_threads * ops_per_thread * 2) as i64);
    }

    #[tokio::test]
    async fn test_full_push_pull_with_mock_server() {
        let mock_server = MockServer::start().await;

        let db_file = NamedTempFile::new().unwrap();
        let db_path = db_file.path().to_str().unwrap().to_string();

        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(
                r#"
                CREATE TABLE items (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL
                );
                "#,
            )
            .unwrap();
        }

        let config = SyncConfig {
            database: db_path.clone(),
            endpoint: mock_server.uri(),
            tables: vec!["items".to_string()],
            auth_header: Some("Bearer test-token".to_string()),
            sync_interval_ms: 10000,
            batch_size: 100,
            prune_days: 30,
        };

        let conn = Connection::open(&db_path).unwrap();
        let (engine, (_status_rx, _prog_rx, _unauth_rx)) = SyncEngine::new(conn, config).unwrap();

        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute(
                "INSERT INTO items (id, name) VALUES ('item-1', 'Local item');",
                [],
            )
            .unwrap();
        }

        Mock::given(method("POST"))
            .and(path("/push"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "status": "ok" })))
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path("/pull"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "watermark": 500,
                "changes": [
                    {
                        "id": 99,
                        "table": "items",
                        "row_id": "item-2",
                        "op": "INSERT",
                        "data": {
                            "id": "item-2",
                            "name": "Pulled remote item"
                        },
                        "timestamp": 450
                    }
                ]
            })))
            .mount(&mock_server)
            .await;

        let progress = engine.sync_pass().await.unwrap();
        assert_eq!(progress.pushed_count, 1);
        assert_eq!(progress.pulled_count, 1);
        assert_eq!(progress.watermark, 500);

        let conn = Connection::open(&db_path).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM items;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 2);

        let pending = CdcManager::count_pending_changes(&conn).unwrap();
        assert_eq!(pending, 0);

        let watermark = CdcManager::get_watermark(&conn).unwrap();
        assert_eq!(watermark, 500);
    }

    #[tokio::test]
    async fn test_pause_resume_and_frontend_config() {
        let temp_db = NamedTempFile::new().unwrap();
        let db_path = temp_db.path().to_str().unwrap().to_string();

        let config = SyncConfig {
            database: db_path.clone(),
            endpoint: "https://example.com/sync".to_string(),
            tables: vec![],
            auth_header: None,
            sync_interval_ms: 5000,
            batch_size: 500,
            prune_days: 30,
        };

        let conn = Connection::open(&db_path).unwrap();
        let (engine, (_status_rx, _prog_rx, _unauth_rx)) = SyncEngine::new(conn, config).unwrap();

        assert!(!engine.is_paused());
        engine.pause();
        assert!(engine.is_paused());
        engine.resume();
        assert!(!engine.is_paused());

        // Update safe frontend configs
        let frontend_cfg = FrontendSyncConfig {
            sync_interval_ms: Some(2500),
            batch_size: Some(100),
            prune_days: Some(14),
        };
        engine.update_frontend_config(frontend_cfg).await.unwrap();

        // Update auth header securely from Rust
        engine.set_auth_header(Some("Bearer secret-rust-token".to_string())).await;
    }

    #[test]
    fn test_builder_pattern_construction() {
        let builder = Builder::new()
            .database("test.db")
            .endpoint("https://api.example.com/sync")
            .bearer_token("my-secret-jwt")
            .tables(vec!["todos".to_string(), "notes".to_string()])
            .sync_interval(Duration::from_secs(15))
            .batch_size(250)
            .prune_days(60);

        // Builder can be cloned and configured fluently
        let cloned = builder.clone();
        let _ = cloned;
    }
}
