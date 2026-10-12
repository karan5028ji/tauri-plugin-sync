// Prevents additional console window on Windows in release
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use tauri::State;

#[derive(Debug, Serialize, Deserialize)]
pub struct TodoItem {
    pub id: String,
    pub title: String,
    pub completed: bool,
}

pub struct AppState {
    pub db_path: String,
    pub conn: Mutex<Connection>,
}

#[tauri::command]
fn get_todos(state: State<'_, AppState>) -> Result<Vec<TodoItem>, String> {
    let conn = state.conn.lock().map_err(|e| e.to_string())?;
    let mut stmt = conn
        .prepare("SELECT id, title, completed FROM todos ORDER BY rowid ASC;")
        .map_err(|e| e.to_string())?;

    let rows = stmt
        .query_map([], |row| {
            let id: String = row.get(0)?;
            let title: String = row.get(1)?;
            let completed_int: i32 = row.get(2)?;
            Ok(TodoItem {
                id,
                title,
                completed: completed_int == 1,
            })
        })
        .map_err(|e| e.to_string())?;

    let mut todos = Vec::new();
    for r in rows {
        todos.push(r.map_err(|e| e.to_string())?);
    }
    Ok(todos)
}

#[tauri::command]
fn add_todo(state: State<'_, AppState>, id: String, title: String) -> Result<(), String> {
    let conn = state.conn.lock().map_err(|e| e.to_string())?;
    conn.execute(
        "INSERT INTO todos (id, title, completed) VALUES (?1, ?2, 0);",
        rusqlite::params![id, title],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

#[tauri::command]
fn toggle_todo(state: State<'_, AppState>, id: String, completed: bool) -> Result<(), String> {
    let conn = state.conn.lock().map_err(|e| e.to_string())?;
    let val = if completed { 1 } else { 0 };
    conn.execute(
        "UPDATE todos SET completed = ?1 WHERE id = ?2;",
        rusqlite::params![val, id],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

#[tauri::command]
fn delete_todo(state: State<'_, AppState>, id: String) -> Result<(), String> {
    let conn = state.conn.lock().map_err(|e| e.to_string())?;
    conn.execute("DELETE FROM todos WHERE id = ?1;", rusqlite::params![id])
        .map_err(|e| e.to_string())?;
    Ok(())
}

#[tauri::command]
fn get_db_path(state: State<'_, AppState>) -> String {
    state.db_path.clone()
}

fn main() {
    let db_path = "todo_demo.db".to_string();
    let conn = Connection::open(&db_path).expect("Failed to open SQLite database");

    // Enable WAL and concurrency pragmas
    conn.execute_batch(
        r#"
        PRAGMA journal_mode = WAL;
        PRAGMA busy_timeout = 5000;
        CREATE TABLE IF NOT EXISTS todos (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            completed INTEGER NOT NULL DEFAULT 0
        );
        "#,
    )
    .expect("Failed to initialize database tables");

    tauri::Builder::default()
        .plugin(tauri_plugin_sync::init())
        .manage(AppState {
            db_path,
            conn: Mutex::new(conn),
        })
        .invoke_handler(tauri::generate_handler![
            get_todos,
            add_todo,
            toggle_todo,
            delete_todo,
            get_db_path,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
