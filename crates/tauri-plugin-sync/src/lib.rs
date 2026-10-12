pub mod cdc;
pub mod commands;
pub mod engine;
pub mod error;
pub mod models;

use std::sync::Arc;
use log::info;
use rusqlite::Connection;
use tauri::{
    plugin::{Builder, TauriPlugin},
    AppHandle, Emitter, Manager, Runtime,
};
use tokio::sync::{oneshot, Mutex, RwLock};

pub use error::{Error, Result};
pub use models::*;

use crate::cdc::CdcManager;
use crate::engine::SyncEngine;

/// Managed state for the Tauri plugin
pub struct SyncPluginState {
    engine: Arc<RwLock<Option<Arc<SyncEngine>>>>,
    stop_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

impl Default for SyncPluginState {
    fn default() -> Self {
        Self {
            engine: Arc::new(RwLock::new(None)),
            stop_tx: Arc::new(Mutex::new(None)),
        }
    }
}

impl SyncPluginState {
    pub async fn init<R: Runtime>(&self, app: AppHandle<R>, config: SyncConfig) -> Result<SyncStatus> {
        // Stop existing engine if running
        self.stop_engine().await;

        let db_path = &config.database;
        let conn = Connection::open(db_path).map_err(|e| {
            Error::NotInitialized(format!("Could not open database at '{}': {}", db_path, e))
        })?;

        // Enforce WAL mode and 5000ms busy timeout on opened connection
        CdcManager::configure_concurrency(&conn)?;

        let (engine, (mut status_rx, mut progress_rx, mut unauthorized_rx)) = SyncEngine::new(conn, config)?;
        let engine_arc = Arc::new(engine);

        // Spawn status event emitter task
        let app_status = app.clone();
        tauri::async_runtime::spawn(async move {
            while let Ok(status) = status_rx.recv().await {
                let _ = app_status.emit("sync://status", &status);
            }
        });

        // Spawn progress event emitter task
        let app_progress = app.clone();
        tauri::async_runtime::spawn(async move {
            while let Ok(prog) = progress_rx.recv().await {
                let _ = app_progress.emit("sync://progress", &prog);
            }
        });

        // Spawn unauthorized event emitter task
        let app_unauthorized = app.clone();
        tauri::async_runtime::spawn(async move {
            while let Ok(()) = unauthorized_rx.recv().await {
                let _ = app_unauthorized.emit("sync://unauthorized", ());
            }
        });

        // Spawn background polling loop
        let (stop_sender, stop_receiver) = oneshot::channel();
        let loop_engine = engine_arc.clone();
        tauri::async_runtime::spawn(async move {
            loop_engine.run_loop(stop_receiver).await;
        });

        *self.stop_tx.lock().await = Some(stop_sender);
        let status = engine_arc.get_status().await;
        *self.engine.write().await = Some(engine_arc);

        info!("tauri-plugin-sync initialized successfully with WAL mode and concurrency hardening");
        Ok(status)
    }

    pub async fn sync_now(&self) -> Result<SyncProgressEvent> {
        let engine = {
            let guard = self.engine.read().await;
            guard.as_ref().cloned().ok_or_else(|| {
                Error::NotInitialized("Sync plugin has not been initialized with init()".into())
            })?
        };

        engine.sync_pass().await
    }

    pub async fn get_status(&self) -> Result<SyncStatus> {
        let engine = {
            let guard = self.engine.read().await;
            guard.as_ref().cloned().ok_or_else(|| {
                Error::NotInitialized("Sync plugin has not been initialized with init()".into())
            })?
        };

        Ok(engine.get_status().await)
    }

    pub async fn configure(&self, config: SyncConfig) -> Result<()> {
        let engine = {
            let guard = self.engine.read().await;
            guard.as_ref().cloned().ok_or_else(|| {
                Error::NotInitialized("Sync plugin has not been initialized with init()".into())
            })?
        };

        engine.update_config(config).await
    }

    async fn stop_engine(&self) {
        let mut stop_guard = self.stop_tx.lock().await;
        if let Some(tx) = stop_guard.take() {
            let _ = tx.send(());
        }
    }
}

/// Initializes the plugin.
pub fn init<R: Runtime>() -> TauriPlugin<R> {
    Builder::new("sync")
        .invoke_handler(tauri::generate_handler![
            commands::init,
            commands::sync_now,
            commands::get_status,
            commands::configure,
        ])
        .setup(|app, _api| {
            app.manage(SyncPluginState::default());
            Ok(())
        })
        .build()
}
