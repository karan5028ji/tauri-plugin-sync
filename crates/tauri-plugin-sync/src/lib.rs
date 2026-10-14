pub mod cdc;
pub mod commands;
pub mod engine;
pub mod error;
pub mod models;

use std::sync::Arc;
use log::info;
use rusqlite::Connection;
use tauri::{
    plugin::TauriPlugin,
    AppHandle, Emitter, Manager, Runtime,
};
use tokio::sync::{oneshot, Mutex, RwLock};

pub use error::{Error, Result};
pub use models::*;

use crate::cdc::CdcManager;
use crate::engine::SyncEngine;

/// Managed state for the Tauri plugin
#[derive(Clone)]
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
                Error::NotInitialized("Sync plugin has not been initialized".into())
            })?
        };

        engine.sync_pass().await
    }

    pub async fn get_status(&self) -> Result<SyncStatus> {
        let engine = {
            let guard = self.engine.read().await;
            guard.as_ref().cloned().ok_or_else(|| {
                Error::NotInitialized("Sync plugin has not been initialized".into())
            })?
        };

        Ok(engine.get_status().await)
    }

    pub async fn pause(&self) -> Result<()> {
        let engine = {
            let guard = self.engine.read().await;
            guard.as_ref().cloned().ok_or_else(|| {
                Error::NotInitialized("Sync plugin has not been initialized".into())
            })?
        };

        engine.pause();
        Ok(())
    }

    pub async fn resume(&self) -> Result<()> {
        let engine = {
            let guard = self.engine.read().await;
            guard.as_ref().cloned().ok_or_else(|| {
                Error::NotInitialized("Sync plugin has not been initialized".into())
            })?
        };

        engine.resume();
        Ok(())
    }

    pub async fn configure(&self, config: SyncConfig) -> Result<()> {
        let engine = {
            let guard = self.engine.read().await;
            guard.as_ref().cloned().ok_or_else(|| {
                Error::NotInitialized("Sync plugin has not been initialized".into())
            })?
        };

        engine.update_config(config).await
    }

    pub async fn configure_frontend(&self, config: FrontendSyncConfig) -> Result<()> {
        let engine = {
            let guard = self.engine.read().await;
            guard.as_ref().cloned().ok_or_else(|| {
                Error::NotInitialized("Sync plugin has not been initialized".into())
            })?
        };

        engine.update_frontend_config(config).await
    }

    pub async fn set_auth_header(&self, header: Option<String>) -> Result<()> {
        let engine = {
            let guard = self.engine.read().await;
            guard.as_ref().cloned().ok_or_else(|| {
                Error::NotInitialized("Sync plugin has not been initialized".into())
            })?
        };

        engine.set_auth_header(header).await;
        Ok(())
    }

    pub async fn set_bearer_token(&self, token: impl Into<String>) -> Result<()> {
        self.set_auth_header(Some(format!("Bearer {}", token.into()))).await
    }

    async fn stop_engine(&self) {
        let mut stop_guard = self.stop_tx.lock().await;
        if let Some(tx) = stop_guard.take() {
            let _ = tx.send(());
        }
    }
}

/// Extension trait providing access to `SyncPluginState` on any Tauri `Manager` (e.g. `AppHandle`, `App`).
pub trait SyncExt<R: Runtime> {
    fn sync(&self) -> &SyncPluginState;
}

impl<R: Runtime, T: Manager<R>> SyncExt<R> for T {
    fn sync(&self) -> &SyncPluginState {
        self.state::<SyncPluginState>().inner()
    }
}

/// Rust Builder for `tauri-plugin-sync` adhering to official Tauri v2 plugin standards.
/// Allows secure backend configuration of SQLite database path, remote endpoints, credentials,
/// and replication intervals without exposing secrets to untrusted WebView JavaScript.
#[derive(Debug, Default, Clone)]
pub struct Builder {
    database: Option<String>,
    endpoint: Option<String>,
    auth_header: Option<String>,
    tables: Vec<String>,
    sync_interval: Option<std::time::Duration>,
    batch_size: Option<usize>,
    prune_days: Option<u32>,
}

pub use self::Builder as SyncBuilder;

impl Builder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn database(mut self, path: impl Into<String>) -> Self {
        self.database = Some(path.into());
        self
    }

    pub fn endpoint(mut self, url: impl Into<String>) -> Self {
        self.endpoint = Some(url.into());
        self
    }

    pub fn bearer_token(mut self, token: impl Into<String>) -> Self {
        self.auth_header = Some(format!("Bearer {}", token.into()));
        self
    }

    pub fn auth_header(mut self, header: impl Into<String>) -> Self {
        self.auth_header = Some(header.into());
        self
    }

    pub fn tables(mut self, tables: Vec<String>) -> Self {
        self.tables = tables;
        self
    }

    pub fn sync_interval(mut self, interval: std::time::Duration) -> Self {
        self.sync_interval = Some(interval);
        self
    }

    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = Some(size);
        self
    }

    pub fn prune_days(mut self, days: u32) -> Self {
        self.prune_days = Some(days);
        self
    }

    pub fn build<R: Runtime>(self) -> TauriPlugin<R, PluginConfig> {
        let builder_db = self.database;
        let builder_endpoint = self.endpoint;
        let builder_auth = self.auth_header;
        let builder_tables = self.tables;
        let builder_interval = self.sync_interval;
        let builder_batch = self.batch_size;
        let builder_prune = self.prune_days;

        tauri::plugin::Builder::<R, PluginConfig>::new("sync")
            .invoke_handler(tauri::generate_handler![
                commands::sync_now,
                commands::get_status,
                commands::pause,
                commands::resume,
                commands::configure,
            ])
            .setup(move |app, api| {
                let conf = api.config();

                let db = builder_db.or_else(|| conf.database.clone());
                let endpoint = builder_endpoint.or_else(|| conf.endpoint.clone());
                let auth_header = builder_auth.or_else(|| conf.auth_header.clone());
                let tables = if !builder_tables.is_empty() {
                    builder_tables
                } else {
                    conf.tables.clone().unwrap_or_default()
                };
                let sync_interval_ms = builder_interval
                    .map(|d| d.as_millis() as u64)
                    .or(conf.sync_interval_ms)
                    .unwrap_or(5000);
                let batch_size = builder_batch
                    .or(conf.batch_size)
                    .unwrap_or(500);
                let prune_days = builder_prune
                    .or(conf.prune_days)
                    .unwrap_or(30);

                let plugin_state = SyncPluginState::default();

                // If database and endpoint are configured (via Builder or tauri.conf.json),
                // automatically initialize and launch the background replication actor.
                if let (Some(db_path), Some(ep)) = (db, endpoint) {
                    let sync_config = SyncConfig {
                        database: db_path,
                        endpoint: ep,
                        tables,
                        auth_header,
                        sync_interval_ms,
                        batch_size,
                        prune_days,
                    };

                    let app_handle = app.clone();
                    let state_clone = plugin_state.clone();
                    tauri::async_runtime::spawn(async move {
                        if let Err(e) = state_clone.init(app_handle, sync_config).await {
                            log::error!("Failed to auto-initialize tauri-plugin-sync: {}", e);
                        }
                    });
                }

                app.manage(plugin_state);
                Ok(())
            })
            .build()
    }
}

/// Initializes the plugin with default configurations from `tauri.conf.json`.
pub fn init<R: Runtime>() -> TauriPlugin<R, PluginConfig> {
    Builder::new().build()
}
