use tauri::{command, AppHandle, Runtime, State};

use crate::error::Result;
use crate::models::{SyncConfig, SyncProgressEvent, SyncStatus};
use crate::SyncPluginState;

#[command]
pub async fn init<R: Runtime>(
    app: AppHandle<R>,
    state: State<'_, SyncPluginState>,
    config: SyncConfig,
) -> Result<SyncStatus> {
    state.init(app, config).await
}

#[command]
pub async fn sync_now(
    state: State<'_, SyncPluginState>,
) -> Result<SyncProgressEvent> {
    state.sync_now().await
}

#[command]
pub async fn get_status(
    state: State<'_, SyncPluginState>,
) -> Result<SyncStatus> {
    state.get_status().await
}

#[command]
pub async fn configure(
    state: State<'_, SyncPluginState>,
    config: SyncConfig,
) -> Result<()> {
    state.configure(config).await
}
