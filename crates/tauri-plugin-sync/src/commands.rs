use tauri::{command, State};

use crate::error::Result;
use crate::models::{FrontendSyncConfig, SyncProgressEvent, SyncStatus};
use crate::SyncPluginState;

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
pub async fn pause(
    state: State<'_, SyncPluginState>,
) -> Result<()> {
    state.pause().await
}

#[command]
pub async fn resume(
    state: State<'_, SyncPluginState>,
) -> Result<()> {
    state.resume().await
}

#[command]
pub async fn configure(
    state: State<'_, SyncPluginState>,
    config: FrontendSyncConfig,
) -> Result<()> {
    state.configure_frontend(config).await
}
