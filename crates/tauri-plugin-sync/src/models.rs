use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum SyncOperation {
    Insert,
    Update,
    Delete,
}

impl std::fmt::Display for SyncOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncOperation::Insert => write!(f, "INSERT"),
            SyncOperation::Update => write!(f, "UPDATE"),
            SyncOperation::Delete => write!(f, "DELETE"),
        }
    }
}

impl std::str::FromStr for SyncOperation {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "INSERT" => Ok(SyncOperation::Insert),
            "UPDATE" => Ok(SyncOperation::Update),
            "DELETE" => Ok(SyncOperation::Delete),
            _ => Err(format!("Unknown sync operation: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SyncState {
    Idle,
    Syncing,
    Offline,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncStatus {
    pub state: SyncState,
    pub last_sync_at: Option<i64>,
    pub pending_changes: usize,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeRecord {
    pub id: i64,
    pub table: String,
    pub row_id: String,
    pub op: SyncOperation,
    pub data: Option<serde_json::Value>,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientChangeset {
    pub client_id: String,
    pub watermark: i64,
    pub changes: Vec<ChangeRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerChangeset {
    pub watermark: i64,
    pub changes: Vec<ChangeRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConfig {
    pub database: String,
    pub endpoint: String,
    pub tables: Vec<String>,
    pub auth_header: Option<String>,
    #[serde(default = "default_sync_interval_ms")]
    pub sync_interval_ms: u64,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Maximum age in days for synced journal entries before automatic pruning (default 30 days)
    #[serde(default = "default_prune_days")]
    pub prune_days: u32,
}

/// Safe configuration parameters that can be updated from frontend JavaScript.
/// Sensitive parameters (database path, auth header, remote endpoint) remain
/// locked in the Rust backend to prevent XSS credential leaks or path traversal.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct FrontendSyncConfig {
    pub sync_interval_ms: Option<u64>,
    pub batch_size: Option<usize>,
    pub prune_days: Option<u32>,
}

/// Static plugin configuration loaded from tauri.conf.json under `plugins.sync`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct PluginConfig {
    pub database: Option<String>,
    pub endpoint: Option<String>,
    pub tables: Option<Vec<String>>,
    pub auth_header: Option<String>,
    pub sync_interval_ms: Option<u64>,
    pub batch_size: Option<usize>,
    pub prune_days: Option<u32>,
}

fn default_sync_interval_ms() -> u64 {
    5000
}

fn default_batch_size() -> usize {
    500
}

fn default_prune_days() -> u32 {
    30
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncProgressEvent {
    pub pushed_count: usize,
    pub pulled_count: usize,
    pub watermark: i64,
}
