const COMMANDS: &[&str] = &["init", "sync_now", "get_status", "configure"];

fn main() {
    tauri_plugin::Builder::new(COMMANDS)
        .build();
}
