const COMMANDS: &[&str] = &["sync_now", "get_status", "pause", "resume", "configure"];

fn main() {
    tauri_plugin::Builder::new(COMMANDS)
        .build();
}
