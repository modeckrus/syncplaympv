use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::process::Command;
use tokio::sync::mpsc;

/// Events from MPV that we care about
#[derive(Debug, Clone)]
pub enum MpvEvent {
    Pause,
    Unpause,
    Disconnected,
}

/// Commands to send to MPV
#[derive(Debug, Clone)]
pub enum MpvCommand {
    Play,
    Pause,
}

/// MPV IPC event (raw JSON)
#[derive(Debug, Deserialize)]
struct MpvEventRaw {
    #[serde(default)]
    event: Option<String>,
}

/// Cross-platform MPV socket path
pub fn default_socket_path() -> PathBuf {
    if cfg!(windows) {
        // Windows: use named pipe prefix or temp dir
        let temp = std::env::temp_dir();
        temp.join("mpv_sync.sock")
    } else {
        // Linux/macOS: Unix socket in /tmp
        PathBuf::from("/tmp/mpv_sync.sock")
    }
}

/// Launch MPV with IPC socket configured
pub async fn launch_mpv(socket_path: &Path) -> Result<tokio::process::Child> {
    let socket_arg = if cfg!(windows) {
        // Windows: MPV supports --input-ipc-server with named pipes or paths
        format!(
            r#"\\.\pipe\{}"#,
            socket_path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
        )
    } else {
        socket_path.to_string_lossy().to_string()
    };

    println!("Launching MPV with IPC socket: {}", socket_arg);

    let mut child = Command::new("mpv")
        .arg(format!("--input-ipc-server={}", socket_arg))
        .arg("--no-terminal") // Don't take over the terminal
        .arg("--keep-open=yes") // Keep window open when file ends
        .arg("--idle=yes") // Stay running even without a file
        .arg("--force-window=yes") // Always show window, even without a file
        .spawn()
        .context("Failed to launch MPV. Make sure 'mpv' is in your PATH")?;

    // Give MPV a moment to create the socket
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Check if MPV is still running
    if let Some(status) = child.try_wait().context("Failed to check MPV status")? {
        anyhow::bail!("MPV exited unexpectedly with status: {}", status);
    }

    println!("MPV launched successfully (PID: {:?})", child.id());
    Ok(child)
}

/// MPV IPC client — connects to MPV socket and exchanges events/commands
pub struct MpvClient {
    socket_path: PathBuf,
}

impl MpvClient {
    pub fn new(socket_path: PathBuf) -> Self {
        Self { socket_path }
    }

    /// Connect to MPV IPC socket
    /// Returns sender for commands and receiver for events
    pub async fn connect(&self) -> Result<(mpsc::Sender<MpvCommand>, mpsc::Receiver<MpvEvent>)> {
        let stream = UnixStream::connect(&self.socket_path)
            .await
            .with_context(|| {
                format!("Failed to connect to MPV socket at {:?}", self.socket_path)
            })?;

        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);

        let (cmd_tx, mut cmd_rx) = mpsc::channel::<MpvCommand>(32);
        let (event_tx, event_rx) = mpsc::channel::<MpvEvent>(32);

        // Read events from MPV
        tokio::spawn(async move {
            let mut line = String::new();
            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) => {
                        let _ = event_tx.send(MpvEvent::Disconnected).await;
                        break;
                    }
                    Ok(_) => {
                        let line = line.trim();
                        if line.is_empty() {
                            continue;
                        }

                        if let Ok(event) = serde_json::from_str::<MpvEventRaw>(line) {
                            if let Some(event_name) = event.event {
                                match event_name.as_str() {
                                    "pause" => {
                                        let _ = event_tx.send(MpvEvent::Pause).await;
                                    }
                                    "unpause" => {
                                        let _ = event_tx.send(MpvEvent::Unpause).await;
                                    }
                                    _ => {} // Ignore other events
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("MPV read error: {}", e);
                        let _ = event_tx.send(MpvEvent::Disconnected).await;
                        break;
                    }
                }
            }
        });

        // Send commands to MPV
        tokio::spawn(async move {
            while let Some(cmd) = cmd_rx.recv().await {
                let json = match cmd {
                    MpvCommand::Play => {
                        serde_json::json!({
                            "command": ["set_property", "pause", false]
                        })
                    }
                    MpvCommand::Pause => {
                        serde_json::json!({
                            "command": ["set_property", "pause", true]
                        })
                    }
                };

                if let Err(e) = writer.write_all(json.to_string().as_bytes()).await {
                    eprintln!("Failed to send command to MPV: {}", e);
                    break;
                }
                if let Err(e) = writer.write_all(b"\n").await {
                    eprintln!("Failed to send command to MPV: {}", e);
                    break;
                }
            }
        });

        Ok((cmd_tx, event_rx))
    }
}
