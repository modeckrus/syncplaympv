use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::process::Command;
use tokio::sync::mpsc;
use std::sync::atomic::{AtomicU32, Ordering};

/// Events from MPV that we care about
#[derive(Debug, Clone)]
pub enum MpvEvent {
    Pause,
    Unpause,
    /// Playback time changed (seconds)
    TimePos(f64),
    Disconnected,
}

/// Commands to send to MPV
#[derive(Debug, Clone)]
pub enum MpvCommand {
    Play,
    Pause,
    /// Seek to a specific time (seconds)
    Seek(f64),
}

/// MPV IPC event (raw JSON) — all fields optional, we pattern-match after parsing
#[derive(Debug, Deserialize)]
struct MpvEventRaw {
    /// Present in all events: "property-change", "shutdown", "file-loaded", "end-file", etc.
    #[serde(default)]
    event: Option<String>,
    /// Present in replies to commands
    #[serde(default)]
    error: Option<String>,
    /// Present in replies and property-change events
    #[serde(default)]
    data: Option<serde_json::Value>,
    /// Present in command replies
    #[serde(default)]
    request_id: Option<u64>,
    /// Present in property-change events: the numeric ID from observe_property
    #[serde(default)]
    id: Option<u64>,
    /// Present in property-change events: the property name
    #[serde(default)]
    name: Option<String>,
}

/// Cross-platform MPV socket path — unique per client instance
pub fn default_socket_path() -> PathBuf {
    // PID + atomic counter to avoid collisions when multiple clients run on the same machine
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let pid = std::process::id();
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let suffix = format!("{:x}_{:x}", pid, seq);

    if cfg!(windows) {
        let temp = std::env::temp_dir();
        temp.join(format!("mpv_sync_{}.sock", suffix))
    } else {
        PathBuf::from(format!("/tmp/mpv_sync_{}.sock", suffix))
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

        // Observe properties we care about: pause state and playback time
        // ID 1 = pause property, ID 2 = playback-time property
        writer
            .write_all(
                serde_json::json!({
                    "command": ["observe_property", 1, "pause"]
                })
                .to_string()
                .as_bytes(),
            )
            .await
            .context("Failed to observe pause property")?;
        writer.write_all(b"\n").await?;

        writer
            .write_all(
                serde_json::json!({
                    "command": ["observe_property", 2, "playback-time"]
                })
                .to_string()
                .as_bytes(),
            )
            .await
            .context("Failed to observe playback-time property")?;
        writer.write_all(b"\n").await?;

        println!("[MPV] Observing pause and playback-time properties");

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

                        match serde_json::from_str::<MpvEventRaw>(line) {
                            Ok(raw) => {
                                // Check if this is a property-change event
                                if raw.event.as_deref() == Some("property-change") {
                                    match (raw.id, raw.name.as_deref()) {
                                        (Some(1), Some("pause")) => {
                                            // pause property: true = paused, false = playing
                                            if let Some(serde_json::Value::Bool(paused)) = raw.data
                                            {
                                                if paused {
                                                    println!("[MPV] Property change: pause=true");
                                                    let _ = event_tx.send(MpvEvent::Pause).await;
                                                } else {
                                                    println!("[MPV] Property change: pause=false");
                                                    let _ = event_tx.send(MpvEvent::Unpause).await;
                                                }
                                            }
                                        }
                                        (Some(2), Some("playback-time")) => {
                                            // playback-time property: float seconds, or null
                                            if let Some(val) = raw.data {
                                                if let Some(time) = val.as_f64() {
                                                    let _ = event_tx
                                                        .send(MpvEvent::TimePos(time))
                                                        .await;
                                                }
                                            }
                                        }
                                        _ => {
                                            println!(
                                                "[MPV] Property change: id={:?} name={:?} (ignored)",
                                                raw.id, raw.name
                                            );
                                        }
                                    }
                                } else if let Some(event_name) = raw.event {
                                    match event_name.as_str() {
                                        "shutdown" => {
                                            println!("[MPV] Raw event: shutdown");
                                            let _ = event_tx.send(MpvEvent::Disconnected).await;
                                            break;
                                        }
                                        "file-loaded" => {
                                            println!("[MPV] Raw event: file-loaded");
                                        }
                                        "end-file" => {
                                            println!("[MPV] Raw event: end-file");
                                        }
                                        _ => {
                                            if let Some(err) = raw.error {
                                                println!(
                                                    "[MPV] Reply error: {} request_id={:?}",
                                                    err, raw.request_id
                                                );
                                            } else {
                                                println!(
                                                    "[MPV] Raw event: {} (ignored)",
                                                    event_name
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                println!("[MPV] Failed to parse event JSON: {} | raw: {}", e, line);
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
                println!("[MPV] Sending command: {:?}", cmd);
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
                    MpvCommand::Seek(time) => {
                        serde_json::json!({
                            "command": ["seek", time, "absolute"]
                        })
                    }
                };

                if let Err(e) = writer.write_all(json.to_string().as_bytes()).await {
                    eprintln!("[MPV] Failed to send command to MPV: {}", e);
                    break;
                }
                if let Err(e) = writer.write_all(b"\n").await {
                    eprintln!("[MPV] Failed to send command to MPV: {}", e);
                    break;
                }
                println!("[MPV] Command sent OK");
            }
        });

        Ok((cmd_tx, event_rx))
    }
}
