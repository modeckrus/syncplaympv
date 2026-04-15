use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

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

/// MPV IPC event (raw JSON)
#[derive(Debug, Deserialize)]
struct MpvEventRaw {
    #[serde(default)]
    event: Option<String>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    data: Option<serde_json::Value>,
    #[serde(default)]
    request_id: Option<u64>,
    #[serde(default)]
    id: Option<u64>,
    #[serde(default)]
    name: Option<String>,
}

// ============================================================================
// Socket path — cross-platform
// ============================================================================

pub fn default_socket_path() -> PathBuf {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let pid = std::process::id();
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let suffix = format!("{:x}_{:x}", pid, seq);

    if cfg!(windows) {
        let temp = std::env::temp_dir();
        temp.join(format!("mpv_sync_{}", suffix))
    } else {
        PathBuf::from(format!("/tmp/mpv_sync_{}.sock", suffix))
    }
}

pub fn mpv_socket_arg(path: &Path) -> String {
    if cfg!(windows) {
        format!(
            r#"\\.\pipe\{}"#,
            path.file_name().unwrap_or_default().to_string_lossy()
        )
    } else {
        path.to_string_lossy().to_string()
    }
}

pub async fn launch_mpv(socket_path: &Path) -> Result<tokio::process::Child> {
    let socket_arg = mpv_socket_arg(socket_path);
    println!("Launching MPV with IPC: {}", socket_arg);

    let mut child = tokio::process::Command::new("mpv")
        .arg(format!("--input-ipc-server={}", socket_arg))
        .arg("--no-terminal")
        .arg("--keep-open=yes")
        .arg("--idle=yes")
        .arg("--force-window=yes")
        .spawn()
        .context("Failed to launch MPV. Make sure 'mpv' is in your PATH")?;

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    if let Some(status) = child.try_wait().context("Failed to check MPV status")? {
        anyhow::bail!("MPV exited unexpectedly with status: {}", status);
    }

    println!("MPV launched successfully (PID: {:?})", child.id());
    Ok(child)
}

// ============================================================================
// Shared helpers
// ============================================================================

async fn send_observe_commands<W: AsyncWriteExt + Unpin>(writer: &mut W) -> Result<()> {
    let cmd1 = serde_json::json!({
        "command": ["observe_property", 1, "pause"]
    })
    .to_string();
    writer
        .write_all(cmd1.as_bytes())
        .await
        .context("Failed to write observe_property pause")?;
    writer.write_all(b"\n").await.context("Failed to write newline")?;

    let cmd2 = serde_json::json!({
        "command": ["observe_property", 2, "playback-time"]
    })
    .to_string();
    writer
        .write_all(cmd2.as_bytes())
        .await
        .context("Failed to write observe_property playback-time")?;
    writer.write_all(b"\n").await.context("Failed to write newline")?;

    Ok(())
}

fn command_to_json(cmd: &MpvCommand) -> String {
    match cmd {
        MpvCommand::Play => {
            serde_json::json!({ "command": ["set_property", "pause", false] }).to_string()
        }
        MpvCommand::Pause => {
            serde_json::json!({ "command": ["set_property", "pause", true] }).to_string()
        }
        MpvCommand::Seek(time) => {
            serde_json::json!({ "command": ["seek", time, "absolute"] }).to_string()
        }
    }
}

async fn read_events_loop<R: AsyncBufReadExt + Unpin>(
    reader: &mut R,
    event_tx: &mpsc::Sender<MpvEvent>,
) {
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => {
                let _ = event_tx.send(MpvEvent::Disconnected).await;
                break;
            }
            Ok(_) => {
                let trimmed = line.trim().to_string();
                if trimmed.is_empty() {
                    continue;
                }

                match serde_json::from_str::<MpvEventRaw>(&trimmed) {
                    Ok(raw) => {
                        if raw.event.as_deref() == Some("property-change") {
                            match (raw.id, raw.name.as_deref()) {
                                (Some(1), Some("pause")) => {
                                    if let Some(serde_json::Value::Bool(paused)) = raw.data {
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
                                    if let Some(val) = raw.data {
                                        if let Some(time) = val.as_f64() {
                                            let _ = event_tx.send(MpvEvent::TimePos(time)).await;
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
                                "file-loaded" => println!("[MPV] Raw event: file-loaded"),
                                "end-file" => println!("[MPV] Raw event: end-file"),
                                _ => {
                                    if let Some(err) = raw.error {
                                        println!(
                                            "[MPV] Reply error: {} request_id={:?}",
                                            err, raw.request_id
                                        );
                                    } else {
                                        println!("[MPV] Raw event: {} (ignored)", event_name);
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("[MPV] Failed to parse event JSON: {} | raw: {}", e, trimmed);
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
}

// ============================================================================
// MpvClient — platform-specific connection
// ============================================================================

pub struct MpvClient {
    socket_path: PathBuf,
}

impl MpvClient {
    pub fn new(socket_path: PathBuf) -> Self {
        Self { socket_path }
    }

    pub async fn connect(&self) -> Result<(mpsc::Sender<MpvCommand>, mpsc::Receiver<MpvEvent>)> {
        let socket_path = self.socket_path.clone();
        let stream = connect_ipc(socket_path).await?;

        // tokio::io::split works on any AsyncRead + AsyncWrite type
        let (reader_half, writer_half) = tokio::io::split(stream);
        let mut reader = BufReader::new(reader_half);
        let mut writer = writer_half;

        let (cmd_tx, mut cmd_rx) = mpsc::channel::<MpvCommand>(32);
        let (event_tx, event_rx) = mpsc::channel::<MpvEvent>(32);

        send_observe_commands(&mut writer)
            .await
            .context("Failed to send observe commands")?;
        println!("[MPV] Observing pause and playback-time properties");

        tokio::spawn(async move {
            read_events_loop(&mut reader, &event_tx).await;
        });

        tokio::spawn(async move {
            while let Some(cmd) = cmd_rx.recv().await {
                let json = command_to_json(&cmd);
                if let Err(e) = writer.write_all(json.as_bytes()).await {
                    eprintln!("[MPV] Failed to send command: {}", e);
                    break;
                }
                if let Err(e) = writer.write_all(b"\n").await {
                    eprintln!("[MPV] Failed to send command: {}", e);
                    break;
                }
            }
        });

        Ok((cmd_tx, event_rx))
    }
}

// ============================================================================
// Platform-specific IPC connection
// ============================================================================

#[cfg(unix)]
async fn connect_ipc(
    path: PathBuf,
) -> Result<impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin> {
    let path = &path;
    tokio::net::UnixStream::connect(path)
        .await
        .with_context(|| format!("Failed to connect to MPV socket at {:?}", path))
}

#[cfg(windows)]
async fn connect_ipc(
    path: PathBuf,
) -> Result<impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin> {
    let path = &path;
    let pipe_name = format!(
        r#"\\.\pipe\{}"#,
        path.file_name().unwrap_or_default().to_string_lossy()
    );

    tokio::net::windows::named_pipe::ClientOptions::new()
        .open(&pipe_name)
        .with_context(|| format!("Failed to connect to MPV named pipe {}", pipe_name))
}
