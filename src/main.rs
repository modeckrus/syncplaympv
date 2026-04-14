mod mpv;
mod network;

use anyhow::Result;
use clap::{Parser, Subcommand};
use mpv::{MpvClient, MpvCommand};
use network::{ClockSync, ClientEvent, RelayClient, ServerEvent};
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Parser)]
#[command(name = "syncplaympv")]
#[command(about = "SyncPlay-like application for MPV — synchronized playback over network")]
struct Cli {
    #[command(subcommand)]
    mode: Mode,

    /// Network port
    #[arg(long, default_value_t = 4001)]
    port: u16,

    /// MPV IPC socket path (auto-detected if not specified)
    #[arg(long)]
    mpv_socket: Option<PathBuf>,
}

#[derive(Subcommand)]
enum Mode {
    /// Run as relay server (simple broadcast relay on VPS)
    Server,
    /// Run as client — launches MPV, connects to relay server, syncs playback
    Client {
        /// Relay server address (hostname only, without port)
        #[arg(default_value = "counsler.pro")]
        server: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.mode {
        Mode::Server => run_server(cli.port).await,
        Mode::Client { server } => run_client(server, cli.port, cli.mpv_socket).await,
    }
}

// ============================================================================
// Relay Server — простой ретранслятор
// ============================================================================

async fn run_server(port: u16) -> Result<()> {
    println!("Starting relay server on port {}...", port);

    let server = network::RelayServer::new(port);
    server.run().await
}

// ============================================================================
// Client — запускает MPV, подключается к серверу, синхронизирует воспроизведение
// ============================================================================

async fn run_client(server: String, port: u16, mpv_socket: Option<PathBuf>) -> Result<()> {
    println!("Starting client mode...");
    println!("Relay server: {}:{}", server, port);

    // Determine MPV socket path
    let socket_path = mpv_socket.unwrap_or_else(|| mpv::default_socket_path());

    // Launch MPV
    println!("[INIT] Launching MPV...");
    let mut mpv_child = mpv::launch_mpv(&socket_path).await?;

    // Connect to MPV IPC
    println!("[INIT] Connecting to MPV IPC socket...");
    let mpv_client = MpvClient::new(socket_path.clone());
    let (mpv_cmd_tx, mut mpv_event_rx) = mpv_client.connect().await?;
    println!("[INIT] Connected to MPV IPC");

    // Connect to relay server (includes clock sync)
    println!("[INIT] Connecting to relay server at {}:{}", server, port);
    let relay_client = RelayClient::new(server, port);
    let (events_to_server, mut events_from_server, clock_sync) = relay_client.connect().await?;
    println!("[INIT] Connected to relay server (clock offset: {}ms)", clock_sync.offset_ms);
    println!("[INIT] === Ready! Drag a file into MPV and press play ===");
    println!("[INIT] Waiting for events...\n");

    // Main loop: bridge MPV ↔ relay server
    let mut is_paused = true;
    let mut current_time: f64 = 0.0;
    // Suppress local MPV events that are side-effects of our own commands (e.g. seek → pause).
    // Duration should cover MPV processing time after seek/pause/play.
    let suppress_duration = Duration::from_millis(400);
    let mut suppress_until: Option<Instant> = None;

    loop {
        tokio::select! {
            // MPV event → react and send to relay server (only if not suppressed)
            Some(mpv_event) = mpv_event_rx.recv() => {
                match mpv_event {
                    mpv::MpvEvent::Pause => {
                        if let Some(deadline) = suppress_until {
                            if Instant::now() < deadline {
                                println!("[MPV] Pause event (suppressed — side effect of our command)");
                                continue;
                            } else {
                                suppress_until = None;
                            }
                        }
                        println!("[MPV] Pause event (local)");
                        is_paused = true;
                        let pos_ms = (current_time * 1000.0) as u64;
                        println!("[NET] Sending PAUSE_READY pos={}ms", pos_ms);
                        let _ = events_to_server.send(ClientEvent::PauseReady(pos_ms)).await;
                    }
                    mpv::MpvEvent::Unpause => {
                        if let Some(deadline) = suppress_until {
                            if Instant::now() < deadline {
                                println!("[MPV] Unpause event (suppressed — side effect of our command)");
                                continue;
                            } else {
                                suppress_until = None;
                            }
                        }
                        println!("[MPV] Play event (local)");
                        is_paused = false;
                        // Our local play is a request to the server — send READY
                        let pos_ms = (current_time * 1000.0) as u64;
                        println!("[NET] Sending READY pos={}ms", pos_ms);
                        let _ = events_to_server.send(ClientEvent::Ready(pos_ms)).await;
                        // Immediately pause locally — we'll play when server says START
                        is_paused = true;
                        suppress_until = Some(Instant::now() + suppress_duration);
                        let _ = mpv_cmd_tx.send(MpvCommand::Pause).await;
                    }
                    mpv::MpvEvent::TimePos(time) => {
                        current_time = time;
                    }
                    mpv::MpvEvent::Disconnected => {
                        eprintln!("\n[ERROR] MPV disconnected, exiting...");
                        break;
                    }
                }
            }
            // Relay server event → schedule actions
            Some(net_event) = events_from_server.recv() => {
                match net_event {
                    ServerEvent::Pong { .. } => {
                        // Already handled during clock sync
                    }
                    ServerEvent::Start { deadline_ms, pos_ms } => {
                        schedule_play(&mpv_cmd_tx, &clock_sync, deadline_ms, pos_ms, &mut is_paused).await;
                        suppress_until = Some(Instant::now() + suppress_duration);
                    }
                    ServerEvent::PauseAt { deadline_ms, pos_ms } => {
                        schedule_pause(&mpv_cmd_tx, &clock_sync, deadline_ms, pos_ms, &mut is_paused).await;
                        suppress_until = Some(Instant::now() + suppress_duration);
                    }
                    ServerEvent::SeekAt { deadline_ms, pos_ms } => {
                        schedule_seek(&mpv_cmd_tx, &clock_sync, deadline_ms, pos_ms, &mut is_paused, &mut current_time).await;
                        suppress_until = Some(Instant::now() + suppress_duration);
                    }
                    ServerEvent::State { playing, pos_ms, playback_started_at_ms } => {
                        handle_state(&mpv_cmd_tx, playing, pos_ms as f64 / 1000.0, playback_started_at_ms, &clock_sync, &mut is_paused, &mut current_time).await;
                        suppress_until = Some(Instant::now() + suppress_duration);
                    }
                }
            }
            // Ctrl+C received
            _ = tokio::signal::ctrl_c() => {
                println!("\n[INIT] Ctrl+C received, shutting down...");
                break;
            }
        }
    }

    // Clean up MPV process
    if let Err(e) = mpv_child.kill().await {
        eprintln!("Failed to kill MPV process: {}", e);
    }

    // Clean up socket file on Unix
    if !cfg!(windows) {
        let _ = std::fs::remove_file(&socket_path);
    }

    Ok(())
}

// ============================================================================
// Scheduled action helpers
// ============================================================================

/// Convert server wall-clock deadline to a local tokio::time::Instant
fn deadline_to_instant(deadline_ms: u64, clock_sync: &ClockSync) -> Instant {
    let local_deadline_ms = clock_sync.server_to_local_ms(deadline_ms);
    // tokio::time::Instant is based on monotonic clock, not wall clock.
    // We calculate the delay from now instead.
    let now_ms = network::now_ms();
    let delay_ms = local_deadline_ms.saturating_sub(now_ms);
    Instant::now() + Duration::from_millis(delay_ms)
}

async fn schedule_play(
    mpv_cmd_tx: &tokio::sync::mpsc::Sender<MpvCommand>,
    clock_sync: &ClockSync,
    deadline_ms: u64,
    pos_ms: u64,
    is_paused: &mut bool,
) {
    let pos_sec = pos_ms as f64 / 1000.0;
    let instant = deadline_to_instant(deadline_ms, clock_sync);
    let now = Instant::now();
    let delay = instant.saturating_duration_since(now);
    println!(
        "[SCHED] START: seek to {:.2}s, play in {:?}",
        pos_sec, delay
    );

    // Seek first
    let _ = mpv_cmd_tx.send(MpvCommand::Seek(pos_sec)).await;

    // Wait until deadline
    if instant > now {
        tokio::time::sleep_until(instant).await;
    } else {
        let elapsed = now.saturating_duration_since(instant);
        let elapsed_ms = elapsed.as_millis() as u64;
        let adjusted_pos = pos_ms + elapsed_ms;
        let adjusted_sec = adjusted_pos as f64 / 1000.0;
        println!(
            "[SCHED] Deadline passed, compensating: seek to {:.2}s",
            adjusted_sec
        );
        let _ = mpv_cmd_tx.send(MpvCommand::Seek(adjusted_sec)).await;
    }

    println!("[SCHED] → Sending Play to MPV");
    *is_paused = false;
    let _ = mpv_cmd_tx.send(MpvCommand::Play).await;
}

async fn schedule_pause(
    mpv_cmd_tx: &tokio::sync::mpsc::Sender<MpvCommand>,
    clock_sync: &ClockSync,
    deadline_ms: u64,
    pos_ms: u64,
    is_paused: &mut bool,
) {
    let pos_sec = pos_ms as f64 / 1000.0;
    let instant = deadline_to_instant(deadline_ms, clock_sync);
    let now = Instant::now();
    let delay = instant.saturating_duration_since(now);
    println!(
        "[SCHED] PAUSE_AT: seek to {:.2}s, pause in {:?}",
        pos_sec, delay
    );

    // Seek first
    let _ = mpv_cmd_tx.send(MpvCommand::Seek(pos_sec)).await;

    // Wait until deadline
    if instant > now {
        tokio::time::sleep_until(instant).await;
    }

    println!("[SCHED] → Sending Pause to MPV");
    *is_paused = true;
    let _ = mpv_cmd_tx.send(MpvCommand::Pause).await;
}

async fn schedule_seek(
    mpv_cmd_tx: &tokio::sync::mpsc::Sender<MpvCommand>,
    clock_sync: &ClockSync,
    deadline_ms: u64,
    pos_ms: u64,
    is_paused: &mut bool,
    current_time: &mut f64,
) {
    let pos_sec = pos_ms as f64 / 1000.0;
    let instant = deadline_to_instant(deadline_ms, clock_sync);
    let now = Instant::now();
    let delay = instant.saturating_duration_since(now);
    println!(
        "[SCHED] SEEK_AT: seek to {:.2}s in {:?}",
        pos_sec, delay
    );

    // Wait until deadline
    if instant > now {
        tokio::time::sleep_until(instant).await;
    }

    println!("[SCHED] → Sending Seek to MPV");
    *current_time = pos_sec;
    let _ = mpv_cmd_tx.send(MpvCommand::Seek(pos_sec)).await;
}

async fn handle_state(
    mpv_cmd_tx: &tokio::sync::mpsc::Sender<MpvCommand>,
    playing: bool,
    pos_sec: f64,
    playback_started_at_ms: Option<u64>,
    clock_sync: &ClockSync,
    is_paused: &mut bool,
    current_time: &mut f64,
) {
    println!(
        "[STATE] Late joiner: playing={} pos={:.2}s",
        playing, pos_sec
    );

    // Seek to the position
    let _ = mpv_cmd_tx.send(MpvCommand::Seek(pos_sec)).await;
    *current_time = pos_sec;

    if playing {
        // If server says playback is active, start playing
        println!("[STATE] → Starting playback to sync with others");
        *is_paused = false;
        let _ = mpv_cmd_tx.send(MpvCommand::Play).await;
    } else {
        println!("[STATE] → Paused (matching server state)");
        *is_paused = true;
    }

    // Suppress unused warnings
    let _ = (clock_sync, playback_started_at_ms);
}
