use anyhow::{Result, bail};
use clap::Parser;
use rfd::FileDialog;
use std::path::PathBuf;
use std::time::Duration;
use syncplaympv::mpv::{self, MpvClient, MpvCommand};
use syncplaympv::network::{self, ClientEvent, RelayClient, ServerEvent};
use tokio::time::Instant;

#[derive(Parser)]
#[command(name = "syncplaympv-client")]
#[command(about = "SyncPlayMPV client — launches MPV, connects to relay server, syncs playback")]
struct Cli {
    /// Relay server address
    #[arg(default_value = "counsler.pro")]
    server: String,

    /// Network port
    #[arg(long, default_value_t = 5001)]
    port: u16,

    /// MPV IPC socket path (auto-detected if not specified)
    #[arg(long)]
    mpv_socket: Option<PathBuf>,

    /// Video file path
    #[arg(short = 'v', long)]
    video: Option<PathBuf>,

    /// External audio file path
    #[arg(short = 'a', long)]
    audio: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    println!("Starting client mode...");
    println!("Relay server: {}:{}", cli.server, cli.port);

    let (video, audio) = match (cli.video, cli.audio) {
        (Some(v), Some(a)) => (Some(v), Some(a)),
        (Some(v), None) => (Some(v), None),
        (None, _) => {
            println!("[INIT] No files specified — opening file picker...");
            let video = FileDialog::new().set_title("Select video file").pick_file();
            let Some(video) = video else {
                bail!("No video file selected");
            };
            let audio = FileDialog::new()
                .set_title("Select audio file (optional — close to skip)")
                .pick_file();
            (Some(video), audio)
        }
    };

    let socket_path = cli.mpv_socket.unwrap_or_else(|| mpv::default_socket_path());

    println!("[INIT] Launching MPV...");
    let mut mpv_child = mpv::launch_mpv(&socket_path, video.as_deref(), audio.as_deref()).await?;

    println!("[INIT] Connecting to MPV IPC socket...");
    let mpv_client = MpvClient::new(socket_path.clone());
    let (mpv_cmd_tx, mut mpv_event_rx) = mpv_client.connect().await?;
    println!("[INIT] Connected to MPV IPC");

    let mut is_paused = true;
    let mut current_time: f64 = 0.0;
    let suppress_duration = Duration::from_millis(400);
    let mut suppress_until: Option<Instant> = None;

    // Relay connection state — replaced on each reconnect
    let relay_client = RelayClient::new(cli.server.clone(), cli.port);
    let (mut events_to_server, mut events_from_server, mut clock_sync) =
        relay_client.connect().await?;
    println!(
        "[INIT] Connected to relay server (clock offset: {}ms)",
        clock_sync.offset_ms
    );
    println!("[INIT] === Ready! Drag a file into MPV and press play ===\n");

    // On first connect we ARE a late joiner, so handle State normally.
    // On reconnect we skip State to avoid disrupting ongoing playback.
    let mut is_reconnect = false;

    loop {
        tokio::select! {
            Some(mpv_event) = mpv_event_rx.recv() => {
                match mpv_event {
                    mpv::MpvEvent::Pause => {
                        if let Some(deadline) = suppress_until {
                            if Instant::now() < deadline {
                                println!("[MPV] Pause event (suppressed)");
                                continue;
                            } else {
                                suppress_until = None;
                            }
                        }
                        println!("[MPV] Pause event (local)");
                        is_paused = true;
                        let pos_ms = (current_time * 1000.0) as u64;
                        let _ = events_to_server.send(ClientEvent::PauseReady(pos_ms)).await;
                    }
                    mpv::MpvEvent::Unpause => {
                        if let Some(deadline) = suppress_until {
                            if Instant::now() < deadline {
                                println!("[MPV] Unpause event (suppressed)");
                                continue;
                            } else {
                                suppress_until = None;
                            }
                        }
                        println!("[MPV] Play event (local)");
                        is_paused = false;
                        let pos_ms = (current_time * 1000.0) as u64;
                        let _ = events_to_server.send(ClientEvent::Ready(pos_ms)).await;
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

            Some(net_event) = events_from_server.recv() => {
                match net_event {
                    ServerEvent::Disconnected => {
                        eprintln!("[NET] Lost connection to relay server, reconnecting...");
                        loop {
                            tokio::time::sleep(Duration::from_secs(2)).await;
                            match relay_client.connect().await {
                                Ok((tx, rx, cs)) => {
                                    events_to_server = tx;
                                    events_from_server = rx;
                                    clock_sync = cs;
                                    is_reconnect = true;
                                    println!(
                                        "[NET] Reconnected to relay server (clock offset: {}ms)",
                                        clock_sync.offset_ms
                                    );
                                    break;
                                }
                                Err(e) => {
                                    eprintln!("[NET] Reconnect failed: {}, retrying...", e);
                                }
                            }
                        }
                    }
                    ServerEvent::Pong { .. } => {}
                    ServerEvent::State { playing, pos_ms, playback_started_at_ms } => {
                        if is_reconnect {
                            println!(
                                "[STATE] Reconnect: skipping state sync (playing={} pos={}ms)",
                                playing, pos_ms
                            );
                            is_reconnect = false;
                        } else {
                            handle_state(&mpv_cmd_tx, playing, pos_ms as f64 / 1000.0, playback_started_at_ms, &clock_sync, &mut is_paused, &mut current_time).await;
                            suppress_until = Some(Instant::now() + suppress_duration);
                        }
                    }
                    ServerEvent::Start { deadline_ms, pos_ms } => {
                        is_reconnect = false;
                        schedule_play(&mpv_cmd_tx, &clock_sync, deadline_ms, pos_ms, &mut is_paused).await;
                        suppress_until = Some(Instant::now() + suppress_duration);
                    }
                    ServerEvent::PauseAt { deadline_ms, pos_ms } => {
                        is_reconnect = false;
                        schedule_pause(&mpv_cmd_tx, &clock_sync, deadline_ms, pos_ms, &mut is_paused).await;
                        suppress_until = Some(Instant::now() + suppress_duration);
                    }
                    ServerEvent::SeekAt { deadline_ms, pos_ms } => {
                        is_reconnect = false;
                        schedule_seek(&mpv_cmd_tx, &clock_sync, deadline_ms, pos_ms, &mut is_paused, &mut current_time).await;
                        suppress_until = Some(Instant::now() + suppress_duration);
                    }
                }
            }

            _ = tokio::signal::ctrl_c() => {
                println!("\n[INIT] Ctrl+C received, shutting down...");
                break;
            }
        }
    }

    if let Err(e) = mpv_child.kill().await {
        eprintln!("Failed to kill MPV process: {}", e);
    }
    if !cfg!(windows) {
        let _ = std::fs::remove_file(&socket_path);
    }

    Ok(())
}

fn deadline_to_instant(deadline_ms: u64, clock_sync: &network::ClockSync) -> Instant {
    let local_deadline_ms = clock_sync.server_to_local_ms(deadline_ms);
    let now_ms = network::now_ms();
    let delay_ms = local_deadline_ms.saturating_sub(now_ms);
    Instant::now() + Duration::from_millis(delay_ms)
}

async fn schedule_play(
    mpv_cmd_tx: &tokio::sync::mpsc::Sender<MpvCommand>,
    clock_sync: &network::ClockSync,
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

    let _ = mpv_cmd_tx.send(MpvCommand::Seek(pos_sec)).await;

    if instant > now {
        tokio::time::sleep_until(instant).await;
    } else {
        let elapsed_ms = now.saturating_duration_since(instant).as_millis() as u64;
        let adjusted_sec = (pos_ms + elapsed_ms) as f64 / 1000.0;
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
    clock_sync: &network::ClockSync,
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

    let _ = mpv_cmd_tx.send(MpvCommand::Seek(pos_sec)).await;

    if instant > now {
        tokio::time::sleep_until(instant).await;
    }

    println!("[SCHED] → Sending Pause to MPV");
    *is_paused = true;
    let _ = mpv_cmd_tx.send(MpvCommand::Pause).await;
}

async fn schedule_seek(
    mpv_cmd_tx: &tokio::sync::mpsc::Sender<MpvCommand>,
    clock_sync: &network::ClockSync,
    deadline_ms: u64,
    pos_ms: u64,
    _is_paused: &mut bool,
    current_time: &mut f64,
) {
    let pos_sec = pos_ms as f64 / 1000.0;
    let instant = deadline_to_instant(deadline_ms, clock_sync);
    let now = Instant::now();
    let delay = instant.saturating_duration_since(now);
    println!("[SCHED] SEEK_AT: seek to {:.2}s in {:?}", pos_sec, delay);

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
    _playback_started_at_ms: Option<u64>,
    _clock_sync: &network::ClockSync,
    is_paused: &mut bool,
    current_time: &mut f64,
) {
    println!(
        "[STATE] Late joiner: playing={} pos={:.2}s",
        playing, pos_sec
    );

    let _ = mpv_cmd_tx.send(MpvCommand::Seek(pos_sec)).await;
    *current_time = pos_sec;

    if playing {
        println!("[STATE] → Starting playback to sync with others");
        *is_paused = false;
        let _ = mpv_cmd_tx.send(MpvCommand::Play).await;
    } else {
        println!("[STATE] → Paused (matching server state)");
        *is_paused = true;
    }
}
