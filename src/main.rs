mod mpv;
mod network;

use anyhow::Result;
use clap::{Parser, Subcommand};
use mpv::{MpvClient, MpvCommand};
use network::{RelayClient, SyncEvent};
use std::path::PathBuf;

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
    let mut mpv_child = mpv::launch_mpv(&socket_path).await?;

    // Connect to MPV IPC
    let mpv_client = MpvClient::new(socket_path.clone());
    let (mpv_cmd_tx, mut mpv_event_rx) = mpv_client.connect().await?;
    println!("Connected to MPV IPC");

    // Connect to relay server
    let relay_client = RelayClient::new(server, port);
    let (events_to_server, mut events_from_server) = relay_client.connect().await?;
    println!("Connected to relay server\n");

    println!("=== Ready! Drag a file into MPV and press play ===");
    println!("Press Ctrl+C to exit\n");

    // Main loop: bridge MPV ↔ relay server
    let mut is_paused = true; // Start in paused state
    loop {
        tokio::select! {
            // MPV event → send to relay server
            Some(mpv_event) = mpv_event_rx.recv() => {
                match mpv_event {
                    mpv::MpvEvent::Pause => {
                        println!("[MPV] Paused");
                        is_paused = true;
                        let _ = events_to_server.send(SyncEvent::Pause).await;
                    }
                    mpv::MpvEvent::Unpause => {
                        println!("[MPV] Playing");
                        is_paused = false;
                        let _ = events_to_server.send(SyncEvent::Play).await;
                    }
                    mpv::MpvEvent::Disconnected => {
                        eprintln!("\nMPV disconnected, exiting...");
                        break;
                    }
                }
            }
            // Relay server event → control local MPV
            Some(net_event) = events_from_server.recv() => {
                match net_event {
                    SyncEvent::Play => {
                        if is_paused {
                            println!("[NET] Play — resuming playback");
                            is_paused = false;
                            let _ = mpv_cmd_tx.send(MpvCommand::Play).await;
                        }
                    }
                    SyncEvent::Pause => {
                        if !is_paused {
                            println!("[NET] Pause — pausing playback");
                            is_paused = true;
                            let _ = mpv_cmd_tx.send(MpvCommand::Pause).await;
                        }
                    }
                }
            }
            // Ctrl+C received
            _ = tokio::signal::ctrl_c() => {
                println!("\nShutting down...");
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
