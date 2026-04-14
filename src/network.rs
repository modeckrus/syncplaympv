use anyhow::Result;
use std::net::SocketAddr;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tspawn::A;

/// Network events for sync
#[derive(Debug, Clone)]
pub enum SyncEvent {
    Play,
    Pause,
    /// Seek to position (seconds)
    Seek(f64),
}

/// Serialize event to string for network transmission
pub fn serialize_event(event: &SyncEvent) -> String {
    match event {
        SyncEvent::Play => "PLAY\n".to_string(),
        SyncEvent::Pause => "PAUSE\n".to_string(),
        SyncEvent::Seek(time) => format!("SEEK {}\n", time),
    }
}

/// Parse string to event
pub fn parse_event(line: &str) -> Option<SyncEvent> {
    let trimmed = line.trim();
    if let Some(time_str) = trimmed.strip_prefix("SEEK ") {
        if let Ok(time) = time_str.parse::<f64>() {
            return Some(SyncEvent::Seek(time));
        }
    }
    match trimmed {
        "PLAY" => Some(SyncEvent::Play),
        "PAUSE" => Some(SyncEvent::Pause),
        _ => None,
    }
}

// ============================================================================
// Relay Server — просто ретранслирует сообщения всем подключённым клиентам
// ============================================================================

pub struct RelayServer {
    port: u16,
}

impl RelayServer {
    pub fn new(port: u16) -> Self {
        Self { port }
    }

    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(format!("0.0.0.0:{}", self.port)).await?;
        println!("[SERVER] Relay server listening on port {}", self.port);

        // Shared list of connected clients using tspawn
        let clients: A<Vec<mpsc::Sender<SyncEvent>>> = A::new(Vec::new());

        loop {
            let (stream, addr) = listener.accept().await?;
            println!(
                "[SERVER] Client connected: {} (total: {})",
                addr,
                clients.read().len() + 1
            );

            let (tx, rx) = mpsc::channel::<SyncEvent>(32);

            // Add client to the list
            let clients_for_spawn = clients.clone();
            {
                let mut c = clients_for_spawn.write();
                c.push(tx);
            }

            // Handle this client
            let clients_for_handler = clients.clone();
            tokio::spawn(async move {
                handle_client(stream, addr, rx, clients_for_handler.clone()).await;
                // Clean up: remove closed clients
                let mut c = clients_for_handler.write();
                c.retain(|tx| !tx.is_closed());
                println!(
                    "[SERVER] Client disconnected: {} (total: {})",
                    addr,
                    c.len()
                );
            });
        }
    }
}

/// Handle a single client connection on the relay server.
/// Reads events from the client and broadcasts to ALL other clients (including itself for echo,
/// but the client already handles local MPV state, so it's fine).
async fn handle_client(
    stream: TcpStream,
    addr: SocketAddr,
    mut rx: mpsc::Receiver<SyncEvent>,
    clients: A<Vec<mpsc::Sender<SyncEvent>>>,
) {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    // Read task: receive events from this client and broadcast
    let clients_for_read = clients.clone();
    let _read_handle = tokio::spawn(async move {
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => {
                    // EOF — client disconnected
                    break;
                }
                Ok(_) => {
                    if let Some(event) = parse_event(&line) {
                        let total = clients_for_read.read().len();
                        println!(
                            "[SERVER] Received {:?} from {} → broadcasting to {} client(s)",
                            event, addr, total
                        );
                        // Broadcast to ALL connected clients
                        let mut c = clients_for_read.write();
                        c.retain(|tx| !tx.is_closed());
                        for client_tx in c.iter() {
                            let _ = client_tx.send(event.clone()).await;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error reading from client {}: {}", addr, e);
                    break;
                }
            }
        }
    });

    // Write task: send events TO this client (from broadcast)
    let _write_handle = tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            let msg = serialize_event(&event);
            if let Err(e) = writer.write_all(msg.as_bytes()).await {
                eprintln!("Error writing to client {}: {}", addr, e);
                break;
            }
        }
    });

    // Wait for read task to finish (client disconnect)
    let _ = _read_handle.await;
}

// ============================================================================
// Relay Client — подключается к relay серверу и обменивается событиями
// ============================================================================

pub struct RelayClient {
    server_addr: String,
    port: u16,
}

impl RelayClient {
    pub fn new(server_addr: String, port: u16) -> Self {
        Self { server_addr, port }
    }

    /// Connect to relay server.
    /// Returns:
    /// - Sender: to send events to server (which broadcasts to all peers)
    /// - Receiver: to receive events from other peers
    pub async fn connect(&self) -> Result<(mpsc::Sender<SyncEvent>, mpsc::Receiver<SyncEvent>)> {
        let addr = format!("{}:{}", self.server_addr, self.port);
        let stream = TcpStream::connect(&addr).await?;
        println!("Connected to relay server at {}", addr);

        let (reader, mut writer) = stream.into_split();
        let reader = BufReader::new(reader);

        let (events_from_peers_tx, events_from_peers_rx) = mpsc::channel::<SyncEvent>(32);
        let (events_to_server_tx, mut events_to_server_rx) = mpsc::channel::<SyncEvent>(32);

        // Read task: receive events from server → forward to peers
        tokio::spawn(async move {
            let mut reader = reader;
            let mut line = String::new();
            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) => {
                        // EOF — server disconnected
                        eprintln!("[NET] Relay server disconnected");
                        break;
                    }
                    Ok(_) => {
                        if let Some(event) = parse_event(&line) {
                            println!("[NET] Received {:?} from relay server", event);
                            let _ = events_from_peers_tx.send(event).await;
                        }
                    }
                    Err(e) => {
                        eprintln!("[NET] Error reading from relay server: {}", e);
                        break;
                    }
                }
            }
        });

        // Write task: send events TO server (from local MPV or forwarded)
        tokio::spawn(async move {
            while let Some(event) = events_to_server_rx.recv().await {
                println!("[NET] Sending {:?} to relay server", event);
                let msg = serialize_event(&event);
                if let Err(e) = writer.write_all(msg.as_bytes()).await {
                    eprintln!("[NET] Error sending to relay server: {}", e);
                    break;
                }
            }
        });

        Ok((events_to_server_tx, events_from_peers_rx))
    }
}
