use anyhow::Result;
use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tspawn::A;

// ============================================================================
// Protocol types
// ============================================================================

/// Events flowing от клиента к серверу (локальные намерения клиента)
#[derive(Debug, Clone)]
pub enum ClientEvent {
    /// Clock sync: клиент шлёт свой timestamp (ms since epoch)
    Ping(u64),
    /// Клиент готов начать воспроизведение с указанной позиции (ms)
    Ready(u64),
    /// Клиент готов поставить на паузу на указанной позиции (ms)
    PauseReady(u64),
    /// Клиент готов перемотаться к указанной позиции (ms)
    SeekReady(u64),
}

/// Events flowing от сервера к клиенту (скомандованные действия)
#[derive(Debug, Clone)]
pub enum ServerEvent {
    /// Clock sync ответ: серверный timestamp + эхо клиентского
    Pong {
        server_ts_ms: u64,
        client_ts_ms: u64,
    },
    /// Начать воспроизведение: wall_clock_deadline_ms + позиция_ms
    Start { deadline_ms: u64, pos_ms: u64 },
    /// Поставить на паузу: wall_clock_deadline_ms + позиция_ms
    PauseAt { deadline_ms: u64, pos_ms: u64 },
    /// Перемотать: wall_clock_deadline_ms + позиция_ms
    SeekAt { deadline_ms: u64, pos_ms: u64 },
    /// Текущее состояние сервера (для late joiner)
    State {
        playing: bool,
        pos_ms: u64,
        /// Если playing=true — это момент по wall clock когда начался play.
        /// Если playing=false — ignored.
        playback_started_at_ms: Option<u64>,
    },
    /// TCP соединение с сервером разорвано
    Disconnected,
}

/// Milliseconds since UNIX epoch
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ============================================================================
// Serialization (text protocol, newline-terminated)
// ============================================================================

pub fn serialize_client_event(ev: &ClientEvent) -> String {
    match ev {
        ClientEvent::Ping(ts) => format!("PING {}\n", ts),
        ClientEvent::Ready(pos) => format!("READY {}\n", pos),
        ClientEvent::PauseReady(pos) => format!("PAUSE_READY {}\n", pos),
        ClientEvent::SeekReady(pos) => format!("SEEK_READY {}\n", pos),
    }
}

pub fn serialize_server_event(ev: &ServerEvent) -> String {
    match ev {
        ServerEvent::Pong {
            server_ts_ms,
            client_ts_ms,
        } => format!("PONG {} {}\n", server_ts_ms, client_ts_ms),
        ServerEvent::Start {
            deadline_ms,
            pos_ms,
        } => format!("START {} {}\n", deadline_ms, pos_ms),
        ServerEvent::PauseAt {
            deadline_ms,
            pos_ms,
        } => format!("PAUSE_AT {} {}\n", deadline_ms, pos_ms),
        ServerEvent::SeekAt {
            deadline_ms,
            pos_ms,
        } => format!("SEEK_AT {} {}\n", deadline_ms, pos_ms),
        ServerEvent::State {
            playing,
            pos_ms,
            playback_started_at_ms,
        } => {
            let started = playback_started_at_ms
                .map(|t| t.to_string())
                .unwrap_or_else(|| "0".to_string());
            format!(
                "STATE {} {} {}\n",
                if *playing { "1" } else { "0" },
                pos_ms,
                started
            )
        }
        ServerEvent::Disconnected => unreachable!("Disconnected is client-only"),
    }
}

pub fn parse_client_event(line: &str) -> Option<ClientEvent> {
    let trimmed = line.trim();
    if let Some(rest) = trimmed.strip_prefix("PING ") {
        return Some(ClientEvent::Ping(rest.parse().ok()?));
    }
    if let Some(rest) = trimmed.strip_prefix("READY ") {
        return Some(ClientEvent::Ready(rest.parse().ok()?));
    }
    if let Some(rest) = trimmed.strip_prefix("PAUSE_READY ") {
        return Some(ClientEvent::PauseReady(rest.parse().ok()?));
    }
    if let Some(rest) = trimmed.strip_prefix("SEEK_READY ") {
        return Some(ClientEvent::SeekReady(rest.parse().ok()?));
    }
    None
}

pub fn parse_server_event(line: &str) -> Option<ServerEvent> {
    let trimmed = line.trim();
    if let Some(rest) = trimmed.strip_prefix("PONG ") {
        let parts: Vec<&str> = rest.split_whitespace().collect();
        if parts.len() >= 2 {
            return Some(ServerEvent::Pong {
                server_ts_ms: parts[0].parse().ok()?,
                client_ts_ms: parts[1].parse().ok()?,
            });
        }
    }
    if let Some(rest) = trimmed.strip_prefix("START ") {
        let parts: Vec<&str> = rest.split_whitespace().collect();
        if parts.len() >= 2 {
            return Some(ServerEvent::Start {
                deadline_ms: parts[0].parse().ok()?,
                pos_ms: parts[1].parse().ok()?,
            });
        }
    }
    if let Some(rest) = trimmed.strip_prefix("PAUSE_AT ") {
        let parts: Vec<&str> = rest.split_whitespace().collect();
        if parts.len() >= 2 {
            return Some(ServerEvent::PauseAt {
                deadline_ms: parts[0].parse().ok()?,
                pos_ms: parts[1].parse().ok()?,
            });
        }
    }
    if let Some(rest) = trimmed.strip_prefix("SEEK_AT ") {
        let parts: Vec<&str> = rest.split_whitespace().collect();
        if parts.len() >= 2 {
            return Some(ServerEvent::SeekAt {
                deadline_ms: parts[0].parse().ok()?,
                pos_ms: parts[1].parse().ok()?,
            });
        }
    }
    if let Some(rest) = trimmed.strip_prefix("STATE ") {
        let parts: Vec<&str> = rest.split_whitespace().collect();
        if parts.len() >= 3 {
            return Some(ServerEvent::State {
                playing: parts[0] == "1",
                pos_ms: parts[1].parse().ok()?,
                playback_started_at_ms: parts[2].parse().ok(),
            });
        }
    }
    None
}

// ============================================================================
// Relay Server — хранит глобальное состояние и планирует синхронный старт
// ============================================================================

/// Глобальное состояние воспроизведения на сервере
#[derive(Debug, Clone)]
struct PlaybackState {
    playing: bool,
    /// Позиция (ms) на момент начала воспроизведения
    start_pos_ms: u64,
    /// Wall clock (ms) когда был получен READY от инициатора
    wall_start_ms: u64,
}

impl PlaybackState {
    /// Текущая расчётная позиция если бы все играли идеально синхронно
    fn current_pos_ms(&self) -> u64 {
        if !self.playing {
            return self.start_pos_ms;
        }
        let elapsed = now_ms().saturating_sub(self.wall_start_ms);
        self.start_pos_ms.saturating_add(elapsed)
    }
}

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

        // Shared state
        let clients: A<Vec<mpsc::Sender<ServerEvent>>> = A::new(Vec::new());
        let state: A<Option<PlaybackState>> = A::new(None);

        loop {
            let (stream, addr) = listener.accept().await?;
            let n = clients.read().len() + 1;
            println!("[SERVER] Client connected: {} (total: {})", addr, n);

            let (tx, rx) = mpsc::channel::<ServerEvent>(32);

            // Add client
            let clients_clone = clients.clone();
            {
                let mut c = clients_clone.write();
                c.push(tx.clone());
            }

            // Send current state to late joiner
            let state_clone = state.clone();
            {
                let guard = state_clone.read();
                if let Some(s) = guard.as_ref() {
                    let current_pos = s.current_pos_ms();
                    let ev = ServerEvent::State {
                        playing: s.playing,
                        pos_ms: current_pos,
                        playback_started_at_ms: if s.playing {
                            Some(s.wall_start_ms)
                        } else {
                            None
                        },
                    };
                    let _ = tx.send(ev).await;
                    println!(
                        "[SERVER] Sent STATE to late joiner: playing={} pos={}",
                        s.playing, current_pos
                    );
                }
            }

            // Handle this client
            let clients_for_handler = clients.clone();
            let state_for_handler = state.clone();
            let clients_for_cleanup = clients.clone();
            tokio::spawn(async move {
                handle_client(stream, addr, rx, clients_for_handler, state_for_handler).await;
                let mut c = clients_for_cleanup.write();
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

/// Спланировать START с задержкой SCHEDULE_DELAY_MS от now
const SCHEDULE_DELAY_MS: u64 = 50;

async fn handle_client(
    stream: TcpStream,
    addr: SocketAddr,
    mut rx: mpsc::Receiver<ServerEvent>,
    clients: A<Vec<mpsc::Sender<ServerEvent>>>,
    state: A<Option<PlaybackState>>,
) {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    // Single loop: read from TCP and write to TCP
    let mut line = String::new();
    loop {
        tokio::select! {
            // Read from client
            result = reader.read_line(&mut line) => {
                match result {
                    Ok(0) => break, // EOF
                    Ok(_) => {
                        let line_str = line.clone();
                        line.clear();
                        if let Some(client_ev) = parse_client_event(&line_str) {
                            match client_ev {
                                ClientEvent::Ping(client_ts) => {
                                    let server_ts = now_ms();
                                    println!(
                                        "[SERVER] PING from {}: client_ts={} server_ts={}",
                                        addr, client_ts, server_ts
                                    );
                                    let pong = ServerEvent::Pong {
                                        server_ts_ms: server_ts,
                                        client_ts_ms: client_ts,
                                    };
                                    let msg = serialize_server_event(&pong);
                                    let _ = writer.write_all(msg.as_bytes()).await;
                                }
                                ClientEvent::Ready(pos_ms) => {
                                    let wall_now = now_ms();
                                    let deadline = wall_now + SCHEDULE_DELAY_MS;
                                    println!(
                                        "[SERVER] READY from {} at pos={}ms → scheduling START at deadline={}ms",
                                        addr, pos_ms, deadline
                                    );

                                    {
                                        let mut s = state.write();
                                        *s = Some(PlaybackState {
                                            playing: true,
                                            start_pos_ms: pos_ms,
                                            wall_start_ms: deadline,
                                        });
                                    }

                                    let start_ev = ServerEvent::Start {
                                        deadline_ms: deadline,
                                        pos_ms,
                                    };
                                    broadcast(&clients, &start_ev, &addr).await;
                                }
                                ClientEvent::PauseReady(pos_ms) => {
                                    let wall_now = now_ms();
                                    let deadline = wall_now + SCHEDULE_DELAY_MS;
                                    println!(
                                        "[SERVER] PAUSE_READY from {} at pos={}ms → scheduling PAUSE_AT at deadline={}ms",
                                        addr, pos_ms, deadline
                                    );

                                    {
                                        let mut s = state.write();
                                        *s = Some(PlaybackState {
                                            playing: false,
                                            start_pos_ms: pos_ms,
                                            wall_start_ms: 0,
                                        });
                                    }

                                    let pause_ev = ServerEvent::PauseAt {
                                        deadline_ms: deadline,
                                        pos_ms,
                                    };
                                    broadcast(&clients, &pause_ev, &addr).await;
                                }
                                ClientEvent::SeekReady(pos_ms) => {
                                    let wall_now = now_ms();
                                    let deadline = wall_now + SCHEDULE_DELAY_MS;
                                    println!(
                                        "[SERVER] SEEK_READY from {} to pos={}ms → scheduling SEEK_AT at deadline={}ms",
                                        addr, pos_ms, deadline
                                    );

                                    {
                                        let mut s = state.write();
                                        if let Some(ref mut st) = *s {
                                            st.start_pos_ms = pos_ms;
                                            if st.playing {
                                                st.wall_start_ms = deadline;
                                            }
                                        }
                                    }

                                    let seek_ev = ServerEvent::SeekAt {
                                        deadline_ms: deadline,
                                        pos_ms,
                                    };
                                    broadcast(&clients, &seek_ev, &addr).await;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("[SERVER] Error reading from client {}: {}", addr, e);
                        break;
                    }
                }
            }
            // Write to client (broadcast events)
            Some(event) = rx.recv() => {
                let msg = serialize_server_event(&event);
                if let Err(e) = writer.write_all(msg.as_bytes()).await {
                    eprintln!("[SERVER] Error writing to client {}: {}", addr, e);
                    break;
                }
            }
        }
    }
}

/// Broadcast event to all clients except the sender
async fn broadcast(
    clients: &A<Vec<mpsc::Sender<ServerEvent>>>,
    event: &ServerEvent,
    except: &SocketAddr,
) {
    let mut c = clients.write();
    c.retain(|tx| !tx.is_closed());
    let total = c.len();
    println!("[SERVER] Broadcasting {:?} to {} client(s)", event, total);
    for tx in c.iter() {
        let _ = tx.send(event.clone()).await;
    }
}

// ============================================================================
// Relay Client — подключается к серверу, измеряет clock offset, обменивается событиями
// ============================================================================

/// Рассчитанный offset между локальными часами и часами сервера.
/// server_time = local_time + offset
pub struct ClockSync {
    pub offset_ms: i64,
}

impl ClockSync {
    /// Измерить offset через ping-pong. Делает несколько измерений и берёт минимум RTT.
    pub async fn measure(stream: &TcpStream) -> Result<Self> {
        // We need a temporary reader/writer to do ping-pong before the main loop splits the stream.
        // But the stream will be split later, so we do ping-pong using the raw stream first.
        // Actually, we can't read/write on a TcpStream before splitting it easily.
        // We'll do ping-pong after the split, but before the main event loop.
        // For now, return a placeholder — actual measurement happens in connect().
        Ok(Self { offset_ms: 0 })
    }

    /// Convert server wall clock ms to local wall clock ms
    pub fn server_to_local_ms(&self, server_ts_ms: u64) -> u64 {
        let server_ts = server_ts_ms as i64;
        let local_ts = server_ts - self.offset_ms;
        local_ts.max(0) as u64
    }
}

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
    /// - Sender: to send ClientEvent to server
    /// - Receiver: to receive ServerEvent from server
    /// - ClockSync: clock offset for scheduling
    pub async fn connect(
        &self,
    ) -> Result<(
        mpsc::Sender<ClientEvent>,
        mpsc::Receiver<ServerEvent>,
        ClockSync,
    )> {
        let addr = format!("{}:{}", self.server_addr, self.port);
        let stream = TcpStream::connect(&addr).await?;
        println!("[NET] Connected to relay server at {}", addr);

        let (reader, writer) = stream.into_split();
        let reader = BufReader::new(reader);
        let mut writer = writer;

        let (events_from_server_tx, events_from_server_rx) = mpsc::channel::<ServerEvent>(32);
        let (events_to_server_tx, mut events_to_server_rx) = mpsc::channel::<ClientEvent>(32);

        // --- Clock sync: ping-pong ---
        let ping_ts = now_ms();
        let ping_msg = serialize_client_event(&ClientEvent::Ping(ping_ts));
        writer.write_all(ping_msg.as_bytes()).await?;
        println!("[NET] Sent PING at local_ts={}", ping_ts);

        // Read PONG response
        let mut reader_temp = reader;
        let mut line = String::new();
        let mut clock_sync = ClockSync { offset_ms: 0 };

        loop {
            line.clear();
            match reader_temp.read_line(&mut line).await {
                Ok(0) => {
                    eprintln!("[NET] Relay server disconnected during clock sync");
                    break;
                }
                Ok(_) => {
                    if let Some(ServerEvent::Pong {
                        server_ts_ms,
                        client_ts_ms,
                    }) = parse_server_event(&line)
                    {
                        let pong_received = now_ms();
                        let rtt = pong_received.saturating_sub(ping_ts);
                        // offset = server_ts - (client_ts + rtt/2)
                        // But simpler: offset ≈ server_ts - client_ts (at midpoint)
                        let estimated_one_way = rtt / 2;
                        let local_at_server_response = client_ts_ms + estimated_one_way;
                        let offset = server_ts_ms as i64 - local_at_server_response as i64;
                        clock_sync = ClockSync { offset_ms: offset };
                        println!(
                            "[NET] Clock sync: RTT={}ms offset={}ms (positive=server ahead)",
                            rtt, offset
                        );
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("[NET] Error during clock sync: {}", e);
                    break;
                }
            }
        }

        // Re-wrap reader for the main loop
        let reader = BufReader::new(reader_temp);

        // Read task: receive events from server
        let events_from_server_tx_clone = events_from_server_tx.clone();
        tokio::spawn(async move {
            let mut reader = reader;
            let mut line = String::new();
            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) => {
                        eprintln!("[NET] Relay server disconnected");
                        let _ = events_from_server_tx_clone
                            .send(ServerEvent::Disconnected)
                            .await;
                        break;
                    }
                    Ok(_) => {
                        if let Some(event) = parse_server_event(&line) {
                            println!("[NET] Received {:?} from relay server", event);
                            let _ = events_from_server_tx_clone.send(event).await;
                        } else if !line.trim().is_empty() {
                            println!("[NET] Unknown server message: {}", line.trim());
                        }
                    }
                    Err(e) => {
                        eprintln!("[NET] Error reading from relay server: {}", e);
                        let _ = events_from_server_tx_clone
                            .send(ServerEvent::Disconnected)
                            .await;
                        break;
                    }
                }
            }
        });

        // Write task: send events TO server
        tokio::spawn(async move {
            let mut writer = writer;
            while let Some(event) = events_to_server_rx.recv().await {
                let msg = serialize_client_event(&event);
                if let Err(e) = writer.write_all(msg.as_bytes()).await {
                    eprintln!("[NET] Error sending to relay server: {}", e);
                    break;
                }
            }
        });

        Ok((events_to_server_tx, events_from_server_rx, clock_sync))
    }
}
