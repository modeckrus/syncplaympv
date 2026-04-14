# SyncPlayMPV

A lightweight **SyncPlay-like** application for [MPV](https://mpv.io/) — enables synchronized video playback across multiple machines over a network.

Built in Rust with `tokio` for async I/O, MPV IPC, and TCP-based relay.

---

## What It Does

SyncPlayMPV allows multiple people to watch a video **together**, with playback (play/pause) actions synchronized across all participants. Think of it as a minimal [SyncPlay](https://github.com/syncplay/syncplay) alternative, but focused only on play/pause sync and tightly integrated with MPV.

### Architecture

The system uses a **client-server** model with a simple broadcast relay server:

```
┌──────────┐     TCP      ┌────────────────┐     TCP      ┌──────────┐
│ Client 1 │◄────────────►│  Relay Server  │◄────────────►│ Client 2 │
│  (MPV)   │   PLAY/PAUSE │  (port 4001)   │   PLAY/PAUSE │  (MPV)   │
└──────────┘              └────────────────┘              └──────────┘
```

- **Relay Server** — a lightweight TCP relay that receives `PLAY`/`PAUSE` events from any connected client and broadcasts them to all others. Stateless, no database, just forwarding.
- **Client** — launches a local MPV instance, monitors its play/pause events via IPC socket, and relays them to the server. Incoming network events control the local MPV.

### Flow

1. One person starts the **server** (`syncplaympv server`) — typically on a VPS or any machine reachable by all participants.
2. Each participant starts a **client** (`syncplaympv client <server-address>`).
3. The client launches MPV with IPC enabled.
4. When any participant presses **play** or **pause** in their MPV, the event is sent to the relay server, which broadcasts it to all other clients.
5. Each client receives the event and issues the corresponding command to its local MPV instance.

---

## Features

- **Play/Pause synchronization** across all connected clients
- **Seek synchronization** — seeking on one client seeks on all others
- **Playback time tracking** — monitors `playback-time` property via MPV IPC
- **Relay server** — simple TCP broadcast relay, no external dependencies
- **MPV IPC integration** — uses `observe_property` for proper event tracking (`pause`, `playback-time`)
- **Cross-platform** — works on Linux/macOS (Unix sockets) and Windows (named pipes)
- **Automatic MPV launch** — client spawns and manages the MPV process
- **Graceful cleanup** — kills MPV and removes socket files on exit

---

## Prerequisites

- [MPV](https://mpv.io/) installed and available in `PATH`
- Rust toolchain (`cargo`)

---

## Usage

### Start the relay server

```bash
cargo run -- server --port 4001
```

The server listens on `0.0.0.0:<port>` and can handle multiple concurrent clients.

### Connect as a client

```bash
cargo run -- client counsler.pro --port 4001
```

This will:
- Launch MPV with IPC enabled
- Connect to the relay server at `counsler.pro:4001`
- Synchronize play/pause with other connected clients

You can also specify a custom MPV socket path:

```bash
cargo run -- client myserver.com --mpv-socket /tmp/my_socket.sock
```

### Default values

| Argument      | Default         |
|---------------|-----------------|
| `--port`      | `4001`          |
| `server`      | `counsler.pro`  |
| `--mpv-socket`| Auto-detect (`/tmp/mpv_sync.sock` on Linux/macOS) |

---

## How It Works

### MPV Communication

- **Launch**: MPV is started with `--input-ipc-server=<socket>`, `--idle=yes`, `--keep-open=yes`, and `--force-window=yes`
- **Property observation**: Uses `observe_property` to watch `pause` (ID 1) and `playback-time` (ID 2) for real-time changes
- **Events received**: `property-change` events for pause state and playback time, plus `shutdown`, `file-loaded`, `end-file`
- **Commands sent**: `set_property "pause" true/false` to control playback, `seek <time> absolute` to seek

### Network Protocol

Simple text-based protocol over TCP:

| Event  | Wire Format    |
|--------|----------------|
| Play   | `PLAY\n`       |
| Pause  | `PAUSE\n`      |
| Seek   | `SEEK <time>\n`|

The relay server broadcasts every received event to **all** connected clients (including the sender, which is safe because clients check their local state before acting).

### Concurrency

- Built on `tokio` async runtime
- Each client connection on the server is handled in its own spawned task
- `tspawn::A` provides thread-safe shared state for the client list on the server
- IPC read/write and network read/write are split into separate tasks per connection

---

## Project Structure

```
src/
├── main.rs      — CLI entry point, server/client dispatch, main event loop
├── mpv.rs       — MPV process launch, IPC socket communication, event/command types
└── network.rs   — Relay server, relay client, TCP connection handling, protocol serialization
```

---

## Building

```bash
cargo build --release
```

The binary will be at `target/release/syncplaympv`.

---

## License

MIT
