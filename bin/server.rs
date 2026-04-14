use anyhow::Result;
use clap::Parser;
use syncplaympv::network::RelayServer;

#[derive(Parser)]
#[command(name = "syncplaympv-server")]
#[command(about = "Relay server for SyncPlayMPV — synchronized playback over network")]
struct Cli {
    /// Network port
    #[arg(long, default_value_t = 5001)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    println!("Starting relay server on port {}...", cli.port);

    let server = RelayServer::new(cli.port);
    server.run().await
}
