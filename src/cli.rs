use crate::{
    discovery::{self, DiscoveredDevice},
    net::{AppDaemon, TransferClient},
};
use clap::{Parser, Subcommand};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
};
use tokio::{
    io::{self, AsyncBufReadExt},
    sync::mpsc,
};

#[derive(Parser)]
#[command(name = "Tensou")]
struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Send a file or folder over the local network
    Send {
        /// The absolute or relative path to the file/folder you want to send
        #[arg(required = true)]
        path: PathBuf,
    },

    /// Listen for incoming file transfers
    Receive {
        /// Optional: Force the server to bind to a specific port
        #[arg(short, long, default_value_t = 0)]
        port: u16,

        /// Optional: Override the default save location
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
}

pub fn resolve_save_directory(user_provided_path: Option<PathBuf>) -> anyhow::Result<PathBuf> {
    if let Some(path) = user_provided_path {
        std::fs::create_dir_all(&path)?;
        return Ok(path.canonicalize()?);
    }

    let downloads_dir = dirs::download_dir()
        .ok_or_else(|| anyhow::anyhow!("Could not locate the system Downloads directory"))?;

    let tensou_dir = downloads_dir.join("Tensou");

    std::fs::create_dir_all(&tensou_dir)?;
    Ok(tensou_dir.canonicalize()?)
}

pub async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Send { path } => {
            println!("Scanning for receivers... (Type a number and press Enter to connect)");

            let (tx, mut rx) = mpsc::channel::<DiscoveredDevice>(10);
            tokio::spawn(async move {
                let _ = discovery::scan_for_receivers(tx).await;
            });

            let mut devices = Vec::new();
            let mut stdin = io::BufReader::new(io::stdin()).lines();

            let selected_addr = loop {
                tokio::select! {
                    Some(device) = rx.recv() => {
                        devices.push(device.addr);
                        println!("[{}] {} ({})", devices.len(), device.hostname, device.addr);
                    }

                    Ok(Some(line)) = stdin.next_line() => {
                        if let Ok(index) = line.trim().parse::<usize>() {
                            if index > 0 && index <= devices.len() {
                                break devices[index - 1];
                            } else {
                                println!("Invalid selection. Please type a number from the list.");
                            }
                        }
                    }
                }
            };

            println!("Connecting to {}...", selected_addr);

            let client = TransferClient::connect(selected_addr, &path).await?;
            client.process_chunks().await?;

            println!("Files sent successfully!");

            Ok(())
        }
        Commands::Receive { port, output } => {
            let target_dir = resolve_save_directory(output)?;
            let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port);
            let daemon = AppDaemon::new(bind_addr)?;

            daemon.run(target_dir).await;

            Ok(())
        }
    }
}
