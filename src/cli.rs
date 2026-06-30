use crate::{
    SERVER_PORT,
    config::Config,
    discovery::{self, DiscoveryEvent},
    gui::state::{ConsentRegistry, GuiConsentHandler, GuiEvent, GuiTransferObserver},
    net::ReceiverDaemon,
};
use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

mod recieve;
mod send;

#[derive(Parser)]
#[command(name = "Tensou")]
struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Send a file or folder over the local network
    Send {
        /// The absolute or relative path to the file/folder you want to send
        #[arg(required = true)]
        path: PathBuf,

        /// Optional: Custom IP address to send to directly
        #[arg(long)]
        ip: Option<IpAddr>,

        /// Optional: Custom port to associate with the IP
        #[arg(short, long, default_value_t = SERVER_PORT, requires = "ip")]
        port: u16,
    },

    /// Listen for incoming file transfers
    Receive {
        /// Optional: Force the server to bind to a specific port
        #[arg(short, long, default_value_t = SERVER_PORT)]
        port: u16,

        /// Optional: Override the default save location
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
}

pub async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Send { path, ip, port }) => send::run(path, ip, port).await,
        Some(Commands::Receive { port, output }) => recieve::run(port, output).await,
        None => {
            println!("Launching Tensou GUI...");

            // For detecting devices
            let (tx, devices_rx) = mpsc::channel::<DiscoveryEvent>(10);
            tokio::spawn(async move {
                let _ = discovery::scan_for_receivers(tx).await;
            });

            // Create channels for GUI events
            let (event_tx, event_rx) = mpsc::unbounded_channel::<GuiEvent>();

            let consent_registry = Arc::new(ConsentRegistry {
                pending: Mutex::new(HashMap::new()),
            });

            let daemon_event_tx = event_tx.clone();
            let daemon_consent_registry = consent_registry.clone();

            tokio::spawn(async move {
                let target_dir = resolve_save_directory(None).unwrap(); // Default ~/Downloads/Tensou
                let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), SERVER_PORT);

                if let Ok(daemon) = ReceiverDaemon::new(
                    bind_addr,
                    Config {
                        target_dir,
                        overwrite_dest: false,
                    },
                ) {
                    let cancel_token = CancellationToken::new();

                    let observer = Arc::new(GuiTransferObserver {
                        transfer_id: 0,
                        tx: daemon_event_tx.clone(),
                        is_sender: false,
                    });

                    let consent_handler = Arc::new(GuiConsentHandler {
                        registry: daemon_consent_registry,
                        event_tx: daemon_event_tx.clone(),
                    });

                    daemon.run(consent_handler, observer, cancel_token).await;
                }
            });

            crate::gui::run_gui(devices_rx, event_tx, event_rx, consent_registry)?;
            Ok(())
        }
    }
}

fn resolve_save_directory(user_provided_path: Option<PathBuf>) -> anyhow::Result<PathBuf> {
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

fn create_transfer_pb(total_bytes: u64, name: &str, is_sender: bool) -> ProgressBar {
    let pb = ProgressBar::new(total_bytes);

    let style = ProgressStyle::default_bar()
        .template(
            "{spinner:.green} {msg}\n{bytes:>10} / {total_bytes:10} [{bar:40.cyan/blue}] {percent}% {bytes_per_sec} | {eta}"
        )
        .unwrap()
        .progress_chars("━╾─");

    pb.set_style(style);

    let label = if is_sender { "Sending" } else { "Receiving" };
    if !name.is_empty() {
        pb.set_message(format!("{label}: {name}"));
    }

    pb
}
