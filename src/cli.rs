use crate::{
    SERVER_PORT,
    config::Config,
    discovery::{self, DiscoveredDevice},
    net::{ReceiverDaemon, Sender, TransferConsentHandler},
    protocol::TransferObserver,
};
use async_trait::async_trait;
use clap::{Parser, Subcommand};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{self, AsyncBufReadExt},
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;

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

struct CliSendTransfer(ProgressBar);

impl TransferObserver for CliSendTransfer {
    fn on_chunk_transferred(&self, _: Option<u32>, bytes: u64) {
        self.0.inc(bytes);
    }
}

struct CliReceiveTransfer {
    multi_progress: MultiProgress,
    // TODO: Use channels here to avoid Mutex locks
    active: Mutex<HashMap<u32, ProgressBar>>,
}

impl TransferObserver for CliReceiveTransfer {
    fn on_transfer_started(
        &self,
        transfer_id: u32,
        _peer: SocketAddr,
        total_bytes: u64,
        job_name: &str,
    ) {
        let pb = self
            .multi_progress
            .add(create_transfer_pb(total_bytes, &job_name, false));
        self.active.lock().unwrap().insert(transfer_id, pb);
    }

    fn on_chunk_transferred(&self, transfer_id: Option<u32>, bytes: u64) {
        let active = self.active.lock().unwrap();
        if let Some(pb) = transfer_id.and_then(|v| active.get(&v)) {
            pb.inc(bytes);
        }
    }

    fn on_transfer_complete(&self, transfer_id: u32) {
        if let Some(pb) = self.active.lock().unwrap().remove(&transfer_id) {
            pb.set_style(
                pb.style()
                    .clone()
                    .template("{spinner:.green} {msg:.green} [{elapsed_precise}] ✔ Completed!")
                    .expect("Invalid style"),
            );
            pb.finish_with_message("Done!");
        }
    }
}

struct CliConsent;

#[async_trait]
impl TransferConsentHandler for CliConsent {
    async fn request_consent(&self, peer: SocketAddr, job_name: &str) -> bool {
        let job_name = job_name.to_string();
        tokio::task::spawn_blocking(move || {
            println!("\nIncoming transfer from {peer}");
            dialoguer::Confirm::new()
                .with_prompt(format!("Accept '{job_name}'?"))
                .interact()
                .unwrap_or(false)
        })
        .await
        .unwrap_or(false)
    }
}

pub async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Send { path, ip, port } => {
            if !path.exists() {
                anyhow::bail!("Path '{}' does not exist.", path.display());
            }

            let display_name = path
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|| path.display().to_string());

            println!("Preparing to send: {display_name}");

            let selected_addr = match ip {
                Some(ip) => SocketAddr::new(ip, port),
                None => {
                    let spinner = ProgressBar::new_spinner();
                    spinner.set_style(
                        ProgressStyle::default_spinner()
                            .template("{spinner:.cyan} {msg}")
                            .unwrap(),
                    );
                    spinner.set_message("Scanning for receivers on the local network...");
                    spinner.enable_steady_tick(std::time::Duration::from_millis(80));

                    let (tx, mut rx) = mpsc::channel::<DiscoveredDevice>(10);
                    tokio::spawn(async move {
                        let _ = discovery::scan_for_receivers(tx).await;
                    });

                    let mut devices = Vec::new();
                    let mut stdin = io::BufReader::new(io::stdin()).lines();

                    let selected_addr = loop {
                        tokio::select! {
                            Some(device) = rx.recv() => {
                                if devices.is_empty() {
                                    spinner.finish_and_clear();
                                    println!("Found receivers (type a number to connect):\n");
                                }
                                devices.push(device.addr);
                                println!("  [{}] {} ({})", devices.len(), device.hostname, device.addr);
                            }

                            Ok(Some(line)) = stdin.next_line() => {
                                let input = line.trim().to_string();

                                if input.is_empty() {
                                    continue;
                                }

                                if devices.is_empty() {
                                    println!("  No devices found yet, still scanning...");
                                    continue;
                                }

                                match input.parse::<usize>() {
                                    Ok(index) if index > 0 && index <= devices.len() => {
                                        break devices[index - 1];
                                    }
                                    Ok(_) => {
                                        println!("  Please enter a number between 1 and {}.", devices.len());
                                    }
                                    Err(_) => {
                                        println!("  Invalid input. Enter a number to select a device.");
                                    }
                                }
                            }
                        }
                    };

                    spinner.finish_and_clear();

                    selected_addr
                }
            };

            println!("Connecting to {selected_addr}...");

            let client = Sender::connect(selected_addr, &path).await?;
            let total_bytes = client.get_remaining_bytes();

            let pb = create_transfer_pb(total_bytes, &display_name, true);

            let transfer_handle =
                tokio::spawn(
                    async move { client.process_chunks(Arc::new(CliSendTransfer(pb))).await },
                );

            transfer_handle.await??;

            println!("\nTransfer complete!");

            Ok(())
        }
        Commands::Receive { port, output } => {
            let cancel_token = CancellationToken::new();
            let cancel_clone = cancel_token.clone();

            tokio::spawn(async move {
                tokio::signal::ctrl_c()
                    .await
                    .expect("Failed to listen for Ctrl+C");
                println!("\n[!] Ctrl+C detected! Safely saving transfer states...");

                cancel_clone.cancel();
            });

            let target_dir = resolve_save_directory(output)?;
            let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port);

            let daemon = ReceiverDaemon::new(
                bind_addr,
                Config {
                    target_dir: target_dir.clone(),
                    overwrite_dest: false,
                },
            )?;

            println!("Listening on port {}", daemon.local_addr()?.port());
            println!("Saving files to: {}", target_dir.display());
            println!("   Waiting for incoming transfers...\n");

            daemon
                .run(
                    Arc::new(CliConsent),
                    Arc::new(CliReceiveTransfer {
                        multi_progress: MultiProgress::new(),
                        active: Mutex::new(HashMap::new()),
                    }),
                    cancel_token,
                )
                .await;

            Ok(())
        }
    }
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
