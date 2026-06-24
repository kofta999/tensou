use crate::{
    discovery::{self, DiscoveredDevice},
    net::{AppDaemon, Sender, TransferConsentHandler},
    protocol::{TransferEvent, TransferEventSender},
};
use async_trait::async_trait;
use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};
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

            let client = Sender::connect(selected_addr, &path).await?;
            let total_bytes = client.get_remaining_bytes();

            let pb = create_transfer_pb(total_bytes, "", true);

            let (tx, mut rx) = mpsc::channel::<u64>(100);

            let transfer_handle =
                tokio::spawn(async move { client.process_chunks(Some(tx)).await });

            while let Some(bytes_sent) = rx.recv().await {
                pb.inc(bytes_sent);
            }

            transfer_handle.await??;

            Ok(())
        }
        Commands::Receive { port, output } => {
            let target_dir = resolve_save_directory(output)?;
            let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port);

            let (tx, mut rx) = tokio::sync::broadcast::channel::<TransferEvent>(100);

            let daemon = AppDaemon::new(
                bind_addr,
                Some(tx.clone()),
                Arc::new(InteractiveConsent { event_tx: tx }),
            )?;

            tokio::spawn(async move {
                daemon.run(target_dir).await;
            });

            let multi_progress = indicatif::MultiProgress::new();
            let mut active_bars: std::collections::HashMap<u32, indicatif::ProgressBar> =
                HashMap::new();

            while let Ok(event) = rx.recv().await {
                match event {
                    TransferEvent::ConsentRequested {
                        peer,
                        job_name,
                        reply_tx,
                    } => {
                        println!("\nIncoming transfer from {}!", peer);

                        let accepted = dialoguer::Confirm::new()
                            .with_prompt(format!(
                                // "Accept '{}' ({} bytes across {} files)?",
                                "Accept '{}'?",
                                job_name, //total_bytes, file_count
                            ))
                            .interact()
                            .unwrap_or(false);

                        if let Some(tx) = reply_tx.lock().expect("Poisoned mutex").take() {
                            let _ = tx.send(accepted);
                        }
                    }
                    TransferEvent::TransferStarted {
                        transfer_id,
                        total_bytes,
                        job_name,
                        ..
                    } => {
                        let pb =
                            multi_progress.add(create_transfer_pb(total_bytes, &job_name, false));
                        active_bars.insert(transfer_id, pb);
                    }
                    TransferEvent::ChunkReceived { transfer_id, bytes } => {
                        if let Some(pb) = active_bars.get(&transfer_id) {
                            pb.inc(bytes);
                        }
                    }
                    TransferEvent::TransferComplete { transfer_id } => {
                        if let Some(pb) = active_bars.remove(&transfer_id) {
                            pb.set_style(
                                pb.style()
                                    .clone()
                                    .template("{spinner:.green} {msg:.green} [{elapsed_precise}] ✔ Completed!")
                                    .expect("Invalid style")
                            );
                            pb.finish_with_message("Done!");
                        }
                    }
                }
            }

            Ok(())
        }
    }
}

struct InteractiveConsent {
    event_tx: TransferEventSender,
}

#[async_trait]
impl TransferConsentHandler for InteractiveConsent {
    async fn request_consent(&self, peer: SocketAddr, job_name: &str) -> bool {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();

        let _ = self.event_tx.send(TransferEvent::ConsentRequested {
            peer,
            job_name: job_name.to_string(),
            reply_tx: Arc::new(Mutex::new(Some(reply_tx))),
        });

        reply_rx.await.unwrap_or(false)
    }
}

pub fn create_transfer_pb(total_bytes: u64, name: &str, is_sender: bool) -> ProgressBar {
    let pb = ProgressBar::new(total_bytes);

    let style = ProgressStyle::default_bar()
        .template(
            "{spinner:.green} {msg}\n{bytes:>10} / {total_bytes:10} [{bar:40.cyan/blue}] {percent}% {bytes_per_sec} | {eta}"
        )
        .unwrap()
        .progress_chars("━╾─");

    pb.set_style(style);

    if !is_sender {
        pb.set_message(format!("Receiving: {}", name));
    }
    pb
}
