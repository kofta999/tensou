use crate::create_transfer_pb;
use indicatif::{ProgressBar, ProgressStyle};
use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::Arc,
};
use tensou_core::{config::Config, net::SendType};
use tensou_core::{
    discovery::{self, DiscoveryEvent},
    net::Sender,
    protocol::TransferObserver,
};
use tokio::{
    io::{self, AsyncBufReadExt},
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;

struct CliSendTransfer(ProgressBar);

impl TransferObserver for CliSendTransfer {
    fn on_chunk_transferred(&self, _: Option<u32>, bytes: u64) {
        self.0.inc(bytes);
    }
}

pub async fn run(path: PathBuf, ip: Option<IpAddr>, port: u16) -> anyhow::Result<()> {
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

            let (tx, mut rx) = mpsc::channel::<DiscoveryEvent>(10);
            let config = Config::load_or_create();

            tokio::spawn(async move {
                let _ = discovery::scan_for_receivers(tx, &config.device_uuid).await;
            });

            let mut devices = Vec::new();
            let mut stdin = io::BufReader::new(io::stdin()).lines();

            let selected_addr = loop {
                tokio::select! {
                    Some(event) = rx.recv() => {
                        if let DiscoveryEvent::DeviceFound(device) = event {
                            if devices.is_empty() {
                                spinner.finish_and_clear();
                                println!("Found receivers (type a number to connect):\n");
                            }
                            devices.push(device.addr);
                            println!("  [{}] {} ({})", devices.len(), device.display_name, device.addr);
                        }
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

    let cancel_token = CancellationToken::new();
    let cancel_clone = cancel_token.clone();

    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for Ctrl+C");
        cancel_clone.cancel();
    });

    let client = Sender::connect(selected_addr, SendType::Single(&path), cancel_token)
        .await?
        .unwrap();
    let total_bytes = client.get_remaining_bytes();

    let pb = create_transfer_pb(total_bytes, &display_name, true);

    let transfer_handle =
        tokio::spawn(async move { client.process_chunks(Arc::new(CliSendTransfer(pb))).await });

    transfer_handle.await??;

    println!("\nTransfer complete!");

    Ok(())
}
