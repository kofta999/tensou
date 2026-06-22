use std::{
    env,
    net::SocketAddr,
    path::{Path, PathBuf},
};
use tensou::{
    discovery::{self, DiscoveredDevice},
    net::{AppDaemon, TransferClient},
};
use tokio::{
    io::{self, AsyncBufReadExt},
    sync::mpsc,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install crypto provider");

    let args: Vec<String> = env::args().collect();
    let mode = args.get(1).map(|s| s.as_str()).unwrap_or("help");

    match mode {
        "receive" => {
            println!("Starting Tensou Receiver Mode...");
            let p = PathBuf::from("recv");
            std::fs::create_dir_all(&p).unwrap();

            let bind_addr: SocketAddr = "0.0.0.0:5000".parse().unwrap();

            AppDaemon::new(bind_addr)
                .unwrap()
                .run(p.canonicalize().unwrap())
                .await;
        }
        "send" => {
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

            let client =
                TransferClient::connect(selected_addr, Path::new("random_file.bin")).await?;
            client.process_chunks().await?;

            println!("Files sent successfully!");
        }
        _ => {
            println!("Usage:");
            println!("  cargo run -- receive");
            println!("  cargo run -- send");
        }
    }

    Ok(())
}
