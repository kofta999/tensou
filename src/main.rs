use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tensou::{
    disk::SendSession,
    net::{AppDaemon, TransferClient},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install crypto provider");

    let server = tokio::spawn(async move {
        let p = PathBuf::from("recv");
        std::fs::create_dir_all(&p).unwrap();
        AppDaemon::new(5000)
            .unwrap()
            .run(p.canonicalize().unwrap())
            .await;
    });

    let server_addr: SocketAddr = "127.0.0.1:5000".parse()?;
    let file_path = "random_file.bin";

    // 1. Prep the disk
    let send_session = Arc::new(SendSession::new(
        &PathBuf::from(file_path),
        4 * 1024 * 1024,
    )?);

    // 2. Connect and Handshake
    let client = TransferClient::connect(server_addr, send_session).await?;

    // 3. Blast the data
    client.process_chunks().await?;

    println!("File sent successfully!");

    server.await?;

    Ok(())
}
