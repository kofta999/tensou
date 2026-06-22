use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};
use tensou::net::{AppDaemon, TransferClient};

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

    let client = TransferClient::connect(server_addr, Path::new("random_file.bin")).await?;

    client.process_chunks().await?;

    println!("Files sent successfully!");

    server.await?;

    Ok(())
}
