use crate::{create_transfer_pb, resolve_save_directory};
use async_trait::async_trait;
use indicatif::{MultiProgress, ProgressBar};
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tensou_core::{
    config::Config,
    net::ReceiverDaemon,
    protocol::{TransferConsentHandler, TransferObserver},
};
use tokio_util::sync::CancellationToken;

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
        _cancel_token: CancellationToken,
    ) {
        let pb = self
            .multi_progress
            .add(create_transfer_pb(total_bytes, job_name, false));
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

    fn on_transfer_failed(&self, transfer_id: u32, error: &str) {
        if let Some(pb) = self.active.lock().unwrap().remove(&transfer_id) {
            pb.finish_with_message(format!("Failed: {}", error));
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

pub async fn run(port: u16, output: Option<PathBuf>) -> anyhow::Result<()> {
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

    let mut config = Config::load_or_create();
    config.target_dir = target_dir.clone();

    let daemon = ReceiverDaemon::new(bind_addr, Arc::new(Mutex::new(config)))?;

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
