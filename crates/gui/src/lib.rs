mod background;
mod callbacks;
pub mod state;
pub mod views;

use crate::state::{ConsentRegistry, GuiConsentHandler, GuiEvent, GuiTransferObserver};
pub use state::{GuiDevice, GuiTransfer};
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
};
use tensou_core::{config::Config, discovery::DiscoveryEvent};
use tensou_core::{
    discovery::{self},
    net::ReceiverDaemon,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
pub use views::run_gui;

pub fn run() -> anyhow::Result<()> {
    println!("Launching Tensou GUI...");
    let config = Config::load_or_create();
    let device_uuid = config.device_uuid.clone();

    // For detecting devices
    let (tx, devices_rx) = mpsc::channel::<DiscoveryEvent>(10);
    tokio::spawn(async move {
        let _ = discovery::scan_for_receivers(tx, &device_uuid).await;
    });

    // Create channels for GUI events
    let (event_tx, event_rx) = mpsc::unbounded_channel::<GuiEvent>();

    let consent_registry = Arc::new(ConsentRegistry {
        pending: Mutex::new(HashMap::new()),
    });

    let daemon_event_tx = event_tx.clone();
    let daemon_consent_registry = consent_registry.clone();
    let port = config.listen_port;
    let config_mutex = Arc::new(Mutex::new(config));
    let config_clone = config_mutex.clone();

    tokio::spawn(async move {
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port);

        let target_dir = {
            let config = config_clone.lock().unwrap();
            config.target_dir.clone()
        };

        if let Ok(daemon) = ReceiverDaemon::new(bind_addr, config_clone) {
            let cancel_token = CancellationToken::new();

            let observer = Arc::new(GuiTransferObserver {
                transfer_id: 0,
                tx: daemon_event_tx.clone(),
                is_sender: false,
                target_dir,
            });

            let consent_handler = Arc::new(GuiConsentHandler {
                registry: daemon_consent_registry,
                event_tx: daemon_event_tx.clone(),
            });

            daemon.run(consent_handler, observer, cancel_token).await;
        }
    });

    crate::run_gui(
        devices_rx,
        event_tx,
        event_rx,
        consent_registry,
        config_mutex,
    )?;

    Ok(())
}
