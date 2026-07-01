use crate::discovery::DiscoveredDevice;
use crate::protocol::{TransferConsentHandler, TransferObserver};
use async_trait::async_trait;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum GuiScreen {
    Send,
    Transfers,
    Settings,
}

impl From<usize> for GuiScreen {
    fn from(value: usize) -> Self {
        match value {
            0 => GuiScreen::Send,
            1 => GuiScreen::Transfers,
            2 => GuiScreen::Settings,
            _ => GuiScreen::Send,
        }
    }
}

impl Into<usize> for GuiScreen {
    fn into(self) -> usize {
        self as usize
    }
}

#[derive(Clone, Debug)]
pub struct GuiDevice {
    pub display_name: String,
    pub device_uuid: String,
    pub os_type: String,
    pub ip: String,
    pub port: u16,
    pub initials: String,
}

impl From<DiscoveredDevice> for GuiDevice {
    fn from(value: DiscoveredDevice) -> Self {
        let ip = value.addr.ip();
        let port = value.addr.port();
        let initials = value
            .display_name
            .chars()
            .take(2)
            .collect::<String>()
            .to_uppercase();
        let initials = if initials.is_empty() {
            "?".to_string()
        } else {
            initials
        };
        Self {
            display_name: value.display_name,
            device_uuid: value.device_uuid,
            os_type: value.os_type,
            initials,
            ip: ip.to_string(),
            port,
        }
    }
}

pub struct GuiTransfer {
    pub id: u32,
    pub is_sender: bool,
    pub job_name: String,
    pub total_bytes: u64,
    pub bytes_transferred: u64,
    pub start_time: Instant,
    pub cancel_token: CancellationToken,
}

pub enum GuiEvent {
    /// A new transfer has started
    TransferStarted {
        transfer_id: u32,
        is_sender: bool,
        job_name: String,
        total_bytes: u64,
        cancel_token: CancellationToken,
    },
    /// A chunk of bytes was successfully transferred
    ChunkTransferred { transfer_id: u32, bytes: u64 },
    /// The transfer has finished successfully
    TransferFinished { transfer_id: u32 },
    /// The transfer failed or was cancelled
    TransferFailed { transfer_id: u32, error: String },
    IncomingConsentRequest {
        transfer_id: u32,
        peer: SocketAddr,
        job_name: String,
    },
}

pub struct GuiTransferObserver {
    pub transfer_id: u32,
    pub tx: UnboundedSender<GuiEvent>,
    pub is_sender: bool,
}

impl TransferObserver for GuiTransferObserver {
    fn on_transfer_started(
        &self,
        transfer_id: u32,
        _peer: SocketAddr,
        total_bytes: u64,
        job_name: &str,
        cancel_token: CancellationToken,
    ) {
        let _ = self.tx.send(GuiEvent::TransferStarted {
            transfer_id,
            is_sender: self.is_sender,
            job_name: job_name.to_string(),
            total_bytes,
            cancel_token,
        });
    }

    fn on_chunk_transferred(&self, transfer_id: Option<u32>, bytes: u64) {
        let tid = transfer_id.unwrap_or(self.transfer_id);
        let _ = self.tx.send(GuiEvent::ChunkTransferred {
            transfer_id: tid,
            bytes,
        });
    }

    fn on_transfer_complete(&self, transfer_id: u32) {
        let _ = self.tx.send(GuiEvent::TransferFinished { transfer_id });
    }

    fn on_transfer_failed(&self, transfer_id: u32, error: &str) {
        let _ = self.tx.send(GuiEvent::TransferFailed {
            transfer_id,
            error: error.to_string(),
        });
    }
}

pub struct ConsentRegistry {
    pub pending: Mutex<HashMap<u32, oneshot::Sender<bool>>>,
}

impl ConsentRegistry {
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
        }
    }

    pub fn accept(&self, transfer_id: u32) {
        println!(
            "[DEBUG] ConsentRegistry::accept: trying to accept transfer_id={}",
            transfer_id
        );
        let mut pending = self.pending.lock().unwrap();
        println!(
            "[DEBUG] ConsentRegistry::accept: pending keys: {:?}",
            pending.keys().collect::<Vec<_>>()
        );
        if let Some(tx) = pending.remove(&transfer_id) {
            println!(
                "[DEBUG] ConsentRegistry::accept: found sender for transfer_id={}",
                transfer_id
            );
            let _ = tx.send(true);
        } else {
            println!(
                "[DEBUG] ConsentRegistry::accept: SENDER NOT FOUND for transfer_id={}",
                transfer_id
            );
        }
    }

    pub fn reject(&self, transfer_id: u32) {
        println!(
            "[DEBUG] ConsentRegistry::reject: trying to reject transfer_id={}",
            transfer_id
        );
        let mut pending = self.pending.lock().unwrap();
        if let Some(tx) = pending.remove(&transfer_id) {
            let _ = tx.send(false);
        }
    }
}

pub struct GuiConsentHandler {
    pub registry: Arc<ConsentRegistry>,
    pub event_tx: mpsc::UnboundedSender<GuiEvent>,
}

#[async_trait]
impl TransferConsentHandler for GuiConsentHandler {
    async fn request_consent(&self, peer: SocketAddr, job_name: &str) -> bool {
        let transfer_id = rand::random::<u32>();
        let (tx, rx) = oneshot::channel();

        println!(
            "[DEBUG] GuiConsentHandler::request_consent: generated transfer_id={}",
            transfer_id
        );

        self.registry
            .pending
            .lock()
            .unwrap()
            .insert(transfer_id, tx);

        let _ = self.event_tx.send(GuiEvent::IncomingConsentRequest {
            transfer_id,
            peer,
            job_name: job_name.to_string(),
        });

        rx.await.unwrap_or(false)
    }
}
