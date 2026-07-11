use async_trait::async_trait;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tensou_core::discovery::DiscoveredDevice;
use tensou_core::protocol::{SenderInfo, TransferConsentHandler, TransferObserver};
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

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

impl From<GuiScreen> for usize {
    fn from(val: GuiScreen) -> Self {
        val as usize
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferStatus {
    Active,
    Paused,
    Resuming,
    Reconnecting { attempt: u32 },
    Completed,
    Failed,
    Cancelled,
}

impl std::fmt::Display for TransferStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => write!(f, "Active"),
            Self::Paused => write!(f, "Paused"),
            Self::Resuming => write!(f, "Resuming..."),
            Self::Reconnecting { attempt } => write!(f, "Reconnecting ({})", attempt),
            Self::Completed => write!(f, "Completed"),
            Self::Failed => write!(f, "Failed"),
            Self::Cancelled => write!(f, "Cancelled"),
        }
    }
}

pub struct GuiTransfer {
    pub id: String,
    pub is_sender: bool,
    pub job_name: String,
    pub total_bytes: u64,
    pub bytes_transferred: u64,
    pub bytes_done_at_start: u64,
    pub start_time: Instant,
    pub cancel_token: CancellationToken,
    pub local_dir: std::path::PathBuf,
    pub status: TransferStatus,
    pub timestamp: String,
    pub peer_name: String,
    pub original_paths: Vec<std::path::PathBuf>,
    pub peer_addr: SocketAddr,
}

pub enum GuiEvent {
    TransferStarted {
        transfer_id: String,
        is_sender: bool,
        job_name: String,
        total_bytes: u64,
        bytes_done: u64,
        cancel_token: CancellationToken,
        local_dir: std::path::PathBuf,
        peer_ip: String,
        original_paths: Vec<std::path::PathBuf>,
        peer_addr: SocketAddr,
    },
    ChunkTransferred {
        transfer_id: String,
        bytes: u64,
    },
    TransferFinished {
        transfer_id: String,
    },
    TransferFailed {
        transfer_id: String,
        error: String,
    },
    IncomingConsentRequest {
        transfer_id: String,
        peer: SocketAddr,
        sender: SenderInfo,
        job_name: String,
    },
    TextReceived {
        job_name: String,
        content: String,
        peer_ip: String,
    },
    TransferReconnecting {
        transfer_id: String,
        attempt: u32,
    },
    TransferReconnected {
        transfer_id: String,
    },
}

pub struct GuiTransferObserver {
    pub tx: UnboundedSender<GuiEvent>,
    pub is_sender: bool,
    pub target_dir: std::path::PathBuf,
}

impl TransferObserver for GuiTransferObserver {
    fn on_transfer_started(
        &self,
        transfer_id: Uuid,
        peer: SocketAddr,
        total_bytes: u64,
        bytes_done: u64,
        job_name: &str,
        cancel_token: CancellationToken,
    ) {
        let _ = self.tx.send(GuiEvent::TransferStarted {
            transfer_id: transfer_id.to_string(),
            is_sender: self.is_sender,
            job_name: job_name.to_string(),
            total_bytes,
            bytes_done,
            cancel_token,
            local_dir: self.target_dir.clone(),
            peer_ip: peer.ip().to_string(),
            original_paths: Vec::new(),
            peer_addr: peer,
        });
    }

    fn on_chunk_transferred(&self, transfer_id: Uuid, bytes: u64) {
        let _ = self.tx.send(GuiEvent::ChunkTransferred {
            transfer_id: transfer_id.to_string(),
            bytes,
        });
    }

    fn on_transfer_complete(&self, transfer_id: Uuid) {
        let _ = self.tx.send(GuiEvent::TransferFinished {
            transfer_id: transfer_id.to_string(),
        });
    }

    fn on_transfer_failed(&self, transfer_id: Uuid, error: &str) {
        let _ = self.tx.send(GuiEvent::TransferFailed {
            transfer_id: transfer_id.to_string(),
            error: error.to_string(),
        });
    }

    fn on_reconnecting(&self, transfer_uuid: Uuid, attempt: u32) {
        let _ = self.tx.send(GuiEvent::TransferReconnecting {
            transfer_id: transfer_uuid.to_string(),
            attempt,
        });
    }

    fn on_reconnected(&self, transfer_uuid: Uuid) {
        let _ = self.tx.send(GuiEvent::TransferReconnected {
            transfer_id: transfer_uuid.to_string(),
        });
    }

    fn on_text_received(&self, peer: SocketAddr, job_name: String, content: String) {
        let _ = self.tx.send(GuiEvent::TextReceived {
            job_name,
            content,
            peer_ip: peer.ip().to_string(),
        });
    }
}

pub struct ConsentRegistry {
    pub pending: Mutex<HashMap<Uuid, oneshot::Sender<bool>>>,
}

impl ConsentRegistry {
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
        }
    }

    pub fn accept(&self, transfer_id: uuid::Uuid) {
        log::debug!(
            "ConsentRegistry::accept: trying to accept transfer_id={}",
            transfer_id
        );
        let mut pending = self.pending.lock().unwrap();
        log::debug!(
            "ConsentRegistry::accept: pending keys: {:?}",
            pending.keys().collect::<Vec<_>>()
        );
        if let Some(tx) = pending.remove(&transfer_id) {
            log::debug!(
                "ConsentRegistry::accept: found sender for transfer_id={}",
                transfer_id
            );
            let _ = tx.send(true);
        } else {
            log::warn!(
                "ConsentRegistry::accept: SENDER NOT FOUND for transfer_id={}",
                transfer_id
            );
        }
    }

    pub fn reject(&self, transfer_id: uuid::Uuid) {
        log::debug!(
            "ConsentRegistry::reject: trying to reject transfer_id={}",
            transfer_id
        );
        let mut pending = self.pending.lock().unwrap();
        if let Some(tx) = pending.remove(&transfer_id) {
            let _ = tx.send(false);
        }
    }
}

impl Default for ConsentRegistry {
    fn default() -> Self {
        Self::new()
    }
}

pub struct GuiConsentHandler {
    pub registry: Arc<ConsentRegistry>,
    pub event_tx: mpsc::UnboundedSender<GuiEvent>,
}

#[async_trait]
impl TransferConsentHandler for GuiConsentHandler {
    async fn request_consent(&self, peer: SocketAddr, sender: &SenderInfo, job_name: &str) -> bool {
        let transfer_id = uuid::Uuid::new_v4();
        let (tx, rx) = oneshot::channel();

        log::debug!(
            "GuiConsentHandler::request_consent: generated transfer_id={}",
            transfer_id
        );

        self.registry
            .pending
            .lock()
            .unwrap()
            .insert(transfer_id, tx);

        let _ = self.event_tx.send(GuiEvent::IncomingConsentRequest {
            transfer_id: transfer_id.to_string(),
            peer,
            sender: sender.clone(),
            job_name: job_name.to_string(),
        });

        rx.await.unwrap_or(false)
    }
}
