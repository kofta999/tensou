use crate::discovery::DiscoveredDevice;
use crate::protocol::{TransferConsentHandler, TransferObserver};
use async_trait::async_trait;
use elegance::{AvatarPresence, AvatarTone, BuiltInTheme};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::oneshot;

#[derive(Clone, Copy)]
pub enum GuiScreen {
    Send,
    Transfers,
    Settings,
}

impl From<usize> for GuiScreen {
    fn from(value: usize) -> Self {
        // self.state.current_tab =
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

/// Holds the application state that drives the GUI.
/// All fields can be read/updated by backend channels.
pub struct GuiState {
    pub download_dir: PathBuf,
    pub devices: Vec<GuiDevice>,
    pub active_transfers: Vec<GuiTransfer>, // Supports multiple simultaneous transfers
    pub pending_consent: Option<GuiConsent>,
    pub listen_port: u16,
    pub current_theme: BuiltInTheme,
    pub direct_ip: String,
    pub current_tab: GuiScreen,
}

pub struct GuiDevice {
    pub hostname: String,
    pub fullname: String,
    pub ip: String,
    pub port: u16,
    pub initials: String,
    pub tone: AvatarTone,
    pub presence: AvatarPresence,
}

impl From<DiscoveredDevice> for GuiDevice {
    fn from(value: DiscoveredDevice) -> Self {
        let ip = value.addr.ip();
        let port = value.addr.port();
        let initials = value
            .hostname
            .chars()
            .take(2)
            .collect::<String>()
            .to_uppercase();
        let initials = if initials.is_empty() {
            "?".to_string()
        } else {
            initials
        };
        let tone = AvatarTone::from_text(&initials);
        Self {
            hostname: value.hostname,
            fullname: value.fullname,
            initials,
            ip: ip.to_string(),
            port,
            presence: AvatarPresence::Online,
            tone,
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
}

pub struct GuiConsent {
    pub transfer_id: u32,
    pub peer_addr: SocketAddr,
    pub job_name: String,
    // pub total_bytes: u64,
}

impl Default for GuiState {
    fn default() -> Self {
        // Mock data to preview visual layout immediately before backend integration
        Self {
            download_dir: PathBuf::from("/home/kofta/Downloads/Tensou"),
            listen_port: 6967,
            current_theme: BuiltInTheme::Slate,
            direct_ip: String::new(),
            current_tab: GuiScreen::Send,
            devices: vec![],
            active_transfers: vec![],
            pending_consent: None, // pending_consent: Some(GuiConsent {
                                   //     transfer_id: 12345,
                                   //     peer_addr: "192.168.1.100:6967".parse().unwrap(),
                                   //     job_name: "tensou_build_backup".to_string(),
                                   //     total_bytes: 1_250_000_000,
                                   // }),
        }
    }
}

pub enum GuiEvent {
    /// A new transfer has started (so the GUI can add a progress bar to the list)
    TransferStarted {
        transfer_id: u32,
        is_sender: bool,
        job_name: String,
        total_bytes: u64,
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
    pub ctx: egui::Context,
    pub is_sender: bool,
}

impl TransferObserver for GuiTransferObserver {
    fn on_transfer_started(
        &self,
        transfer_id: u32,
        _peer: SocketAddr,
        total_bytes: u64,
        job_name: &str,
    ) {
        let _ = self.tx.send(GuiEvent::TransferStarted {
            transfer_id,
            is_sender: self.is_sender,
            job_name: job_name.to_string(),
            total_bytes,
        });
        self.ctx.request_repaint();
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
        self.ctx.request_repaint();
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
        if let Some(tx) = self.pending.lock().unwrap().remove(&transfer_id) {
            let _ = tx.send(true);
        }
    }

    pub fn reject(&self, transfer_id: u32) {
        if let Some(tx) = self.pending.lock().unwrap().remove(&transfer_id) {
            let _ = tx.send(false);
        }
    }
}

pub struct GuiConsentHandler {
    pub registry: Arc<ConsentRegistry>,
    pub event_tx: mpsc::UnboundedSender<GuiEvent>,
    pub ctx: egui::Context,
}

#[async_trait]
impl TransferConsentHandler for GuiConsentHandler {
    async fn request_consent(&self, peer: SocketAddr, job_name: &str) -> bool {
        let transfer_id = rand::random::<u32>();
        let (tx, rx) = oneshot::channel();

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

        self.ctx.request_repaint();

        rx.await.unwrap_or(false)
    }
}
