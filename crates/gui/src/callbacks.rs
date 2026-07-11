use crate::GuiTransfer;
use crate::state::ConsentRegistry;
use crate::state::GuiEvent;
use crate::state::TransferStatus;
use crate::views::AppData;
use crate::views::Logic;
use crate::views::MainWindow;
use slint::ComponentHandle;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use tensou_core::config::Config;
use tensou_core::net;
use tensou_core::protocol::{SenderInfo, TransferError};
use tensou_core::util::generate_job_name;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub fn setup(
    main_window: &MainWindow,
    event_tx: mpsc::UnboundedSender<GuiEvent>,
    consent_registry: Arc<ConsentRegistry>,
    config: Arc<Mutex<Config>>,
    local_transfers: Arc<Mutex<Vec<GuiTransfer>>>,
    local_completed_transfers: Arc<Mutex<Vec<GuiTransfer>>>,
    reload_tx: mpsc::Sender<()>,
) {
    let direct_ip = Arc::new(std::sync::Mutex::new(String::new()));

    // Direct Connect changes
    main_window.global::<Logic>().on_direct_ip_changed({
        let direct_ip = direct_ip.clone();
        move |text| {
            *direct_ip.lock().unwrap() = text.to_string();
        }
    });

    // Direct Send File
    main_window.global::<Logic>().on_direct_send_file({
        let event_tx = event_tx.clone();
        let config = config.clone();
        move |ip_str| {
            if let Ok(target_addr) = ip_str.parse::<SocketAddr>() {
                if let Some(path) = rfd::FileDialog::new()
                    .set_title("Select File to Send")
                    .pick_files()
                {
                    let sender_info = SenderInfo::from(&*config.lock().unwrap());
                    send_file_background(event_tx.clone(), target_addr, sender_info, path, None);
                }
            } else {
                log::warn!("Invalid target IP address");
            }
        }
    });

    // Direct Send Folder
    main_window.global::<Logic>().on_direct_send_folder({
        let event_tx = event_tx.clone();
        let config = config.clone();
        move |ip_str| {
            if let Ok(target_addr) = ip_str.parse::<SocketAddr>() {
                if let Some(path) = rfd::FileDialog::new()
                    .set_title("Select Folder to Send")
                    .pick_folders()
                {
                    let sender_info = SenderInfo::from(&*config.lock().unwrap());
                    send_file_background(event_tx.clone(), target_addr, sender_info, path, None);
                }
            } else {
                log::warn!("Invalid target IP address");
            }
        }
    });

    // Device Send File
    main_window.global::<Logic>().on_device_send_file({
        let event_tx = event_tx.clone();
        let config = config.clone();
        move |dev| {
            let target_addr = SocketAddr::new(dev.ip.parse().unwrap(), dev.port as u16);
            if let Some(path) = rfd::FileDialog::new()
                .set_title("Select File to Send")
                .pick_files()
            {
                let sender_info = SenderInfo::from(&*config.lock().unwrap());
                send_file_background(event_tx.clone(), target_addr, sender_info, path, None);
            }
        }
    });

    // Device Send Folder
    main_window.global::<Logic>().on_device_send_folder({
        let event_tx = event_tx.clone();
        let config = config.clone();
        move |dev| {
            let target_addr = SocketAddr::new(dev.ip.parse().unwrap(), dev.port as u16);
            if let Some(path) = rfd::FileDialog::new()
                .set_title("Select Folder to Send")
                .pick_folders()
            {
                let sender_info = SenderInfo::from(&*config.lock().unwrap());
                send_file_background(event_tx.clone(), target_addr, sender_info, path, None);
            }
        }
    });

    // Change Download Directory
    main_window.global::<Logic>().on_change_download_dir({
        let main_window_weak = main_window.as_weak();
        let config = config.clone();
        move || {
            if let Some(path) = rfd::FileDialog::new()
                .set_title("Select Download Directory")
                .pick_folder()
            {
                let mut cfg = config.lock().unwrap();
                cfg.target_dir = path.clone();
                let _ = cfg.save();
                if let Some(ui) = main_window_weak.upgrade() {
                    ui.global::<AppData>()
                        .set_download_dir(path.to_string_lossy().to_string().into());
                }
            }
        }
    });

    // Update Display Name
    main_window.global::<Logic>().on_update_display_name({
        let config = config.clone();
        let reload_tx = reload_tx.clone();
        move |name| {
            let mut cfg = config.lock().unwrap();
            cfg.display_name = name.to_string();
            let _ = cfg.save();
            let _ = reload_tx.try_send(());
        }
    });

    // Toggle Overwrite
    main_window.global::<Logic>().on_toggle_overwrite_dest({
        let config = config.clone();
        move |val| {
            let mut cfg = config.lock().unwrap();
            cfg.overwrite_dest = val;
            let _ = cfg.save();
        }
    });

    // Toggle Auto Accept
    main_window.global::<Logic>().on_toggle_auto_accept({
        let config = config.clone();
        move |val| {
            let mut cfg = config.lock().unwrap();
            cfg.auto_accept = val;
            let _ = cfg.save();
        }
    });

    // Consent Response
    main_window.global::<Logic>().on_consent_response({
        let consent_registry = consent_registry.clone();
        let main_window_weak = main_window.as_weak();
        move |transfer_id, accepted| {
            let transfer_id = Uuid::parse_str(&transfer_id).expect("Transfer ID must be a UUID");
            if accepted {
                consent_registry.accept(transfer_id);
            } else {
                consent_registry.reject(transfer_id);
            }
            if let Some(ui) = main_window_weak.upgrade() {
                ui.global::<AppData>().set_has_consent_request(false);
            }
        }
    });

    // Cancel Transfer
    let event_clone = event_tx.clone();
    let config_cancel_clone = config.clone();
    main_window.global::<Logic>().on_cancel_transfer({
        let local_transfers = local_transfers.clone();
        move |transfer_id| {
            log::info!("Cancel clicked for transfer: {}", transfer_id);
            let transfers = local_transfers.lock().unwrap();
            if let Some(transfer) = transfers.iter().find(|t| transfer_id == t.id) {
                let is_paused = transfer.status == TransferStatus::Paused;
                transfer.cancel_token.cancel();

                let _ = event_clone.send(GuiEvent::TransferFailed {
                    transfer_id: transfer_id.to_string(),
                    error: TransferError::Cancelled,
                });

                if is_paused {
                    let target_addr = transfer.peer_addr;
                    let sender_info = SenderInfo::from(&*config_cancel_clone.lock().unwrap());
                    let transfer_uuid =
                        Uuid::parse_str(&transfer_id).expect("Must be a valid UUID");
                    let paths = transfer.original_paths.clone();
                    tokio::spawn(async move {
                        log::info!("Connecting to receiver to send cancel signal...");
                        if let Ok(Some(client)) = net::Sender::connect(
                            target_addr,
                            net::SendType::Files(&paths),
                            sender_info,
                            CancellationToken::new(),
                            transfer_uuid,
                        )
                        .await
                        {
                            client.close_with_error(&TransferError::Cancelled);
                        }
                    });
                }
            }
        }
    });

    // Pause Transfer
    let event_pause_tx = event_tx.clone();
    main_window.global::<Logic>().on_pause_transfer({
        let local_transfers = local_transfers.clone();
        move |transfer_id| {
            log::info!("Pause clicked for transfer: {}", transfer_id,);
            let mut transfers = local_transfers.lock().unwrap();
            if let Some(transfer) = transfers.iter_mut().find(|t| transfer_id == t.id) {
                transfer.status = TransferStatus::Paused;
                transfer
                    .is_paused
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                transfer.cancel_token.cancel();

                let _ = event_pause_tx.send(GuiEvent::TransferPaused {
                    transfer_id: transfer_id.to_string(),
                });
            }
        }
    });

    // Resume Transfer
    let event_resume_clone = event_tx.clone();
    main_window.global::<Logic>().on_resume_transfer({
        let local_transfers = local_transfers.clone();
        let config = config.clone();
        move |transfer_id| {
            log::info!("Resume clicked for transfer: {}", transfer_id,);
            let mut transfers = local_transfers.lock().unwrap();
            if let Some(transfer) = transfers.iter_mut().find(|t| transfer_id == t.id) {
                transfer.status = TransferStatus::Resuming;
                transfer
                    .is_paused
                    .store(false, std::sync::atomic::Ordering::Relaxed);

                let target_addr = transfer.peer_addr;
                let paths = transfer.original_paths.clone();
                let sender_info = SenderInfo::from(&*config.lock().unwrap());

                let _ = event_resume_clone.send(GuiEvent::TransferResuming {
                    transfer_id: transfer_id.to_string(),
                });

                send_file_background(
                    event_resume_clone.clone(),
                    target_addr,
                    sender_info,
                    paths,
                    Some(Uuid::parse_str(&transfer_id).expect("Must be a valid UUID")),
                );
            }
        }
    });

    // Open Transfer Folder
    main_window
        .global::<Logic>()
        .on_open_transfer_folder(move |transfer_id| {
            let completed = local_completed_transfers.lock().unwrap();
            if let Some(t) = completed.iter().find(|x| transfer_id == x.id) {
                log::info!(
                    "Opening folder for completed transfer: {}",
                    t.local_dir.display()
                );
                let _ = open::that(&t.local_dir);
            }
        });

    /*
    // Device Send Dropped File
    main_window.global::<Logic>().on_device_send_dropped_file({
        let event_tx = event_tx.clone();
        move |dev, data_transfer| {
            if let Ok(text) = data_transfer.plain_text() {
                let mut paths = Vec::new();
                for line in text.lines() {
                    let line = line.trim();
                    if let Some(path_str) = line.strip_prefix("file://") {
                        if let Ok(decoded) = urlencoding::decode(path_str) {
                            paths.push(PathBuf::from(decoded.into_owned()));
                        }
                    } else if !line.is_empty() {
                        paths.push(PathBuf::from(line));
                    }
                }
                if !paths.is_empty() {
                    let target_addr = SocketAddr::new(dev.ip.parse().unwrap(), dev.port as u16);
                    send_file_background(event_tx.clone(), target_addr, paths);
                }
            }
        }
    });

    // Direct Send Dropped File
    main_window.global::<Logic>().on_direct_send_dropped_file({
        let event_tx = event_tx.clone();
        move |ip_str, data_transfer| {
            if let Ok(text) = data_transfer.plain_text() {
                let mut paths = Vec::new();
                for line in text.lines() {
                    let line = line.trim();
                    if let Some(path_str) = line.strip_prefix("file://") {
                        if let Ok(decoded) = urlencoding::decode(path_str) {
                            paths.push(PathBuf::from(decoded.into_owned()));
                        }
                    } else if !line.is_empty() {
                        paths.push(PathBuf::from(line));
                    }
                }
                if !paths.is_empty() {
                    let ip_str = ip_str.to_string();
                    let target_addr: Result<SocketAddr, _> = if ip_str.contains(':') {
                        ip_str.parse()
                    } else {
                        format!("{}:{}", ip_str, SERVER_PORT).parse()
                    };

                    if let Ok(target_addr) = target_addr {
                        send_file_background(event_tx.clone(), target_addr, paths);
                    }
                }
            }
        }
    });
    */

    // Send Text to Device
    main_window.global::<Logic>().on_send_text_to_device({
        let config = config.clone();
        move |dev, text| {
            if let Ok(target_addr) = format!("{}:{}", dev.ip, dev.port).parse::<SocketAddr>() {
                let text_content = text.to_string();
                let sender_info = SenderInfo::from(&*config.lock().unwrap());

                tokio::spawn(async move {
                    let send_type = net::SendType::Text(text_content);
                    if let Err(e) = net::Sender::connect(
                        target_addr,
                        send_type,
                        sender_info,
                        CancellationToken::new(),
                        Uuid::new_v4(),
                    )
                    .await
                    {
                        log::error!("Failed to send text to device: {e}");
                    }
                });
            }
        }
    });

    // Send Text Direct
    main_window.global::<Logic>().on_send_text_direct({
        let config = config.clone();
        move |ip_str, text| {
            let ip_str = ip_str.to_string();
            let target_addr: Result<SocketAddr, _> = if ip_str.contains(':') {
                ip_str.parse()
            } else {
                format!("{}:{}", ip_str, tensou_core::SERVER_PORT).parse()
            };

            if let Ok(target_addr) = target_addr {
                let text_content = text.to_string();
                let sender_info = SenderInfo::from(&*config.lock().unwrap());
                tokio::spawn(async move {
                    let send_type = net::SendType::Text(text_content);
                    if let Err(e) = net::Sender::connect(
                        target_addr,
                        send_type,
                        sender_info,
                        CancellationToken::new(),
                        Uuid::new_v4(),
                    )
                    .await
                    {
                        log::error!("Failed to send text direct: {e}");
                    }
                });
            }
        }
    });

    // Copy to Clipboard
    main_window
        .global::<Logic>()
        .on_copy_to_clipboard(move |text| {
            if let Ok(mut ctx) = arboard::Clipboard::new() {
                let _ = ctx.set_text(text.to_string());
            }
        });

    // Paste from Clipboard
    main_window
        .global::<Logic>()
        .on_paste_from_clipboard(move || {
            if let Ok(mut ctx) = arboard::Clipboard::new() {
                ctx.get_text().unwrap_or_default().into()
            } else {
                "".into()
            }
        });

    // Clear Clipboard History
    main_window.global::<Logic>().on_clear_clipboard_history({
        let main_window_weak = main_window.as_weak();
        move || {
            if let Some(ui) = main_window_weak.upgrade() {
                let empty_model = std::rc::Rc::new(slint::VecModel::default());
                ui.global::<AppData>()
                    .set_clipboard_history(empty_model.into());
            }
        }
    });
}

fn send_file_background(
    event_tx: mpsc::UnboundedSender<GuiEvent>,
    target_addr: SocketAddr,
    sender_info: SenderInfo,
    paths: Vec<PathBuf>,
    preset_id: Option<Uuid>,
) {
    let transfer_id = preset_id.unwrap_or_else(|| Uuid::new_v4());
    let tx_clone = event_tx.clone();

    tokio::spawn(async move {
        let job_name = generate_job_name(&paths);
        let is_paused = Arc::new(AtomicBool::new(false));

        match net::Sender::connect(
            target_addr,
            net::SendType::Files(&paths),
            sender_info,
            CancellationToken::new(),
            transfer_id,
        )
        .await
        {
            Ok(Some(mut client)) => {
                // Determine a safe base parent directory to store completed reference
                let local_dir = paths[0]
                    .parent()
                    .map(|p| p.to_path_buf())
                    .unwrap_or_else(|| paths[0].clone());
                let total_bytes = client.get_total_bytes();
                let bytes_done = client.get_bytes_done();

                let _ = tx_clone.send(GuiEvent::TransferStarted {
                    transfer_id: transfer_id.to_string(),
                    is_sender: true,
                    is_paused: is_paused.clone(),
                    job_name,
                    total_bytes,
                    bytes_done,
                    cancel_token: client.cancel_token.clone(),
                    local_dir: local_dir.clone(),
                    peer_ip: target_addr.ip().to_string(),
                    original_paths: paths.clone(),
                    peer_addr: target_addr,
                });

                let observer = std::sync::Arc::new(crate::state::GuiTransferObserver {
                    tx: tx_clone.clone(),
                    is_sender: true,
                    target_dir: local_dir,
                });

                match client.process_chunks(observer, is_paused).await {
                    Ok(()) => {
                        let _ = tx_clone.send(GuiEvent::TransferFinished {
                            transfer_id: transfer_id.to_string(),
                        });
                    }
                    Err(_) if client.cancel_token.is_cancelled() => {
                        // Intentional pause or cancel — send.rs already closed
                        // the connection with the appropriate reason ("Paused" or
                        // "Cancelled"). The GUI state machine already set the
                        // correct status before triggering the token, so we
                        // don't fire a redundant TransferFailed event here.
                    }
                    Err(e) => {
                        let error = if net::Sender::is_connection_error(&e) {
                            TransferError::ConnectionLoss
                        } else {
                            TransferError::Other(e.to_string())
                        };
                        let _ = tx_clone.send(GuiEvent::TransferFailed {
                            transfer_id: transfer_id.to_string(),
                            error,
                        });
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                let error = if net::Sender::is_connection_error(&e) {
                    TransferError::ConnectionLoss
                } else {
                    TransferError::Other(format!("Connection failed: {}", e))
                };
                let _ = tx_clone.send(GuiEvent::TransferFailed {
                    transfer_id: transfer_id.to_string(),
                    error,
                });
            }
        }
    });
}
