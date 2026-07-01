use crate::config::Config;
use crate::gui::GuiTransfer;
use crate::gui::state::ConsentRegistry;
use crate::gui::state::GuiEvent;
use crate::gui::views::AppData;
use crate::gui::views::Logic;
use crate::gui::views::MainWindow;
use crate::net;
use slint::ComponentHandle;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub fn setup(
    main_window: &MainWindow,
    event_tx: mpsc::UnboundedSender<GuiEvent>,
    consent_registry: Arc<ConsentRegistry>,
    config: Arc<Mutex<Config>>,
    local_transfers: Arc<Mutex<Vec<GuiTransfer>>>,
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
        move |ip_str| {
            if let Ok(target_addr) = ip_str.parse::<SocketAddr>() {
                if let Some(path) = rfd::FileDialog::new()
                    .set_title("Select File to Send")
                    .pick_file()
                {
                    send_file_background(event_tx.clone(), target_addr, path);
                }
            } else {
                println!("Invalid target IP address");
            }
        }
    });

    // Direct Send Folder
    main_window.global::<Logic>().on_direct_send_folder({
        let event_tx = event_tx.clone();
        move |ip_str| {
            if let Ok(target_addr) = ip_str.parse::<SocketAddr>() {
                if let Some(path) = rfd::FileDialog::new()
                    .set_title("Select Folder to Send")
                    .pick_folder()
                {
                    send_file_background(event_tx.clone(), target_addr, path);
                }
            } else {
                println!("Invalid target IP address");
            }
        }
    });

    // Device Send File
    main_window.global::<Logic>().on_device_send_file({
        let event_tx = event_tx.clone();
        move |dev| {
            let target_addr = SocketAddr::new(dev.ip.parse().unwrap(), dev.port as u16);
            if let Some(path) = rfd::FileDialog::new()
                .set_title("Select File to Send")
                .pick_file()
            {
                send_file_background(event_tx.clone(), target_addr, path);
            }
        }
    });

    // Device Send Folder
    main_window.global::<Logic>().on_device_send_folder({
        let event_tx = event_tx.clone();
        move |dev| {
            let target_addr = SocketAddr::new(dev.ip.parse().unwrap(), dev.port as u16);
            if let Some(path) = rfd::FileDialog::new()
                .set_title("Select Folder to Send")
                .pick_folder()
            {
                send_file_background(event_tx.clone(), target_addr, path);
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
        move |name| {
            let mut cfg = config.lock().unwrap();
            cfg.display_name = name.to_string();
            let _ = cfg.save();
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

    // Consent Response
    main_window.global::<Logic>().on_consent_response({
        let consent_registry = consent_registry.clone();
        let main_window_weak = main_window.as_weak();
        move |transfer_id, accepted| {
            if accepted {
                consent_registry.accept(transfer_id as u32);
            } else {
                consent_registry.reject(transfer_id as u32);
            }
            if let Some(ui) = main_window_weak.upgrade() {
                ui.global::<AppData>().set_has_consent_request(false);
            }
        }
    });

    // Cancel Transfer
    main_window
        .global::<Logic>()
        .on_cancel_transfer(move |transfer_id| {
            println!("Cancel clicked for transfer: {}", transfer_id);
            let transfers = local_transfers.lock().unwrap();
            if let Some(transfer) = transfers.iter().find(|t| t.id == transfer_id as u32) {
                transfer.cancel_token.cancel();
            }
        });
}

fn send_file_background(
    event_tx: mpsc::UnboundedSender<GuiEvent>,
    target_addr: SocketAddr,
    path: PathBuf,
) {
    let transfer_id = rand::random::<u32>();
    let tx_clone = event_tx.clone();

    tokio::spawn(async move {
        let job_name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "Unknown".to_string());

        match net::Sender::connect(target_addr, &path, CancellationToken::new()).await {
            Ok(client) => {
                let total_bytes = client.get_remaining_bytes();
                let _ = tx_clone.send(GuiEvent::TransferStarted {
                    transfer_id,
                    is_sender: true,
                    job_name: job_name.clone(),
                    total_bytes,
                    cancel_token: client.cancel_token.clone(),
                });

                let observer = std::sync::Arc::new(crate::gui::state::GuiTransferObserver {
                    transfer_id,
                    tx: tx_clone.clone(),
                    is_sender: true,
                });

                match client.process_chunks(observer).await {
                    Ok(()) => {
                        let _ = tx_clone.send(GuiEvent::TransferFinished { transfer_id });
                    }
                    Err(e) => {
                        let _ = tx_clone.send(GuiEvent::TransferFailed {
                            transfer_id,
                            error: e.to_string(),
                        });
                    }
                }
            }
            Err(e) => {
                let _ = tx_clone.send(GuiEvent::TransferFailed {
                    transfer_id,
                    error: format!("Connection failed: {}", e),
                });
            }
        }
    });
}
