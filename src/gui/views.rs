use crate::discovery::DiscoveryEvent;
use crate::gui::state::{ConsentRegistry, GuiDevice, GuiEvent, GuiTransfer};
use crate::net;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use slint::Model;

slint::include_modules!();

pub fn run_gui(
    mut devices_rx: mpsc::Receiver<DiscoveryEvent>,
    event_tx: mpsc::UnboundedSender<GuiEvent>,
    mut event_rx: mpsc::UnboundedReceiver<GuiEvent>,
    consent_registry: Arc<ConsentRegistry>,
) -> anyhow::Result<()> {
    let selector = slint::BackendSelector::new()
        .backend_name("winit".into())
        .renderer_name("software".into());
    if let Err(err) = selector.select() {
        eprintln!("Failed to select backend: {:?}", err);
    }

    let main_window = MainWindow::new()?;

    // Track state locally in the main UI thread
    let download_dir = Arc::new(std::sync::Mutex::new(PathBuf::from(
        "/home/kofta/Downloads/Tensou",
    )));
    let direct_ip = Arc::new(std::sync::Mutex::new(String::new()));

    // Set initial settings on the window
    main_window.global::<AppData>().set_download_dir(
        download_dir
            .lock()
            .unwrap()
            .to_string_lossy()
            .to_string()
            .into(),
    );
    main_window.global::<AppData>().set_listen_port(6967);

    // Create a mutable model and attach it to the UI immediately
    let initial_transfers_model = std::rc::Rc::new(slint::VecModel::<Transfer>::default());
    main_window.global::<AppData>().set_active_transfers(initial_transfers_model.clone().into());

    // Setup callbacks
    // 1. Direct Connect changes
    main_window.global::<Logic>().on_direct_ip_changed({
        let direct_ip = direct_ip.clone();
        move |text| {
            *direct_ip.lock().unwrap() = text.to_string();
        }
    });

    // 2. Direct Send File
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

    // 3. Direct Send Folder
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

    // 4. Device Send File
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

    // 5. Device Send Folder
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

    // 6. Change Download Directory
    main_window.global::<Logic>().on_change_download_dir({
        let main_window_weak = main_window.as_weak();
        let download_dir = download_dir.clone();
        move || {
            if let Some(path) = rfd::FileDialog::new()
                .set_title("Select Download Directory")
                .pick_folder()
            {
                *download_dir.lock().unwrap() = path.clone();
                if let Some(ui) = main_window_weak.upgrade() {
                    ui.global::<AppData>().set_download_dir(path.to_string_lossy().to_string().into());
                }
            }
        }
    });

    // 7. Consent Response
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

    // Background task to process mDNS discovery and GUI events
    let main_window_weak = main_window.as_weak();

    let local_devices = Arc::new(std::sync::Mutex::new(Vec::<GuiDevice>::new()));
    let local_transfers = Arc::new(std::sync::Mutex::new(Vec::<GuiTransfer>::new()));

    // Spawn discovery events loop
    let local_devices_clone = local_devices.clone();
    let main_window_weak_devices = main_window_weak.clone();
    tokio::spawn(async move {
        while let Some(event) = devices_rx.recv().await {
            let mut devices = local_devices_clone.lock().unwrap();
            match event {
                DiscoveryEvent::DeviceFound(discovered_device) => {
                    devices.push(discovered_device.into());
                }
                DiscoveryEvent::DeviceLost(fullname) => {
                    devices.retain(|v| v.fullname != fullname);
                }
            }

            let slint_devices: Vec<Device> = devices
                .iter()
                .map(|d| Device {
                    hostname: d.hostname.clone().into(),
                    ip: d.ip.clone().into(),
                    port: d.port as i32,
                    initials: d.initials.clone().into(),
                })
                .collect();

            let _ = main_window_weak_devices.upgrade_in_event_loop(move |ui| {
                let model = rc_model_from_vec(slint_devices);
                ui.global::<AppData>().set_devices(model.into());
            });
        }
    });

    // Spawn GUI transfer and consent events loop
    let local_transfers_clone = local_transfers.clone();
    let main_window_weak_transfers = main_window_weak.clone();
    tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            let mut transfers = local_transfers_clone.lock().unwrap();
            let mut consent_to_set = None; // (has_consent, transfer_id, peer_ip, job_name)

            match event {
                GuiEvent::TransferStarted {
                    transfer_id,
                    is_sender,
                    job_name,
                    total_bytes,
                    cancel_token,
                } => {
                    transfers.push(GuiTransfer {
                        id: transfer_id,
                        is_sender,
                        job_name,
                        total_bytes,
                        bytes_transferred: 0,
                        start_time: std::time::Instant::now(),
                        cancel_token,
                    });
                }
                GuiEvent::ChunkTransferred { transfer_id, bytes } => {
                    if let Some(t) = transfers.iter_mut().find(|x| x.id == transfer_id) {
                        t.bytes_transferred += bytes;
                    }
                }
                GuiEvent::TransferFinished { transfer_id } => {
                    transfers.retain(|x| x.id != transfer_id);
                }
                GuiEvent::TransferFailed {
                    transfer_id,
                    error: _,
                } => {
                    transfers.retain(|x| x.id != transfer_id);
                }
                GuiEvent::IncomingConsentRequest {
                    transfer_id,
                    peer,
                    job_name,
                } => {
                    consent_to_set = Some((true, transfer_id, peer.ip().to_string(), job_name));
                }
            }

            let slint_transfers: Vec<Transfer> = transfers
                .iter()
                .map(|t| {
                    let trans_mb = t.bytes_transferred as f64 / 1_048_576.0;
                    let total_mb = t.total_bytes as f64 / 1_048_576.0;

                    let progress = if t.total_bytes > 0 {
                        t.bytes_transferred as f32 / t.total_bytes as f32
                    } else {
                        0.0
                    };

                    let elapsed = t.start_time.elapsed().as_secs_f64();
                    let speed_eta = if elapsed > 0.0 && t.bytes_transferred > 0 {
                        let speed = t.bytes_transferred as f64 / elapsed; // bytes/sec
                        let speed_mb = speed / 1_048_576.0;

                        let remaining = t.total_bytes.saturating_sub(t.bytes_transferred);
                        let eta = remaining as f64 / speed;
                        format!("{:.1} MB/s | ETA: {:.0}s", speed_mb, eta)
                    } else {
                        "Waiting...".to_string()
                    };

                    Transfer {
                        id: t.id as i32,
                        is_sender: t.is_sender,
                        job_name: t.job_name.clone().into(),
                        total_bytes: format!("{:.1} MB", total_mb).into(),
                        bytes_transferred: format!("{:.1} MB", trans_mb).into(),
                        progress,
                        speed_eta: speed_eta.into(),
                    }
                })
                .collect();

            let _ = main_window_weak_transfers.upgrade_in_event_loop(move |ui| {
                // 1. Get the current model from the UI
                let current_model = ui.global::<AppData>().get_active_transfers();
                
                // 2. Downcast it to the mutable VecModel we created earlier
                if let Some(vec_model) = current_model.as_any().downcast_ref::<slint::VecModel<Transfer>>() {
                    if vec_model.row_count() == slint_transfers.len() {
                        // NO TRANSFERS ADDED OR REMOVED.
                        // Update the data in place. This prevents the ZenButton 
                        // from being destroyed, fixing the click bug!
                        for (i, transfer) in slint_transfers.into_iter().enumerate() {
                            vec_model.set_row_data(i, transfer);
                        }
                    } else {
                        // A transfer was added or removed. It is safe to rebuild the list.
                        vec_model.set_vec(slint_transfers);
                    }
                }

                if let Some((has_req, id, ip, name)) = consent_to_set {
                    ui.global::<AppData>().set_has_consent_request(has_req);
                    ui.global::<AppData>().set_consent_transfer_id(id as i32);
                    ui.global::<AppData>().set_consent_peer_ip(ip.into());
                    ui.global::<AppData>().set_consent_job_name(name.into());
                }
            });
        }
    });

    // 8. Cancel Transfer
    let transfers_clone = local_transfers.clone();
    main_window.global::<Logic>().on_cancel_transfer(move |transfer_id| {
        println!("Cancel clicked for transfer: {}", transfer_id);
        let transfers = transfers_clone.lock().unwrap();
        if let Some(transfer) = transfers.iter().find(|t| t.id == transfer_id as u32) {
            transfer.cancel_token.cancel();
        }
    });

    main_window.run()?;
    Ok(())
}

fn rc_model_from_vec<T: Clone + 'static>(v: Vec<T>) -> slint::ModelRc<T> {
    let vec_model = slint::VecModel::from(v);
    slint::ModelRc::new(vec_model)
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
