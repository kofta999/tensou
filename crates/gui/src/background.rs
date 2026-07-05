use crate::GuiDevice;
use crate::GuiTransfer;
use crate::state::GuiEvent;
use crate::views::AppData;
use crate::views::Device;
use crate::views::MainWindow;
use crate::views::Transfer;
use slint::{ComponentHandle, Model, Weak};
use std::sync::{Arc, Mutex};
use tensou_core::discovery::DiscoveryEvent;
use tokio::sync::mpsc;

/// Background task to process mDNS discovery
pub fn spawn_discovery(
    main_window_weak: &Weak<MainWindow>,
    mut devices_rx: mpsc::Receiver<DiscoveryEvent>,
) {
    let local_devices = Arc::new(std::sync::Mutex::new(Vec::<GuiDevice>::new()));
    let main_window_weak_devices = main_window_weak.clone();
    tokio::spawn(async move {
        while let Some(event) = devices_rx.recv().await {
            let mut devices = local_devices.lock().unwrap();
            match event {
                DiscoveryEvent::DeviceFound(discovered_device) => {
                    println!(
                        "Discovered device: name={}, uuid={}, os={}",
                        discovered_device.display_name,
                        discovered_device.device_uuid,
                        discovered_device.os_type
                    );
                    // Check if we already have this device to prevent duplicate entries
                    devices.retain(|v| v.device_uuid != discovered_device.device_uuid);
                    devices.push(discovered_device.into());
                }
                DiscoveryEvent::DeviceLost(fullname) => {
                    // Extract display_name from fullname to match lost device
                    let display_name = fullname.split('.').next().unwrap_or(&fullname);
                    devices.retain(|v| v.display_name != display_name);
                }
            }

            let slint_devices: Vec<Device> = devices
                .iter()
                .map(|d| Device {
                    display_name: d.display_name.clone().into(),
                    device_uuid: d.device_uuid.clone().into(),
                    os_type: d.os_type.clone().into(),
                    ip: d.ip.clone().into(),
                    port: d.port as i32,
                    initials: d.initials.clone().into(),
                })
                .collect();

            let _ = main_window_weak_devices.upgrade_in_event_loop(move |ui| {
                let model = rc_model_from_vec(slint_devices);
                ui.global::<AppData>().set_devices(model);
            });
        }
    });
}

/// Spawn GUI transfer and consent events loop
pub fn spawn_transfers(
    main_window_weak: &Weak<MainWindow>,
    local_transfers: Arc<Mutex<Vec<GuiTransfer>>>,
    local_completed_transfers: Arc<Mutex<Vec<GuiTransfer>>>,
    mut event_rx: mpsc::UnboundedReceiver<GuiEvent>,
) {
    let local_transfers_clone = local_transfers.clone();
    let local_completed_transfers_clone = local_completed_transfers.clone();
    let main_window_weak_transfers = main_window_weak.clone();
    tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            let mut transfers = local_transfers_clone.lock().unwrap();
            let mut completed_transfers = local_completed_transfers_clone.lock().unwrap();
            let mut consent_to_set = None; // (has_consent, transfer_id, peer_ip, job_name)

            match event {
                GuiEvent::TransferStarted {
                    transfer_id,
                    is_sender,
                    job_name,
                    total_bytes,
                    cancel_token,
                    local_dir,
                } => {
                    transfers.push(GuiTransfer {
                        id: transfer_id,
                        is_sender,
                        job_name,
                        total_bytes,
                        bytes_transferred: 0,
                        start_time: std::time::Instant::now(),
                        cancel_token,
                        local_dir,
                    });
                }
                GuiEvent::ChunkTransferred { transfer_id, bytes } => {
                    if let Some(t) = transfers.iter_mut().find(|x| x.id == transfer_id) {
                        t.bytes_transferred += bytes;
                    }
                }
                GuiEvent::TransferFinished { transfer_id } => {
                    if let Some(pos) = transfers.iter().position(|x| x.id == transfer_id) {
                        let mut completed_t = transfers.remove(pos);
                        completed_t.bytes_transferred = completed_t.total_bytes;
                        completed_transfers.push(completed_t);
                    }
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

            let slint_completed: Vec<Transfer> = completed_transfers
                .iter()
                .map(|t| {
                    let total_mb = t.total_bytes as f64 / 1_048_576.0;
                    Transfer {
                        id: t.id as i32,
                        is_sender: t.is_sender,
                        job_name: t.job_name.clone().into(),
                        total_bytes: format!("{:.1} MB", total_mb).into(),
                        bytes_transferred: format!("{:.1} MB", total_mb).into(),
                        progress: 1.0,
                        speed_eta: "Completed".into(),
                    }
                })
                .collect();

            let _ = main_window_weak_transfers.upgrade_in_event_loop(move |ui| {
                let current_model = ui.global::<AppData>().get_active_transfers();

                if let Some(vec_model) = current_model
                    .as_any()
                    .downcast_ref::<slint::VecModel<Transfer>>()
                {
                    if vec_model.row_count() == slint_transfers.len() {
                        for (i, transfer) in slint_transfers.into_iter().enumerate() {
                            vec_model.set_row_data(i, transfer);
                        }
                    } else {
                        vec_model.set_vec(slint_transfers);
                    }
                }

                let current_completed = ui.global::<AppData>().get_completed_transfers();
                if let Some(vec_model) = current_completed
                    .as_any()
                    .downcast_ref::<slint::VecModel<Transfer>>()
                {
                    vec_model.set_vec(slint_completed);
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
}

pub fn rc_model_from_vec<T: Clone + 'static>(v: Vec<T>) -> slint::ModelRc<T> {
    let vec_model = slint::VecModel::from(v);
    slint::ModelRc::new(vec_model)
}
