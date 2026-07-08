use crate::GuiDevice;
use crate::GuiTransfer;
use crate::state::GuiEvent;
use crate::views::ToastType;
use crate::views::{
    AppData, ClipboardMessage, ConsentRequest, Device, MainWindow, ToastData, Transfer,
};
use slint::{ComponentHandle, Model, Weak};
use std::sync::{Arc, Mutex};
use tensou_core::discovery::DiscoveryEvent;
use tokio::sync::mpsc;

/// Background task to process mDNS discovery
pub fn spawn_discovery(
    main_window_weak: &Weak<MainWindow>,
    mut devices_rx: mpsc::Receiver<DiscoveryEvent>,
    local_devices: Arc<Mutex<Vec<GuiDevice>>>,
) {
    let main_window_weak_devices = main_window_weak.clone();
    tokio::spawn(async move {
        while let Some(event) = devices_rx.recv().await {
            let mut devices = local_devices.lock().unwrap();
            match event {
                DiscoveryEvent::DeviceFound(discovered_device) => {
                    log::info!(
                        "Discovered device: name={}, uuid={}, os={}",
                        discovered_device.display_name,
                        discovered_device.device_uuid,
                        discovered_device.os_type
                    );
                    show_toast(
                        main_window_weak_devices.clone(),
                        format!("Device online: {}", discovered_device.display_name),
                        ToastType::Info,
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
                .map(|d| {
                    let mut hash: u32 = 0;
                    for b in d.device_uuid.bytes() {
                        hash = hash.wrapping_add(b as u32).wrapping_mul(31);
                    }
                    let r = 120 + ((hash & 0xFF) % 100) as u8;
                    let g = 120 + (((hash >> 8) & 0xFF) % 100) as u8;
                    let b = 120 + (((hash >> 16) & 0xFF) % 100) as u8;
                    let color = slint::Color::from_rgb_u8(r, g, b);

                    Device {
                        display_name: d.display_name.clone().into(),
                        device_uuid: d.device_uuid.clone().into(),
                        os_type: d.os_type.clone().into(),
                        ip: d.ip.clone().into(),
                        port: d.port as i32,
                        initials: d.initials.clone().into(),
                        avatar_color: color,
                    }
                })
                .collect();

            let _ = main_window_weak_devices.upgrade_in_event_loop(move |ui| {
                let model = rc_model_from_vec(slint_devices);
                ui.global::<AppData>().set_devices(model);
            });
        }
    });
}

pub fn spawn_transfers(
    main_window_weak: &Weak<MainWindow>,
    local_transfers: Arc<Mutex<Vec<GuiTransfer>>>,
    local_completed_transfers: Arc<Mutex<Vec<GuiTransfer>>>,
    mut event_rx: mpsc::UnboundedReceiver<GuiEvent>,
    local_devices: Arc<Mutex<Vec<GuiDevice>>>,
) {
    let local_transfers_clone = local_transfers.clone();
    let local_completed_transfers_clone = local_completed_transfers.clone();
    let main_window_weak_transfers = main_window_weak.clone();
    tokio::spawn(async move {
        let mut clipboard_ctx = arboard::Clipboard::new().ok();
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(33)); // ~30 FPS
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut active_dirty = false;
        let mut completed_dirty = false;
        let mut consent_to_set = None; // (has_consent, transfer_id, peer_ip, job_name)

        loop {
            tokio::select! {
                maybe_event = event_rx.recv() => {
                    let event = match maybe_event {
                        Some(e) => e,
                        None => break, // Channel closed, exit task
                    };

                    let mut transfers = local_transfers_clone.lock().unwrap();
                    let mut completed_transfers = local_completed_transfers_clone.lock().unwrap();

                    match event {
                        GuiEvent::TransferStarted {
                            transfer_id,
                            is_sender,
                            job_name,
                            total_bytes,
                            bytes_done,
                            cancel_token,
                            local_dir,
                            peer_ip,
                        } => {
                            let peer_name = {
                                let devices = local_devices.lock().unwrap();
                                let clean_peer_ip = peer_ip.split(':').next().unwrap_or(&peer_ip);
                                devices
                                    .iter()
                                    .find(|d| {
                                        let clean_dev_ip = d.ip.split(':').next().unwrap_or(&d.ip);
                                        clean_dev_ip == clean_peer_ip
                                    })
                                    .map(|d| d.display_name.clone())
                                    .unwrap_or_else(|| peer_ip.clone())
                            };

                            transfers.push(GuiTransfer {
                                id: transfer_id,
                                is_sender,
                                job_name,
                                total_bytes,
                                bytes_transferred: bytes_done,
                                bytes_done_at_start: bytes_done,
                                start_time: std::time::Instant::now(),
                                cancel_token,
                                local_dir,
                                status: "Active".to_string(),
                                timestamp: chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                                peer_name,
                            });
                            active_dirty = true;
                            completed_dirty = true;
                        }
                        GuiEvent::ChunkTransferred { transfer_id, bytes } => {
                            if let Some(t) = transfers.iter_mut().find(|x| x.id == transfer_id) {
                                t.bytes_transferred += bytes;
                            }
                            active_dirty = true;
                        }
                        GuiEvent::TransferFinished { transfer_id } => {
                            if let Some(pos) = transfers.iter().position(|x| x.id == transfer_id) {
                                let mut completed_t = transfers.remove(pos);
                                completed_t.bytes_transferred = completed_t.total_bytes;
                                completed_t.status = "Completed".to_string();
                                show_toast(
                                    main_window_weak_transfers.clone(),
                                    format!("Completed: {}", completed_t.job_name),
                                    ToastType::Success,
                                );
                                completed_transfers.push(completed_t);
                            }
                            active_dirty = true;
                            completed_dirty = true;
                        }
                        GuiEvent::TransferFailed {
                            transfer_id,
                            error,
                        } => {
                            if let Some(pos) = transfers.iter().position(|x| x.id == transfer_id) {
                                let mut failed_t = transfers.remove(pos);
                                failed_t.status = if error.contains("Cancelled") || error.contains("cancelled") {
                                    "Cancelled".to_string()
                                } else {
                                    "Failed".to_string()
                                };
                                show_toast(
                                    main_window_weak_transfers.clone(),
                                    format!("{}: {}", failed_t.status, failed_t.job_name),
                                    ToastType::Error,
                                );
                                completed_transfers.push(failed_t);
                            }
                            active_dirty = true;
                            completed_dirty = true;
                        }
                        GuiEvent::TextReceived {
                            job_name,
                            content,
                            peer_ip,
                        } => {
                            show_toast(
                                main_window_weak_transfers.clone(),
                                format!("Received text from {}", job_name),
                                ToastType::Success,
                            );

                            if let Some(ref mut ctx) = clipboard_ctx {
                                let _ = ctx.set_text(content.clone());
                            }

                            let _ = main_window_weak_transfers.upgrade_in_event_loop(move |ui| {
                                let app_data = ui.global::<AppData>();
                                let current_history = app_data.get_clipboard_history();
                                let vec_model = slint::VecModel::default();

                                let new_msg = ClipboardMessage {
                                    id: rand::random::<i32>(),
                                    sender_name: job_name.into(),
                                    sender_ip: peer_ip.into(),
                                    content: content.into(),
                                };

                                vec_model.push(new_msg);
                                for old_msg in current_history.iter() {
                                    vec_model.push(old_msg.clone());
                                }
                                app_data.set_clipboard_history(slint::ModelRc::new(vec_model));
                            });
                        }
                        GuiEvent::IncomingConsentRequest {
                            transfer_id,
                            peer,
                            job_name,
                        } => {
                            consent_to_set = Some((true, transfer_id, peer.ip().to_string(), job_name));
                            active_dirty = true; // Refresh to show consent modal
                        }
                    }
                }
                _ = interval.tick() => {
                    if active_dirty || completed_dirty {
                        let update_active = active_dirty;
                        let update_completed = completed_dirty;
                        active_dirty = false;
                        completed_dirty = false;

                        let slint_transfers = if update_active {
                            let transfers = local_transfers_clone.lock().unwrap();
                            Some(transfers
                                .iter()
                                .map(|t| {
                                     let trans_mb = t.bytes_transferred as f64 / 1_048_576.0;
                                    let total_mb = t.total_bytes as f64 / 1_048_576.0;
                                    let remaining = t.total_bytes.saturating_sub(t.bytes_transferred);

                                    let progress = if t.total_bytes > 0 {
                                        t.bytes_transferred as f32 / t.total_bytes as f32
                                    } else {
                                        0.0
                                    };

                                    // Speed is derived from newly transferred bytes only (not resumed portion)
                                    let elapsed = t.start_time.elapsed().as_secs_f64();
                                    let new_bytes = t.bytes_transferred.saturating_sub(t.bytes_done_at_start);
                                    let speed_eta = if elapsed > 0.0 && new_bytes > 0 {
                                        let speed = new_bytes as f64 / elapsed;
                                        let speed_mb = speed / 1_048_576.0;
                                        let eta = remaining as f64 / speed;
                                        format!("{:.1} MB/s | ETA: {:.0}s", speed_mb, eta)
                                    } else {
                                        "Waiting...".to_string()
                                    };

                                    let bytes_label = if t.bytes_done_at_start > 0 {
                                        format!("{:.1} / {:.1} MB (+{:.1} MB resumed)",
                                            trans_mb, total_mb,
                                            t.bytes_done_at_start as f64 / 1_048_576.0)
                                    } else {
                                        format!("{:.1} / {:.1} MB", trans_mb, total_mb)
                                    };

                                    Transfer {
                                        id: t.id as i32,
                                        is_sender: t.is_sender,
                                        job_name: t.job_name.clone().into(),
                                        total_bytes: format!("{:.1} MB", total_mb).into(),
                                        bytes_transferred: bytes_label.into(),
                                        progress,
                                        speed_eta: speed_eta.into(),
                                        timestamp: t.timestamp.clone().into(),
                                        peer_name: t.peer_name.clone().into(),
                                    }
                                })
                                .collect::<Vec<Transfer>>())
                        } else {
                            None
                        };

                        let slint_completed = if update_completed {
                            let completed_transfers = local_completed_transfers_clone.lock().unwrap();
                            Some(completed_transfers
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
                                        speed_eta: t.status.clone().into(),
                                        timestamp: t.timestamp.clone().into(),
                                        peer_name: t.peer_name.clone().into(),
                                    }
                                })
                                .collect::<Vec<Transfer>>())
                        } else {
                            None
                        };

                        let consent_data = consent_to_set.take();

                        let _ = main_window_weak_transfers.upgrade_in_event_loop(move |ui| {
                            if let Some(active) = slint_transfers {
                                let current_model = ui.global::<AppData>().get_active_transfers();

                                if let Some(vec_model) = current_model
                                    .as_any()
                                    .downcast_ref::<slint::VecModel<Transfer>>()
                                {
                                    if vec_model.row_count() == active.len() {
                                        for (i, transfer) in active.into_iter().enumerate() {
                                            vec_model.set_row_data(i, transfer);
                                        }
                                    } else {
                                        vec_model.set_vec(active);
                                    }
                                }
                            }

                            if let Some(completed) = slint_completed {
                                let current_completed = ui.global::<AppData>().get_completed_transfers();
                                if let Some(vec_model) = current_completed
                                    .as_any()
                                    .downcast_ref::<slint::VecModel<Transfer>>()
                                {
                                    vec_model.set_vec(completed);
                                }
                            }

                            if let Some((has_req, id, ip, name)) = consent_data {
                                ui.global::<AppData>().set_has_consent_request(has_req);

                                // Resolve device name and OS from mDNS devices list
                                let devices = ui.global::<AppData>().get_devices();
                                let mut resolved_name = String::new();
                                let mut resolved_os = String::new();

                                for i in 0..devices.row_count() {
                                    if let Some(dev) = devices.row_data(i) {
                                        let clean_peer_ip = ip.split(':').next().unwrap_or(&ip);
                                        let clean_dev_ip = dev.ip.as_str().split(':').next().unwrap_or(dev.ip.as_str());
                                        if clean_peer_ip == clean_dev_ip {
                                            resolved_name = dev.display_name.to_string();
                                            resolved_os = dev.os_type.to_string();
                                            break;
                                        }
                                    }
                                }

                                let consent_req = ConsentRequest {
                                    transfer_id: id as i32,
                                    peer_ip: ip.clone().into(),
                                    device_name: resolved_name.into(),
                                    device_os: resolved_os.into(),
                                    job_name: name.into(),
                                };
                                ui.global::<AppData>().set_consent_request(consent_req);
                            }
                        });
                    }
                }
            }
        }
    });
}

pub fn rc_model_from_vec<T: Clone + 'static>(v: Vec<T>) -> slint::ModelRc<T> {
    let vec_model = slint::VecModel::from(v);
    slint::ModelRc::new(vec_model)
}

fn show_toast(ui_weak: Weak<MainWindow>, message: String, toast_type: ToastType) {
    let _ = ui_weak.upgrade_in_event_loop(move |ui| {
        let toast = ui.global::<ToastData>();
        toast.set_message(message.into());
        toast.set_toast_type(toast_type);
        toast.set_show(true);
    });

    let ui_weak_clone = ui_weak.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        let _ = ui_weak_clone.upgrade_in_event_loop(|ui| {
            ui.global::<ToastData>().set_show(false);
        });
    });
}
