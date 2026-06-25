use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Instant};

use crate::{
    discovery::DiscoveryEvent,
    gui::{
        GuiConsent, GuiDevice, GuiTransfer,
        state::{ConsentRegistry, GuiEvent, GuiScreen, GuiState, GuiTransferObserver},
    },
    net,
};
use elegance::{
    Accent, Avatar, AvatarSize, BuiltInTheme, Button, Card, IndicatorState, Modal, ProgressBar,
    Spinner, StatusPill, TabBar, TextInput, Theme, ThemeSwitcher, egui, glyphs,
};
use tokio::sync::mpsc;

pub struct GuiApp {
    pub state: GuiState,
    devices_rx: mpsc::Receiver<DiscoveryEvent>,
    event_tx: mpsc::UnboundedSender<GuiEvent>,
    event_rx: mpsc::UnboundedReceiver<GuiEvent>,
    consent_registry: Arc<ConsentRegistry>,
}

impl GuiApp {
    pub fn new(
        devices_rx: mpsc::Receiver<DiscoveryEvent>,
        event_tx: mpsc::UnboundedSender<GuiEvent>,
        event_rx: mpsc::UnboundedReceiver<GuiEvent>,
        consent_registry: Arc<ConsentRegistry>,
    ) -> Self {
        Self {
            state: GuiState::default(),
            devices_rx,
            event_tx,
            event_rx,
            consent_registry,
        }
    }
}

impl eframe::App for GuiApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();

        self.apply_theme(&ctx);
        self.handle_incoming_events(&ctx);

        ui.spacing_mut().item_spacing = egui::vec2(10.0, 15.0);

        self.draw_header(ui);
        self.draw_tab_bar(ui);

        match self.state.current_tab {
            GuiScreen::Send => {
                self.draw_send_tab(ui);
            }
            GuiScreen::Transfers => {
                self.draw_transfers_tab(ui);
            }
            GuiScreen::Settings => {
                self.draw_settings_tab(ui);
            }
        }

        self.draw_consent_model(&ctx);
    }
}

impl GuiApp {
    fn handle_incoming_events(&mut self, ctx: &egui::Context) {
        // Handle app events
        while let Ok(event) = self.event_rx.try_recv() {
            match event {
                GuiEvent::TransferStarted {
                    transfer_id,
                    is_sender,
                    job_name,
                    total_bytes,
                } => {
                    self.state.active_transfers.push(GuiTransfer {
                        id: transfer_id,
                        is_sender,
                        job_name,
                        total_bytes,
                        bytes_transferred: 0,
                        start_time: Instant::now(),
                    });
                }
                GuiEvent::ChunkTransferred { transfer_id, bytes } => {
                    if let Some(t) = self
                        .state
                        .active_transfers
                        .iter_mut()
                        .find(|x| x.id == transfer_id)
                    {
                        t.bytes_transferred += bytes;
                    }
                }
                GuiEvent::TransferFinished { transfer_id } => {
                    self.state.active_transfers.retain(|x| x.id != transfer_id);
                }
                GuiEvent::TransferFailed { transfer_id, error } => {
                    self.state.active_transfers.retain(|x| x.id != transfer_id);
                    println!("Transfer {} failed: {}", transfer_id, error);
                }
                GuiEvent::IncomingConsentRequest {
                    transfer_id,
                    peer,
                    job_name,
                } => {
                    self.state.pending_consent = Some(GuiConsent {
                        transfer_id,
                        peer_addr: peer,
                        job_name,
                    });

                    ctx.request_repaint();
                }
            }
        }

        while let Ok(event) = self.devices_rx.try_recv() {
            match event {
                DiscoveryEvent::DeviceFound(discovered_device) => {
                    self.state.devices.push(discovered_device.into());
                }
                DiscoveryEvent::DeviceLost(fullname) => {
                    self.state.devices.retain(|v| v.fullname != fullname);
                }
            }
        }
    }

    fn apply_theme(&self, ctx: &egui::Context) {
        // TODO: Only use 1 theme
        let theme = match self.state.current_theme {
            BuiltInTheme::Charcoal => Theme::charcoal(),
            BuiltInTheme::Frost => Theme::frost(),
            BuiltInTheme::Paper => Theme::paper(),
            BuiltInTheme::Slate => Theme::slate(),
            _ => Theme::slate(),
        };
        theme.install(&ctx);

        // Continuous repaint if there are active transfers to animate progress bars smoothly
        if !self.state.active_transfers.is_empty() {
            elegance::request_repaint_at_rate(&ctx, 30.0);
        }
    }

    fn draw_header(&self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.label(
                egui::RichText::new("Tensou")
                    .size(24.0)
                    .strong()
                    .color(ui.visuals().widgets.active.text_color()),
            );

            ui.add_space(5.0);

            // Show local daemon status indicator
            // TODO: Wire this status to whether the ReceiverDaemon listener task is successfully running
            ui.add(StatusPill::new().item("Daemon", IndicatorState::On));
        });
    }

    fn draw_tab_bar(&mut self, ui: &mut egui::Ui) {
        let transfers_tab_title = if self.state.active_transfers.is_empty() {
            "Transfers".to_string()
        } else {
            format!("Transfers ({})", self.state.active_transfers.len())
        };

        let mut current_idx = self.state.current_tab.into();

        ui.add(TabBar::new(
            &mut current_idx,
            [
                "Send (Devices)".to_string(),
                transfers_tab_title,
                "Settings".to_string(),
            ],
        ));

        self.state.current_tab = current_idx.into();
    }

    fn draw_send_tab(&mut self, ui: &mut egui::Ui) {
        ui.vertical(|ui| {
            Card::new().heading("Local Network Devices").show(ui, |ui| {
                if self.state.devices.is_empty() {
                    ui.vertical_centered(|ui| {
                        ui.add_space(20.0);
                        ui.add(Spinner::new().size(24.0));
                        ui.add_space(10.0);
                        ui.label("Scanning for receivers on the LAN...");
                    });
                } else {
                    egui::ScrollArea::vertical()
                        .max_height(200.0)
                        .show(ui, |ui| {
                            for dev in &self.state.devices {
                                self.draw_device_row(ui, dev);
                                ui.add_space(4.0);
                            }
                        });
                }
            });

            ui.add_space(5.0);

            Card::new().heading("Direct Connect").show(ui, |ui| {
                ui.vertical(|ui| {
                    ui.label("Target Device Address:");
                    ui.add(TextInput::new(&mut self.state.direct_ip).hint("192.168.1.100:6967"));

                    ui.add_space(10.0);

                    ui.horizontal(|ui| {
                        // Send File
                        if ui
                            .add(Button::new("Send File").accent(Accent::Blue))
                            .clicked()
                        {
                            if let Ok(target_addr) = self.state.direct_ip.parse::<SocketAddr>() {
                                if let Some(path) = rfd::FileDialog::new()
                                    .set_title("Select File to Send")
                                    .pick_file()
                                {
                                    self.send(ui, target_addr, path);
                                }
                            } else {
                                println!("Invalid target IP address");
                            }
                        }

                        ui.add_space(5.0);

                        // Send Folder
                        if ui.add(Button::new("Send Folder")).clicked() {
                            if let Ok(target_addr) = self.state.direct_ip.parse::<SocketAddr>() {
                                if let Some(path) = rfd::FileDialog::new()
                                    .set_title("Select Folder to Send")
                                    .pick_folder()
                                {
                                    self.send(ui, target_addr, path);
                                }
                            } else {
                                println!("Invalid target IP address");
                            }
                        }
                    });
                });
            });
        });
    }

    fn draw_device_row(&self, ui: &mut egui::Ui, dev: &GuiDevice) {
        ui.group(|ui| {
            ui.horizontal(|ui| {
                ui.add(
                    Avatar::new(&dev.initials)
                        .size(AvatarSize::Medium)
                        .tone(dev.tone)
                        .presence(dev.presence),
                );

                ui.vertical(|ui| {
                    ui.label(egui::RichText::new(&dev.hostname).strong());
                    ui.label(format!("{}:{}", dev.ip, dev.port));
                });

                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if let Ok(target_addr) =
                        format!("{}:{}", dev.ip, dev.port).parse::<SocketAddr>()
                    {
                        // Send File
                        if ui
                            .add(Button::new("Send File").accent(Accent::Blue))
                            .clicked()
                        {
                            if let Some(path) = rfd::FileDialog::new()
                                .set_title("Select File to Send")
                                .pick_file()
                            {
                                self.send(ui, target_addr, path);
                            }
                        }

                        ui.add_space(5.0);

                        // Send Folder
                        if ui.add(Button::new("Send Folder")).clicked() {
                            if let Some(path) = rfd::FileDialog::new()
                                .set_title("Select Folder to Send")
                                .pick_folder()
                            {
                                self.send(ui, target_addr, path);
                            }
                        }
                    }
                });
            });
        });
    }

    fn send(&self, ui: &mut egui::Ui, target_addr: SocketAddr, path: PathBuf) {
        // let path = std::path::PathBuf::from("random_file.bin");
        let transfer_id = rand::random::<u32>();
        let tx_clone = self.event_tx.clone();
        let ctx_clone = ui.ctx().clone();

        tokio::spawn(async move {
            let job_name = path
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|| "Unknown".to_string());

            match net::Sender::connect(target_addr, &path).await {
                Ok(client) => {
                    let total_bytes = client.get_remaining_bytes();
                    let _ = tx_clone.send(GuiEvent::TransferStarted {
                        transfer_id,
                        is_sender: true,
                        job_name,
                        total_bytes,
                    });
                    ctx_clone.request_repaint();

                    let observer = std::sync::Arc::new(GuiTransferObserver {
                        transfer_id,
                        tx: tx_clone.clone(),
                        ctx: ctx_clone.clone(),
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
            ctx_clone.request_repaint();
        });
    }

    fn draw_transfers_tab(&self, ui: &mut egui::Ui) {
        Card::new()
            .heading("Active Transfers Monitor")
            .show(ui, |ui| {
                if self.state.active_transfers.is_empty() {
                    ui.vertical_centered(|ui| {
                        ui.add_space(40.0);
                        ui.label(
                            egui::RichText::new(glyphs::NETWORK.to_string())
                                .size(32.0)
                                .color(ui.visuals().weak_text_color()),
                        );
                        ui.add_space(10.0);
                        ui.label("No active file transfers.");
                        ui.label("Choose a device on the 'Send' tab to start transferring files.");
                        ui.add_space(40.0);
                    });
                } else {
                    egui::ScrollArea::vertical()
                        .max_height(400.0)
                        .show(ui, |ui| {
                            let mut cancel_triggered_idx: Option<usize> = None;

                            for (idx, transfer) in self.state.active_transfers.iter().enumerate() {
                                let direction = if transfer.is_sender {
                                    "Sending"
                                } else {
                                    "Receiving"
                                };
                                ui.group(|ui| {
                                    let progress = if transfer.total_bytes > 0 {
                                        transfer.bytes_transferred as f32
                                            / transfer.total_bytes as f32
                                    } else {
                                        0.0
                                    };

                                    let elapsed = transfer.start_time.elapsed().as_secs_f64();
                                    let speed_bytes_s = if elapsed > 0.1 {
                                        transfer.bytes_transferred as f64 / elapsed
                                    } else {
                                        0.0
                                    };

                                    let speed_mb_s = speed_bytes_s / 1_048_576.0; // MiB/s

                                    let remaining_bytes = transfer
                                        .total_bytes
                                        .saturating_sub(transfer.bytes_transferred);

                                    let eta_seconds = if speed_bytes_s > 0.0 {
                                        (remaining_bytes as f64 / speed_bytes_s) as u32
                                    } else {
                                        0
                                    };

                                    ui.vertical(|ui| {
                                        ui.horizontal(|ui| {
                                            ui.label(
                                                egui::RichText::new(if transfer.is_sender {
                                                    glyphs::UPLOAD.to_string()
                                                } else {
                                                    glyphs::DOWNLOAD.to_string()
                                                })
                                                .size(20.0),
                                            );
                                            ui.label(
                                                egui::RichText::new(&transfer.job_name).strong(),
                                            );
                                            ui.label(egui::RichText::new(direction).weak());
                                        });

                                        ui.add_space(4.0);

                                        // Progress bar
                                        ui.add(
                                            ProgressBar::new(progress)
                                                .accent(if transfer.is_sender {
                                                    Accent::Blue
                                                } else {
                                                    Accent::Green
                                                })
                                                .text(format!("{:.1}%", progress * 100.0)),
                                        );

                                        ui.add_space(4.0);

                                        ui.horizontal(|ui| {
                                            let trans_mb =
                                                transfer.bytes_transferred as f64 / 1_048_576.0;
                                            let total_mb =
                                                transfer.total_bytes as f64 / 1_048_576.0;
                                            ui.label(format!(
                                                "{:.1} MB / {:.1} MB",
                                                trans_mb, total_mb
                                            ));
                                            ui.separator();
                                            ui.label(format!("Speed: {:.1} MB/s", speed_mb_s));
                                            ui.separator();
                                            ui.label(format!("ETA: {} seconds", eta_seconds));

                                            ui.with_layout(
                                                egui::Layout::right_to_left(egui::Align::Center),
                                                |ui| {
                                                    if ui
                                                        .add(
                                                            Button::new("Cancel")
                                                                .accent(Accent::Red),
                                                        )
                                                        .clicked()
                                                    {
                                                        cancel_triggered_idx = Some(idx);
                                                    }
                                                },
                                            );
                                        });
                                    });
                                });
                                ui.add_space(8.0);
                            }

                            if let Some(cancel_idx) = cancel_triggered_idx {
                                // TODO: Abort the task for transfer at index cancel_idx and remove it from self.state.active_transfers
                                println!("Cancel clicked for transfer index: {}", cancel_idx);
                            }
                        });
                }
            });
    }

    fn draw_settings_tab(&mut self, ui: &mut egui::Ui) {
        Card::new().heading("Application Settings").show(ui, |ui| {
            ui.vertical(|ui| {
                ui.horizontal(|ui| {
                    ui.label("Download Save Location:");
                    ui.label(
                        egui::RichText::new(self.state.download_dir.to_string_lossy()).strong(),
                    );

                    // TODO: Trigger platform directory picker and update self.state.download_dir
                    if ui
                        .add(Button::new("Change Directory").accent(Accent::Blue))
                        .clicked()
                    {
                        println!("Change directory picker requested");
                    }
                });

                ui.add_space(10.0);
                ui.separator();
                ui.add_space(10.0);

                ui.horizontal(|ui| {
                    ui.label("Listening Network Port:");
                    ui.label(egui::RichText::new(format!("{}", self.state.listen_port)).strong());
                });

                ui.add_space(10.0);
                ui.separator();
                ui.add_space(10.0);

                ui.horizontal(|ui| {
                    ui.label("Visual App Theme Palette:");
                    ui.add(ThemeSwitcher::new(&mut self.state.current_theme));
                });
            });
        });
    }

    fn draw_consent_model(&mut self, ctx: &egui::Context) {
        let mut consent_open = self.state.pending_consent.is_some();
        let mut action_taken = None;
        if let Some(consent) = &self.state.pending_consent {
            let transfer_id = consent.transfer_id;
            Modal::new("incoming_consent_modal", &mut consent_open)
                .close_on_backdrop(false)
                .close_on_escape(false)
                .heading("Incoming Transfer Request")
                .header_icon(glyphs::CIRCLE_ALERT.to_string())
                .header_accent(Accent::Amber)
                .show(&ctx, |ui| {
                    ui.vertical(|ui| {
                        ui.label(
                            egui::RichText::new(format!(
                                "{} wants to send you:",
                                consent.peer_addr.ip()
                            ))
                            .strong(),
                        );
                        ui.label(egui::RichText::new(&consent.job_name).size(15.0).strong());

                        ui.add_space(15.0);

                        ui.horizontal(|ui| {
                            if ui
                                .add(Button::new("Accept").accent(Accent::Green))
                                .clicked()
                            {
                                action_taken = Some(true);
                                println!("Accepting transfer request!");
                            }
                            ui.add_space(5.0);
                            if ui.add(Button::new("Decline").accent(Accent::Red)).clicked() {
                                action_taken = Some(false);
                                println!("Declining transfer request!");
                            }
                        });
                    });
                });

            if !consent_open {
                action_taken = Some(false);
            }

            if let Some(accepted) = action_taken {
                if accepted {
                    self.consent_registry.accept(transfer_id);
                } else {
                    self.consent_registry.reject(transfer_id);
                }
                self.state.pending_consent = None;
            }
        }
    }
}
