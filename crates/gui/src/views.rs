use crate::{
    background, callbacks,
    state::{ConsentRegistry, GuiEvent, GuiTransfer},
};
use std::sync::Arc;
use tensou_core::config::Config;
use tensou_core::discovery::DiscoveryEvent;
use tokio::sync::mpsc;

slint::include_modules!();

pub fn run_gui(
    devices_rx: mpsc::Receiver<DiscoveryEvent>,
    event_tx: mpsc::UnboundedSender<GuiEvent>,
    event_rx: mpsc::UnboundedReceiver<GuiEvent>,
    consent_registry: Arc<ConsentRegistry>,
    config: Config,
) -> anyhow::Result<()> {
    let selector = slint::BackendSelector::new()
        .backend_name("winit".into())
        .renderer_name("software".into());
    if let Err(err) = selector.select() {
        log::error!("Failed to select backend: {:?}", err);
    }

    let main_window = MainWindow::new()?;
    let main_window_weak = main_window.as_weak();

    let config_state = Arc::new(std::sync::Mutex::new(config));

    {
        let cfg = config_state.lock().unwrap();
        let app_data = main_window.global::<AppData>();
        app_data.set_device_uuid(cfg.device_uuid.clone().into());
        app_data.set_display_name(cfg.display_name.clone().into());
        app_data.set_os_type(cfg.os_type.clone().into());
        app_data.set_download_dir(cfg.target_dir.to_string_lossy().to_string().into());
        app_data.set_overwrite_dest(cfg.overwrite_dest);
        app_data.set_listen_port(cfg.listen_port as i32);
    }

    let local_transfers = Arc::new(std::sync::Mutex::new(Vec::<GuiTransfer>::new()));
    let local_completed_transfers = Arc::new(std::sync::Mutex::new(Vec::<GuiTransfer>::new()));

    // Create a mutable model and attach it to the UI immediately
    let initial_transfers_model = std::rc::Rc::new(slint::VecModel::<Transfer>::default());
    main_window
        .global::<AppData>()
        .set_active_transfers(initial_transfers_model.clone().into());

    let initial_completed_model = std::rc::Rc::new(slint::VecModel::<Transfer>::default());
    main_window
        .global::<AppData>()
        .set_completed_transfers(initial_completed_model.clone().into());

    callbacks::setup(
        &main_window,
        event_tx,
        consent_registry,
        config_state,
        local_transfers.clone(),
        local_completed_transfers.clone(),
    );

    background::spawn_discovery(&main_window_weak, devices_rx);
    background::spawn_transfers(
        &main_window_weak,
        local_transfers,
        local_completed_transfers,
        event_rx,
    );

    main_window.run()?;
    Ok(())
}
