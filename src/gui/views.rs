use crate::config::Config;
use crate::discovery::DiscoveryEvent;
use crate::gui::state::{ConsentRegistry, GuiEvent, GuiTransfer};
use crate::gui::{background, callbacks};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;

slint::include_modules!();

pub fn run_gui(
    devices_rx: mpsc::Receiver<DiscoveryEvent>,
    event_tx: mpsc::UnboundedSender<GuiEvent>,
    event_rx: mpsc::UnboundedReceiver<GuiEvent>,
    consent_registry: Arc<ConsentRegistry>,
    _config: Config,
) -> anyhow::Result<()> {
    let selector = slint::BackendSelector::new()
        .backend_name("winit".into())
        .renderer_name("software".into());
    if let Err(err) = selector.select() {
        eprintln!("Failed to select backend: {:?}", err);
    }

    let main_window = MainWindow::new()?;
    let main_window_weak = main_window.as_weak();

    // Track state locally in the main UI thread
    let download_dir = Arc::new(std::sync::Mutex::new(PathBuf::from(
        "/home/kofta/Downloads/Tensou",
    )));

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

    let local_transfers = Arc::new(std::sync::Mutex::new(Vec::<GuiTransfer>::new()));

    // Create a mutable model and attach it to the UI immediately
    let initial_transfers_model = std::rc::Rc::new(slint::VecModel::<Transfer>::default());
    main_window
        .global::<AppData>()
        .set_active_transfers(initial_transfers_model.clone().into());

    callbacks::setup(
        &main_window,
        event_tx,
        consent_registry,
        download_dir,
        local_transfers.clone(),
    );

    background::spawn_discovery(&main_window_weak, devices_rx);
    background::spawn_transfers(&main_window_weak, local_transfers, event_rx);

    main_window.run()?;
    Ok(())
}
