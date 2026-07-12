use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::{SERVER_PORT, protocol::TransferMode};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Unique identifier for this device (generated on first run).
    /// Used in mDNS TXT records to prevent self-scanning / infinite loops.
    pub device_uuid: String,

    /// User-defined device alias (defaults to system hostname).
    pub display_name: String,

    /// The operating system of this device (e.g., "linux", "macos", "windows").
    /// Sent via discovery to allow the UI to display native OS icons next to receivers.
    pub os_type: String,

    /// Base target directory for receiving files.
    pub target_dir: PathBuf,

    /// How to handle file conflicts on the receiver side.
    pub transfer_mode: TransferMode,

    /// The port to bind for incoming QUIC connections (default: 6967).
    pub listen_port: u16,

    /// Whether to auto-accept incoming requests without consent
    pub auto_accept: bool,
}

impl Default for Config {
    fn default() -> Self {
        let os_type = std::env::consts::OS.to_string();
        let hostname = gethostname::gethostname()
            .into_string()
            .unwrap_or_else(|_| "Unknown Device".to_string());

        let target_dir = dirs::download_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("Tensou");

        Self {
            device_uuid: uuid::Uuid::new_v4().to_string(),
            display_name: hostname,
            os_type,
            target_dir,
            transfer_mode: TransferMode::Unique,
            listen_port: SERVER_PORT,
            auto_accept: false,
        }
    }
}

impl Config {
    /// Returns the standard system path for Tensou's config (e.g., ~/.config/tensou/config.toml)
    pub fn config_path() -> Option<PathBuf> {
        dirs::config_dir().map(|path| path.join("tensou").join("config.toml"))
    }

    /// Load config from disk, or create and save defaults if it doesn't exist
    pub fn load_or_create() -> Self {
        if let Some(path) = Self::config_path()
            && path.exists()
            && let Ok(content) = std::fs::read_to_string(&path)
        {
            match toml::from_str::<Config>(&content) {
                Ok(config) => return config,
                Err(e) => log::error!("Config parsing error: {e}"),
            }
        }

        // Fallback to default, write it to disk
        let default_config = Config::default();
        let _ = default_config.save();
        default_config
    }

    /// Serialize and write config back to disk
    pub fn save(&self) -> Result<(), std::io::Error> {
        if let Some(path) = Self::config_path() {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let toml_str = toml::to_string_pretty(self).map_err(std::io::Error::other)?;
            std::fs::write(path, toml_str)?;
        }
        Ok(())
    }
}
