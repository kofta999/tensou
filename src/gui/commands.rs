use std::{net::SocketAddr, path::PathBuf};

pub enum GuiCommand {
    /// Start sending a file or folder to a remote address
    SendPath {
        recipient: SocketAddr,
        path: PathBuf,
    },
    /// Respond to an incoming transfer consent prompt
    ConsentResponse { transfer_id: u32, accepted: bool },
    /// Cancel an active transfer
    CancelTransfer { transfer_id: u32 },
    /// Change the active download/save directory
    UpdateDownloadDir(PathBuf),
}
