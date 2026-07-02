use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};
use std::{net::IpAddr, path::PathBuf};
use tensou_core::SERVER_PORT;

mod recieve;
mod send;

#[derive(Parser)]
#[command(name = "Tensou")]
struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Send a file or folder over the local network
    Send {
        /// The absolute or relative path to the file/folder you want to send
        #[arg(required = true)]
        path: PathBuf,

        /// Optional: Custom IP address to send to directly
        #[arg(long)]
        ip: Option<IpAddr>,

        /// Optional: Custom port to associate with the IP
        #[arg(short, long, default_value_t = SERVER_PORT, requires = "ip")]
        port: u16,
    },

    /// Listen for incoming file transfers
    Receive {
        /// Optional: Force the server to bind to a specific port
        #[arg(short, long, default_value_t = SERVER_PORT)]
        port: u16,

        /// Optional: Override the default save location
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
}

pub async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Send { path, ip, port }) => send::run(path, ip, port).await,
        Some(Commands::Receive { port, output }) => recieve::run(port, output).await,
        None => tensou_gui::run(),
    }
}

fn resolve_save_directory(user_provided_path: Option<PathBuf>) -> anyhow::Result<PathBuf> {
    if let Some(path) = user_provided_path {
        std::fs::create_dir_all(&path)?;
        return Ok(path.canonicalize()?);
    }

    let downloads_dir = dirs::download_dir()
        .ok_or_else(|| anyhow::anyhow!("Could not locate the system Downloads directory"))?;

    let tensou_dir = downloads_dir.join("Tensou");

    std::fs::create_dir_all(&tensou_dir)?;
    Ok(tensou_dir.canonicalize()?)
}

fn create_transfer_pb(total_bytes: u64, name: &str, is_sender: bool) -> ProgressBar {
    let pb = ProgressBar::new(total_bytes);

    let style = ProgressStyle::default_bar()
        .template(
            "{spinner:.green} {msg}\n{bytes:>10} / {total_bytes:10} [{bar:40.cyan/blue}] {percent}% {bytes_per_sec} | {eta}"
        )
        .unwrap()
        .progress_chars("━╾─");

    pb.set_style(style);

    let label = if is_sender { "Sending" } else { "Receiving" };
    if !name.is_empty() {
        pb.set_message(format!("{label}: {name}"));
    }

    pb
}
