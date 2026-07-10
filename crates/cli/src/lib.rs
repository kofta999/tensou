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
        #[arg(required = true, num_args= 1..)]
        paths: Vec<PathBuf>,

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
        Some(Commands::Send { paths, ip, port }) => send::run(paths, ip, port).await,
        Some(Commands::Receive { port, output }) => recieve::run(port, output).await,
        None => tensou_gui::run(),
    }
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
