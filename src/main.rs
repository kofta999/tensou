#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let default_level = if cfg!(debug_assertions) {
        "debug"
    } else {
        "info"
    };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(default_level))
        .filter_module("tracing::span", log::LevelFilter::Off)
        .filter_module("winit", log::LevelFilter::Off)
        .init();

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install crypto provider");

    tensou_cli::run().await?;

    Ok(())
}
