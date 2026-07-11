use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::anyhow;
use quinn::{ClientConfig, Endpoint, crypto::rustls::QuicClientConfig};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{crypto::SkipServerVerification, protocol::TransferObserver};

#[derive(Debug)]
pub struct ConnectionManager {
    target_addr: SocketAddr,
    endpoint: quinn::Endpoint,
    pub(crate) connection: quinn::Connection,
}

impl ConnectionManager {
    pub async fn connect(target_addr: SocketAddr) -> anyhow::Result<Self> {
        let client_cfg = Self::configure_client()?;
        let bind_addr: SocketAddr = "0.0.0.0:0".parse()?;
        let mut endpoint = Endpoint::client(bind_addr)?;
        endpoint.set_default_client_config(client_cfg);

        log::info!("Connecting to remote receiver at {}...", target_addr);
        let connection = endpoint.connect(target_addr, "localhost")?.await?;
        log::debug!("QUIC connection established with {}", target_addr);

        Ok(Self {
            target_addr,
            endpoint,
            connection,
        })
    }

    async fn try_connect(
        target_addr: SocketAddr,
        endpoint: &quinn::Endpoint,
    ) -> anyhow::Result<quinn::Connection> {
        Ok(endpoint.connect(target_addr, "localhost")?.await?)
    }

    /// Called when a stream error indicates connection loss.
    /// Retries with exponential backoff. Returns Ok once re-established.
    pub async fn reconnect(
        &mut self,
        transfer_uuid: Uuid,
        observer: &dyn TransferObserver,
        cancel_token: &CancellationToken,
    ) -> anyhow::Result<()> {
        let mut attempt = 0u32;
        let max_wait = Duration::from_secs(30);
        let start = Instant::now();

        loop {
            if cancel_token.is_cancelled() {
                return Err(anyhow!("Cancelled during reconnect"));
            }
            if start.elapsed() > max_wait {
                return Err(anyhow!("Reconnect timeout"));
            }

            attempt += 1;
            observer.on_reconnecting(transfer_uuid, attempt);

            let backoff = Duration::from_secs(2).min(Duration::from_secs(attempt as u64));
            tokio::time::sleep(backoff).await;

            match Self::try_connect(self.target_addr, &self.endpoint).await {
                Ok(conn) => {
                    self.connection = conn;
                    observer.on_reconnected(transfer_uuid);
                    return Ok(());
                }
                Err(e) => log::warn!("Reconnect attempt {}: {}", attempt, e),
            }
        }
    }

    pub async fn open_bi(&self) -> anyhow::Result<(quinn::SendStream, quinn::RecvStream)> {
        Ok(self.connection.open_bi().await?)
    }

    pub async fn open_uni(&self) -> anyhow::Result<quinn::SendStream> {
        Ok(self.connection.open_uni().await?)
    }

    pub fn close_with(&self, code: u32, reason: &[u8]) {
        self.connection.close(code.into(), reason);
    }

    pub async fn close_gracefully(&self) {
        self.connection.closed().await;
    }

    pub fn connection(&self) -> quinn::Connection {
        self.connection.clone()
    }

    fn configure_client() -> anyhow::Result<ClientConfig> {
        let rustls_config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(SkipServerVerification::new())
            .with_no_client_auth();

        Ok(ClientConfig::new(Arc::new(QuicClientConfig::try_from(
            rustls_config,
        )?)))
    }
}
