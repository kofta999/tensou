mod recv;
mod send;
pub use recv::Receiver;
pub use recv::ReceiverDaemon;
pub use send::Sender;

#[cfg(test)]
mod tests {
    use crate::config::Config;
    use crate::protocol::TransferConsentHandler;
    use crate::protocol::TransferObserver;
    use async_trait::async_trait;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use rand::Rng;
    use tempfile::tempdir;
    use tokio::time::Duration;

    struct AutoAccept;

    #[async_trait]
    impl TransferConsentHandler for AutoAccept {
        async fn request_consent(&self, _peer: SocketAddr, _job_name: &str) -> bool {
            true
        }
    }

    struct TestObserver;
    impl TransferObserver for TestObserver {}

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_full_network_transfer() -> anyhow::Result<()> {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let source_dir = tempdir()?;
        let dest_dir = tempdir()?;
        let source_path = source_dir.path().join("source.bin");
        let received_dir = dest_dir.path().to_path_buf();

        let mut buffer = vec![0u8; 10 * 1024 * 1024];
        rand::rng().fill_bytes(&mut buffer);
        std::fs::write(&source_path, &buffer)?;

        let mut config = Config::default();
        config.target_dir = received_dir.clone();
        config.overwrite_dest = true;

        let app_daemon = ReceiverDaemon::new("127.0.0.1:0".parse()?, config)?;
        let bound_server_addr = app_daemon.endpoint.local_addr()?;

        let server_handle = tokio::spawn(async move {
            app_daemon
                .run(
                    Arc::new(AutoAccept),
                    Arc::new(TestObserver),
                    CancellationToken::new(),
                )
                .await;
        });

        // Give the server 50ms to boot up and start listening
        tokio::time::sleep(Duration::from_millis(50)).await;

        let client =
            Sender::connect(bound_server_addr, &source_path, CancellationToken::new()).await?;
        client.process_chunks(Arc::new(TestObserver {})).await?;

        // Give the server a tiny moment to flush the final commit() to disk
        tokio::time::sleep(Duration::from_millis(100)).await;

        // 7. Verification: Diff the files
        assert!(file_diff::diff(
            source_path.to_str().unwrap(),
            received_dir.join("source.bin").to_str().unwrap()
        ));

        // Clean up the background server task
        server_handle.abort();

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_unique_naming_transfer() -> anyhow::Result<()> {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let source_dir = tempdir()?;
        let dest_dir = tempdir()?;
        let source_path_1 = source_dir.path().join("source.bin");
        let source_path_2 = source_dir.path().join("source_different.bin");
        let received_dir = dest_dir.path().to_path_buf();

        let mut buffer_1 = vec![0u8; 1 * 1024 * 1024];
        rand::rng().fill_bytes(&mut buffer_1);
        std::fs::write(&source_path_1, &buffer_1)?;

        let mut buffer_2 = vec![0u8; 1 * 1024 * 1024];
        rand::rng().fill_bytes(&mut buffer_2);
        std::fs::write(&source_path_2, &buffer_2)?;

        let source_dir_2 = tempdir()?;
        let source_path_2_named_same = source_dir_2.path().join("source.bin");
        std::fs::copy(&source_path_2, &source_path_2_named_same)?;

        let mut config = Config::default();
        config.target_dir = received_dir.clone();

        let app_daemon = ReceiverDaemon::new("127.0.0.1:0".parse()?, config)?;
        let bound_server_addr = app_daemon.endpoint.local_addr()?;

        let server_handle = tokio::spawn(async move {
            app_daemon
                .run(
                    Arc::new(AutoAccept),
                    Arc::new(TestObserver),
                    CancellationToken::new(),
                )
                .await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let client_1 =
            Sender::connect(bound_server_addr, &source_path_1, CancellationToken::new()).await?;
        client_1.process_chunks(Arc::new(TestObserver {})).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;

        let client_2 = Sender::connect(
            bound_server_addr,
            &source_path_2_named_same,
            CancellationToken::new(),
        )
        .await?;
        client_2.process_chunks(Arc::new(TestObserver {})).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(file_diff::diff(
            source_path_1.to_str().unwrap(),
            received_dir.join("source.bin").to_str().unwrap()
        ));
        assert!(file_diff::diff(
            source_path_2_named_same.to_str().unwrap(),
            received_dir.join("source (1).bin").to_str().unwrap()
        ));

        server_handle.abort();
        Ok(())
    }
}
