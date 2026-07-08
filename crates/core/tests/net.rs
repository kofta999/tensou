use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use tensou_core::config::Config;
use tensou_core::net::ReceiverDaemon;
use tensou_core::net::SendType;
use tensou_core::net::Sender;
use tensou_core::protocol::TransferConsentHandler;
use tensou_core::protocol::TransferObserver;
use tokio_util::sync::CancellationToken;

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

struct AutoReject;

#[async_trait]
impl TransferConsentHandler for AutoReject {
    async fn request_consent(&self, _peer: SocketAddr, _job_name: &str) -> bool {
        false
    }
}

struct TestObserver;
impl TransferObserver for TestObserver {}

fn make_config(dest: &std::path::Path, overwrite: bool) -> Arc<Mutex<Config>> {
    Arc::new(Mutex::new(Config {
        target_dir: dest.to_path_buf(),
        overwrite_dest: overwrite,
        ..Default::default()
    }))
}

async fn spawn_daemon(
    config: Arc<Mutex<Config>>,
    consent: Arc<dyn TransferConsentHandler>,
    observer: Arc<dyn TransferObserver>,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let daemon = ReceiverDaemon::new("127.0.0.1:0".parse().unwrap(), config).unwrap();
    let addr = daemon.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let (_tx, rx) = tokio::sync::mpsc::channel::<()>(1);
        daemon
            .run(consent, observer, CancellationToken::new(), rx)
            .await;
    });
    // Give the server a moment to start listening
    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, handle)
}

/// Verifies a successful, byte-perfect single file transfer over the loopback network.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_full_network_transfer() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let source_dir = tempdir()?;
    let dest_dir = tempdir()?;
    let source_path = source_dir.path().join("source.bin");

    let mut buffer = vec![0u8; 10 * 1024 * 1024];
    rand::rng().fill_bytes(&mut buffer);
    std::fs::write(&source_path, &buffer)?;

    let (addr, server_handle) = spawn_daemon(
        make_config(dest_dir.path(), true),
        Arc::new(AutoAccept),
        Arc::new(TestObserver),
    )
    .await;

    let client = Sender::connect(
        addr,
        SendType::Single(&source_path),
        CancellationToken::new(),
    )
    .await?
    .unwrap();
    client.process_chunks(Arc::new(TestObserver {})).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(file_diff::diff(
        source_path.to_str().unwrap(),
        dest_dir.path().join("source.bin").to_str().unwrap()
    ));

    server_handle.abort();
    Ok(())
}

/// Verifies unique file name generation when a naming collision occurs at the receiver destination.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_unique_naming_transfer() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let source_dir = tempdir()?;
    let dest_dir = tempdir()?;
    let source_path_1 = source_dir.path().join("source.bin");
    let source_path_2 = source_dir.path().join("source_different.bin");

    let mut buffer_1 = vec![0u8; 1024 * 1024];
    rand::rng().fill_bytes(&mut buffer_1);
    std::fs::write(&source_path_1, &buffer_1)?;

    let mut buffer_2 = vec![0u8; 1024 * 1024];
    rand::rng().fill_bytes(&mut buffer_2);
    std::fs::write(&source_path_2, &buffer_2)?;

    let source_dir_2 = tempdir()?;
    let source_path_2_named_same = source_dir_2.path().join("source.bin");
    std::fs::copy(&source_path_2, &source_path_2_named_same)?;

    let (addr, server_handle) = spawn_daemon(
        make_config(dest_dir.path(), false),
        Arc::new(AutoAccept),
        Arc::new(TestObserver),
    )
    .await;

    let client_1 = Sender::connect(
        addr,
        SendType::Single(&source_path_1),
        CancellationToken::new(),
    )
    .await?
    .unwrap();
    client_1.process_chunks(Arc::new(TestObserver {})).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let client_2 = Sender::connect(
        addr,
        SendType::Single(&source_path_2_named_same),
        CancellationToken::new(),
    )
    .await?
    .unwrap();
    client_2.process_chunks(Arc::new(TestObserver {})).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(file_diff::diff(
        source_path_1.to_str().unwrap(),
        dest_dir.path().join("source.bin").to_str().unwrap()
    ));
    assert!(file_diff::diff(
        source_path_2_named_same.to_str().unwrap(),
        dest_dir.path().join("source (1).bin").to_str().unwrap()
    ));

    server_handle.abort();
    Ok(())
}

/// Verifies that a transfer is rejected properly by the receiver consent handler.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_transfer_rejected() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let source_dir = tempdir()?;
    let dest_dir = tempdir()?;
    let source_path = source_dir.path().join("file.bin");
    std::fs::write(&source_path, b"hello")?;

    let (addr, server_handle) = spawn_daemon(
        make_config(dest_dir.path(), true),
        Arc::new(AutoReject),
        Arc::new(TestObserver),
    )
    .await;

    let result = Sender::connect(
        addr,
        SendType::Single(&source_path),
        CancellationToken::new(),
    )
    .await;

    assert!(result.is_err(), "Sender should receive a rejection error");
    assert!(result.unwrap_err().to_string().contains("rejected"));

    assert!(dest_dir.path().read_dir()?.next().is_none());

    server_handle.abort();
    Ok(())
}

/// Verifies a complete directory transfer containing nested subdirectories and files.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_directory_transfer() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let source_dir = tempdir()?;
    let dest_dir = tempdir()?;

    let job_dir = source_dir.path().join("job");
    std::fs::create_dir_all(job_dir.join("nested"))?;
    std::fs::write(job_dir.join("a.txt"), b"file A contents")?;
    std::fs::write(job_dir.join("nested/b.txt"), b"file B in nested dir")?;

    let (addr, server_handle) = spawn_daemon(
        make_config(dest_dir.path(), true),
        Arc::new(AutoAccept),
        Arc::new(TestObserver),
    )
    .await;

    let client = Sender::connect(addr, SendType::Single(&job_dir), CancellationToken::new())
        .await?
        .unwrap();
    client.process_chunks(Arc::new(TestObserver {})).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let received_job = dest_dir.path().join("job");

    assert!(file_diff::diff(
        job_dir.join("a.txt").to_str().unwrap(),
        received_job.join("a.txt").to_str().unwrap(),
    ));
    assert!(file_diff::diff(
        job_dir.join("nested/b.txt").to_str().unwrap(),
        received_job.join("nested/b.txt").to_str().unwrap(),
    ));

    assert!(
        !received_job.join(".tensou").exists(),
        ".tensou should be removed after success"
    );

    server_handle.abort();
    Ok(())
}

/// Verifies that sender-side cancellation mid-transfer keeps partial staging files intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_sender_cancel_leaves_partial_files() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let source_dir = tempdir()?;
    let dest_dir = tempdir()?;
    let source_path = source_dir.path().join("big.bin");
    std::fs::write(&source_path, vec![0xBBu8; 20 * 1024 * 1024])?;

    let (addr, server_handle) = spawn_daemon(
        make_config(dest_dir.path(), true),
        Arc::new(AutoAccept),
        Arc::new(TestObserver),
    )
    .await;

    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    let client = Sender::connect(addr, SendType::Single(&source_path), cancel.clone())
        .await?
        .unwrap();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel_clone.cancel();
    });

    let _ = client.process_chunks(Arc::new(TestObserver {})).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let staging_dir = dest_dir.path().join(".tensou");
    assert!(
        staging_dir.exists(),
        ".tensou should survive a cancelled transfer"
    );

    server_handle.abort();
    Ok(())
}

/// Verifies that receiver-side cancellation mid-transfer keeps partial staging files intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_receiver_cancel_leaves_partial_files() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let source_dir = tempdir()?;
    let dest_dir = tempdir()?;
    let source_path = source_dir.path().join("big.bin");
    std::fs::write(&source_path, vec![0xCCu8; 20 * 1024 * 1024])?;

    let parent_cancel = CancellationToken::new();
    let daemon = ReceiverDaemon::new("127.0.0.1:0".parse()?, make_config(dest_dir.path(), true))?;
    let addr = daemon.local_addr()?;
    let parent_cancel_clone = parent_cancel.clone();

    let server_handle = tokio::spawn(async move {
        let (_tx, rx) = tokio::sync::mpsc::channel::<()>(1);
        daemon
            .run(
                Arc::new(AutoAccept),
                Arc::new(TestObserver),
                parent_cancel_clone,
                rx,
            )
            .await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = Sender::connect(
        addr,
        SendType::Single(&source_path),
        CancellationToken::new(),
    )
    .await?
    .unwrap();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        parent_cancel.cancel();
    });

    let _ = client.process_chunks(Arc::new(TestObserver {})).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let staging_dir = dest_dir.path().join(".tensou");
    assert!(
        staging_dir.exists(),
        ".tensou should survive receiver-side cancellation"
    );

    server_handle.abort();
    Ok(())
}

/// Measures the performance of transferring many small files.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_many_small_files_performance() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let source_dir = tempdir()?;
    let dest_dir = tempdir()?;
    let source_folder = source_dir.path().join("small_files");
    std::fs::create_dir(&source_folder)?;

    let num_files = 500;
    let file_size = 10 * 1024; // 10 KB
    let content = vec![0xEEu8; file_size];
    for i in 0..num_files {
        let file_path = source_folder.join(format!("file_{}.bin", i));
        std::fs::write(&file_path, &content)?;
    }

    let (addr, server_handle) = spawn_daemon(
        make_config(dest_dir.path(), true),
        Arc::new(AutoAccept),
        Arc::new(TestObserver),
    )
    .await;

    let start = std::time::Instant::now();
    let client = Sender::connect(
        addr,
        SendType::Single(&source_folder),
        CancellationToken::new(),
    )
    .await?
    .unwrap();
    client.process_chunks(Arc::new(TestObserver {})).await?;
    let elapsed = start.elapsed();

    let total_size = num_files * file_size;
    let speed_mb_s = (total_size as f64 / 1024.0 / 1024.0) / elapsed.as_secs_f64();
    eprintln!(
        "=== PERF RESULT: Transferred {} small files ({:.2} MB total) in {:.2?} ({:.2} MB/s) ===",
        num_files,
        total_size as f64 / 1024.0 / 1024.0,
        elapsed,
        speed_mb_s
    );

    // Verify some files are present and match
    for i in [0, num_files / 2, num_files - 1] {
        let src = source_folder.join(format!("file_{}.bin", i));
        let dst = dest_dir
            .path()
            .join("small_files")
            .join(format!("file_{}.bin", i));
        assert!(
            dst.exists(),
            "Destination file {} should exist",
            dst.display()
        );
        assert!(
            file_diff::diff(src.to_str().unwrap(), dst.to_str().unwrap()),
            "File diff should match"
        );
    }

    server_handle.abort();
    Ok(())
}
