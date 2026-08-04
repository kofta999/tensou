use rand::Rng;
use std::sync::Arc;
use tempfile::tempdir;
use tensou_core::disk::ReceiveSession;
use tensou_core::protocol::TransferObserver;
use tensou_core::{
    CHUNK_SIZE,
    disk::{SendSession, TransferStaging},
    protocol::{JobInstruction, Metadata},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

fn make_staging(dir: &std::path::Path, top_level_prefix: Option<&str>) -> Arc<TransferStaging> {
    let s = Arc::new(TransferStaging::new(dir.to_path_buf(), top_level_prefix));
    s.prepare().unwrap();
    s
}

struct TestObserver;
impl TransferObserver for TestObserver {}

/// Verifies a complete and byte-perfect local transfer of a single file.
#[tokio::test]
async fn test_full_local_transfer() -> anyhow::Result<()> {
    let source_dir = tempdir()?;
    let dest_dir = tempdir()?;
    let source_path = source_dir.path().join("source.bin");

    let mut buffer = vec![0u8; 10 * 1024 * 1024];
    rand::rng().fill_bytes(&mut buffer);
    std::fs::write(&source_path, &buffer)?;

    let metadata = Metadata {
        file_id: 0,
        relative_path: "source.bin".to_string(),
        size: 10 * 1024 * 1024,
        chunk_size: CHUNK_SIZE as u64,
        modified: 0,
    };
    let send_session = SendSession::new(metadata, &source_path)?;

    let instruction = JobInstruction::new(send_session.get_metadata());
    let staging = make_staging(dest_dir.path(), None);

    let receive_session = ReceiveSession::new(
        instruction,
        Uuid::new_v4(),
        Arc::new(TestObserver {}),
        CancellationToken::new(),
        staging,
    );

    for i in 0..send_session.get_total_chunks() {
        let (header, bytes) = send_session.get_chunk(i as u64).await?;
        receive_session.write_chunk(header, bytes).await?;
    }

    receive_session.join_writer().await?;

    assert!(file_diff::diff(
        source_path.to_str().unwrap(),
        dest_dir.path().join("source.bin").to_str().unwrap()
    ));

    Ok(())
}

struct ChunkSignalObserver {
    tx: mpsc::Sender<()>,
}
impl TransferObserver for ChunkSignalObserver {
    fn on_chunk_transferred(&self, _transfer_id: Uuid, _bytes: u64) {
        let tx = self.tx.clone();
        tokio::spawn(async move {
            let _ = tx.send(()).await;
        });
    }
}

/// Verifies that cancellation mid-transfer leaves the partial .part file intact.
#[tokio::test]
async fn test_cancel_preserves_partial_files() -> anyhow::Result<()> {
    let source_dir = tempdir()?;
    let dest_dir = tempdir()?;
    let source_path = source_dir.path().join("big.bin");

    let file_size = 3 * CHUNK_SIZE as u64;
    std::fs::write(&source_path, vec![0xABu8; file_size as usize])?;

    let metadata = Metadata {
        file_id: 0,
        relative_path: "big.bin".to_string(),
        size: file_size,
        chunk_size: CHUNK_SIZE as u64,
        modified: 0,
    };
    let send_session = SendSession::new(metadata, &source_path)?;
    let instruction = JobInstruction::new(send_session.get_metadata());
    let cancel_token = CancellationToken::new();

    let staging = make_staging(dest_dir.path(), None);
    let part_path = staging.part_path("big.bin");

    let (obs_tx, mut obs_rx) = mpsc::channel(1);
    let observer = Arc::new(ChunkSignalObserver { tx: obs_tx });

    let receive_session = ReceiveSession::new(
        instruction,
        Uuid::new_v4(),
        observer,
        cancel_token.clone(),
        staging,
    );

    let (header, bytes) = send_session.get_chunk(0).await?;
    receive_session.write_chunk(header, bytes).await?;

    let _ = obs_rx.recv().await;

    cancel_token.cancel();

    let _ = receive_session.join_writer().await;

    assert!(
        part_path.exists(),
        ".part file should be kept after cancellation"
    );

    Ok(())
}

/// Verifies that small file payload writing sets correct content, nested directory structure, and mtime.
#[tokio::test]
async fn test_small_files_disk_writer_direct() -> anyhow::Result<()> {
    use std::sync::mpsc;
use std::time::SystemTime;
    use tensou_core::disk::{SmallFilePayload, write_small_files_to_disk};

    let dest_dir = tempdir()?;
    let staging = make_staging(dest_dir.path(), None);

    let (tx, rx) = mpsc::sync_channel::<SmallFilePayload>(10);
    let expected_mtime_secs = 1_700_000_000u64;

    let payload1 = SmallFilePayload {
        relative_path: "sub/folder/file1.txt".to_string(),
        modified: expected_mtime_secs,
        data: b"hello small file 1".to_vec(),
    };

    let payload2 = SmallFilePayload {
        relative_path: "file2.json".to_string(),
        modified: expected_mtime_secs + 500,
        data: b"{\"test\": true}".to_vec(),
    };

    tx.send(payload1)?;
    tx.send(payload2)?;
    drop(tx);

    let observer = Arc::new(TestObserver {});
    let transfer_id = Uuid::new_v4();

    write_small_files_to_disk(rx, staging, observer, transfer_id)?;

    let path1 = dest_dir.path().join("sub/folder/file1.txt");
    let path2 = dest_dir.path().join("file2.json");

    assert_eq!(std::fs::read(&path1)?, b"hello small file 1");
    assert_eq!(std::fs::read(&path2)?, b"{\"test\": true}");

    let meta1 = std::fs::metadata(&path1)?;
    let mtime1 = meta1.modified()?.duration_since(SystemTime::UNIX_EPOCH)?.as_secs();
    assert_eq!(mtime1, expected_mtime_secs);

    let meta2 = std::fs::metadata(&path2)?;
    let mtime2 = meta2.modified()?.duration_since(SystemTime::UNIX_EPOCH)?.as_secs();
    assert_eq!(mtime2, expected_mtime_secs + 500);

    Ok(())
}
