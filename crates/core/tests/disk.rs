use rand::Rng;
use std::sync::Arc;
use tempfile::tempdir;
use tensou_core::disk::IgnitionPayload;
use tensou_core::disk::ReceiveSession;
use tensou_core::protocol::TransferObserver;
use tensou_core::{
    CHUNK_SIZE,
    disk::{SendSession, TransferStaging},
    protocol::{ChunkPacket, JobInstruction, Metadata},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

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
    };
    let send_session = SendSession::new(metadata, &source_path)?;

    let (tx, rx) = mpsc::channel::<ChunkPacket>(16);
    let instruction = JobInstruction::new(send_session.get_metadata());

    let staging = make_staging(dest_dir.path(), None);

    let ignition = IgnitionPayload {
        ins: instruction,
        rx,
        transfer_id: 0,
        observer: Arc::new(TestObserver {}),
        cancel_token: CancellationToken::new(),
        staging,
    };
    let receive_session = ReceiveSession::new(tx, ignition);

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
    fn on_chunk_transferred(&self, _transfer_id: Option<u32>, _bytes: u64) {
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
    };
    let send_session = SendSession::new(metadata, &source_path)?;

    let (tx, rx) = mpsc::channel::<ChunkPacket>(16);
    let instruction = JobInstruction::new(send_session.get_metadata());
    let cancel_token = CancellationToken::new();

    let staging = make_staging(dest_dir.path(), None);
    let part_path = staging.part_path("big.bin");

    let (obs_tx, mut obs_rx) = mpsc::channel(1);
    let observer = Arc::new(ChunkSignalObserver { tx: obs_tx });

    let ignition = IgnitionPayload {
        ins: instruction,
        rx,
        transfer_id: 0,
        observer,
        cancel_token: cancel_token.clone(),
        staging,
    };
    let receive_session = ReceiveSession::new(tx, ignition);

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
