use async_trait::async_trait;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::io::Write;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use tensou_core::config::Config;
use tensou_core::net::{ReceiverDaemon, SendType, Sender};
use tensou_core::protocol::{SenderInfo, TransferConsentHandler, TransferMode, TransferObserver};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

struct Fixture {
    name: &'static str,
    size: u64,
    count: u64,
    chunk_size: u64,
}

const FIXTURES: &[Fixture] = &[
    Fixture {
        name: "1GB_Single",
        chunk_size: 4 * 1024 * 1024,
        count: 1,
        size: 1024 * 1024 * 1024,
    },
    Fixture {
        name: "4KB_small_10K",
        chunk_size: 4 * 1024,
        count: 10_000,
        size: 4 * 1024,
    },
    Fixture {
        name: "4KB_small_100K",
        chunk_size: 4 * 1024,
        count: 100_000,
        size: 4 * 1024,
    },
    Fixture {
        name: "1K_medium_1MB",
        chunk_size: 1024 * 1024,
        count: 1000,
        size: 1024 * 1024,
    },
];

struct AutoAccept;

#[async_trait]
impl TransferConsentHandler for AutoAccept {
    async fn request_consent(
        &self,
        _peer: SocketAddr,
        _sender_info: &SenderInfo,
        _job_name: &str,
    ) -> bool {
        true
    }
}

struct TestObserver;
impl TransferObserver for TestObserver {}

fn make_sender_info() -> SenderInfo {
    SenderInfo {
        display_name: "BenchSender".to_string(),
        device_uuid: "bench-uuid-123".to_string(),
        os_type: "linux".to_string(),
    }
}

fn make_config(dest: &Path) -> Arc<Mutex<Config>> {
    Arc::new(Mutex::new(Config {
        target_dir: dest.to_path_buf(),
        transfer_mode: TransferMode::Overwrite,
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
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (addr, handle)
}

fn create_test_file(path: &Path, size_bytes: u64, chunk_size: usize) {
    let mut file = std::fs::File::create(path).unwrap();
    let chunk = vec![0xABu8; chunk_size];
    let mut remaining = size_bytes;

    while remaining > 0 {
        let to_write = (remaining as usize).min(chunk_size);
        file.write_all(&chunk[..to_write]).unwrap();
        remaining -= to_write as u64;
    }
    file.flush().unwrap();
}

fn benchmark_transfer(c: &mut Criterion) {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("transfer");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(60));

    for fixture in FIXTURES {
        let source_dir = tempfile::tempdir().unwrap();

        for i in 0..fixture.count {
            let source_path = source_dir.path().join(format!("{i}_{}.bin", fixture.name));
            create_test_file(&source_path, fixture.size, fixture.chunk_size as usize);
        }

        group.throughput(Throughput::Bytes(fixture.count * fixture.size));

        group.bench_function(fixture.name, |b| {
            b.iter_custom(|iters| {
                let mut total_duration = std::time::Duration::ZERO;

                for _ in 0..iters {
                    let dest_dir = tempfile::tempdir().unwrap();
                    let (addr, server_handle) = rt.block_on(spawn_daemon(
                        make_config(dest_dir.path()),
                        Arc::new(AutoAccept),
                        Arc::new(TestObserver),
                    ));

                    let start = std::time::Instant::now();
                    rt.block_on(async {
                        let mut client = Sender::connect(
                            addr,
                            SendType::Files(&[source_dir.path().to_path_buf()]),
                            make_sender_info(),
                            CancellationToken::new(),
                            Uuid::new_v4(),
                        )
                        .await
                        .unwrap()
                        .unwrap();

                        client
                            .process_chunks(
                                Arc::new(TestObserver),
                                Arc::new(AtomicBool::new(false)),
                            )
                            .await
                            .unwrap();
                    });
                    total_duration += start.elapsed();

                    server_handle.abort();
                }

                total_duration
            });
        });
    }

    group.finish();
}

fn benchmark_disk_baseline(c: &mut Criterion) {
    let data = vec![0xABu8; 4096];
    let mut group = c.benchmark_group("disk_baseline");
    group.sample_size(10);

    group.bench_function("10K_raw_creates_tmpfs", |b| {
        b.iter(|| {
            let dir = tempfile::tempdir().unwrap();
            for i in 0..10_000u64 {
                let path = dir.path().join(format!("file_{i}.bin"));
                std::fs::write(&path, &data).unwrap();
            }
        });
    });

    group.finish();
}

criterion_group!(benches, benchmark_transfer, benchmark_disk_baseline);
criterion_main!(benches);
