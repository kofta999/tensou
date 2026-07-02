use crate::{
    CHUNK_SIZE, FileId, MAX_CONCURRENT_STREAMS, MAX_METADATA_SIZE, QUIC_RECEIVE_WINDOW,
    QUIC_STREAM_RECEIVE_WINDOW,
    config::Config,
    discovery,
    disk::{IgnitionPayload, ReceiveSession, TransferStaging},
    protocol::{
        ChunkHeader, ChunkPacket, Manifest, ManifestManager, TransferConsentHandler,
        TransferObserver,
    },
};
use mdns_sd::ServiceDaemon;
use quinn::{Endpoint, ServerConfig, TransportConfig};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::{collections::HashMap, net::SocketAddr, path::Path, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

// Server listener
pub struct ReceiverDaemon {
    pub(super) endpoint: quinn::Endpoint,
    pub(super) config: Config,
    pub(super) _discovery_daemon: ServiceDaemon,
}

impl ReceiverDaemon {
    pub fn new(bind_addr: SocketAddr, config: Config) -> anyhow::Result<Self> {
        let server_config = Self::configure_server()?;
        let endpoint = Endpoint::server(server_config, bind_addr)?;
        let _discovery_daemon = discovery::register_service(&config)?;

        Ok(Self {
            endpoint,
            config,
            _discovery_daemon,
        })
    }

    pub fn local_addr(&self) -> anyhow::Result<SocketAddr> {
        Ok(self.endpoint.local_addr()?)
    }

    pub async fn run(
        &self,
        consent: Arc<dyn TransferConsentHandler>,
        observer: Arc<dyn TransferObserver>,
        parent_cancel_token: CancellationToken,
    ) {
        use tokio::task::JoinSet;
        use tokio::time::{Duration, timeout};

        let mut active_transfers = JoinSet::new();

        loop {
            tokio::select! {
                _ = parent_cancel_token.cancelled() =>  {
                    break;
                }

            incoming = self.endpoint.accept() => {
                let Some(incoming) = incoming else {break};

                let target_dir_clone = self.config.target_dir.clone();
                let observer_clone = observer.clone();
                let transfer_id = rand::random::<u32>();
                let consent_clone = consent.clone();
                let conn_cancel_token = parent_cancel_token.child_token();
                let config_clone = self.config.clone();

                while active_transfers.try_join_next().is_some() {}

                active_transfers.spawn(async move {
                    let connection = match timeout(Duration::from_secs(5), incoming).await {
                        Ok(Ok(conn)) => conn,
                        Ok(Err(e)) => {
                            eprintln!("Quinn connection establishment failed: {e}");
                            return anyhow::Ok(());
                        }
                        Err(_) => {
                            eprintln!("Connection handshake timed out.");
                            return anyhow::Ok(());
                        }
                    };

                    let peer = connection.remote_address();

                    match timeout(
                        Duration::from_secs(10),
                        PendingTransfer::read_manifest(connection.clone()),
                    )
                    .await
                    {
                        Ok(Ok(pending)) => {
                            let is_accepted = consent_clone
                                .request_consent(peer, &pending.manifest.job_name)
                                .await;

                            if is_accepted {
                                let receiver = pending
                                    .accept(
                                        &target_dir_clone,
                                        observer_clone.clone(),
                                        transfer_id,
                                        conn_cancel_token.clone(),
                                        config_clone.overwrite_dest
                                    )
                                    .await?;

                                observer_clone.on_transfer_started(
                                    transfer_id,
                                    peer,
                                    receiver.total_size,
                                    &receiver.job_name,
                                    conn_cancel_token.clone()
                                );

                                tokio::select!{
                                    _ = conn_cancel_token.cancelled() =>  {
                                        connection.close(0u32.into(), b"Cancelled by receiver");
                                        observer_clone.on_transfer_failed(transfer_id, "Cancelled locally");
                                    }

                                    err = connection.closed() => {
                                        if let quinn::ConnectionError::ApplicationClosed(app_close) = &err {
                                            let reason = String::from_utf8_lossy(&app_close.reason);
                                            observer_clone.on_transfer_failed(transfer_id, &reason);
                                        } else {
                                            observer_clone.on_transfer_failed(transfer_id, &err.to_string());
                                        }
                                    }

                                    res = receiver.process_chunks(conn_cancel_token.clone()) => {
                                        match res {
                                            Ok(_) => {
                                                observer_clone.on_transfer_complete(transfer_id);
                                            }
                                            Err(e) => {
                                                // Tell the UI about the error instead of silently aborting!
                                                observer_clone.on_transfer_failed(transfer_id, &e.to_string());
                                            }
                                        }
                                    }
                                }

                            } else {
                                println!("Transfer from {} was rejected.", peer);
                                pending.reject().await?;
                            }
                        }
                        Ok(Err(e)) => {
                            eprintln!("Handshake manifest read failed! {e}");
                        }
                        Err(_) => {
                            eprintln!("Timed out waiting for manifest from {peer}");
                        }
                    };

                    anyhow::Ok(())
                });
            }}
        }

        println!("Waiting for active transfers to safely flush to disk...");
        while active_transfers.join_next().await.is_some() {}
    }

    fn configure_server() -> anyhow::Result<ServerConfig> {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
        let cert_der = CertificateDer::from(cert.cert);
        let priv_key = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());

        let mut transport_config = TransportConfig::default();
        transport_config.max_concurrent_uni_streams(MAX_CONCURRENT_STREAMS.into());
        transport_config.stream_receive_window(QUIC_STREAM_RECEIVE_WINDOW.into());
        transport_config.receive_window(QUIC_RECEIVE_WINDOW.into());

        let mut server_config =
            ServerConfig::with_single_cert(vec![cert_der.clone()], priv_key.into())?;
        server_config.transport = Arc::new(transport_config);

        Ok(server_config)
    }
}

pub(super) struct PendingTransfer {
    pub(super) connection: quinn::Connection,
    pub(super) send_stream: quinn::SendStream,
    pub(super) manifest: Manifest,
}

impl PendingTransfer {
    pub async fn read_manifest(connection: quinn::Connection) -> anyhow::Result<Self> {
        let (send_stream, mut recv_stream) = connection.accept_bi().await?;

        let buf = recv_stream.read_to_end(MAX_METADATA_SIZE as usize).await?;
        let manifest: Manifest = rmp_serde::from_slice(&buf)?;

        Ok(Self {
            connection,
            manifest,
            send_stream,
        })
    }

    pub async fn accept(
        mut self,
        target_dir: &Path,
        observer: Arc<dyn TransferObserver>,
        transfer_id: u32,
        cancel_token: CancellationToken,
        overwrite: bool,
    ) -> anyhow::Result<Receiver> {
        self.send_stream.write_u8(1).await?;

        let is_single_file =
            self.manifest.files.len() == 1 && self.manifest.files[0].relative_path.is_empty();

        let staging = Arc::new(TransferStaging::new(
            target_dir.to_path_buf(),
            &self.manifest.job_name,
            overwrite,
            is_single_file,
        ));
        let job_name = staging.job_name().to_string();

        let staging_clone = staging.clone();
        tokio::task::spawn_blocking(move || staging_clone.prepare()).await??;

        let staging_clone = staging.clone();
        let instructions = tokio::task::spawn_blocking(move || {
            ManifestManager::parse(self.manifest, staging_clone.clone())
        })
        .await??;

        let (total_remaining_size, states) =
            instructions
                .iter()
                .fold((0, Vec::new()), |(total, mut states), ins| {
                    states.push(&ins.state);
                    (total + ins.remaining_bytes, states)
                });

        let state_buf = rmp_serde::to_vec(&states)?;

        self.send_stream.write_all(&state_buf).await?;
        self.send_stream.finish()?;

        let mut sessions = HashMap::new();

        for ins in instructions.into_iter() {
            let (tx, rx) = tokio::sync::mpsc::channel::<ChunkPacket>(MAX_CONCURRENT_STREAMS.into());
            let file_id = ins.metadata.file_id;

            let payload = IgnitionPayload {
                ins,
                observer: observer.clone(),
                rx,
                transfer_id,
                cancel_token: cancel_token.clone(),
                staging: staging.clone(),
            };

            sessions.insert(file_id, Arc::new(ReceiveSession::new(tx, payload)));
        }

        Ok(Receiver {
            connection: self.connection,
            total_size: total_remaining_size,
            job_name,
            sessions: Arc::new(sessions),
            staging: staging.clone(),
        })
    }

    pub async fn reject(mut self) -> anyhow::Result<()> {
        self.send_stream.write_u8(0).await?;
        self.send_stream.finish()?;
        self.connection
            .close(0u32.into(), b"Transfer rejected by user");
        Ok(())
    }
}

pub struct Receiver {
    pub(super) connection: quinn::Connection,
    pub(super) total_size: u64,
    pub(super) job_name: String,
    pub(super) sessions: Arc<HashMap<FileId, Arc<ReceiveSession>>>,
    pub(super) staging: Arc<TransferStaging>,
}

impl Receiver {
    pub async fn process_chunks(self, cancel_token: CancellationToken) -> anyhow::Result<()> {
        let mut join_set = tokio::task::JoinSet::new();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_STREAMS.into()));

        let mut recv = self.connection.accept_uni().await?;
        let buf = recv.read_to_end(64).await?;
        let chunk_count: usize = rmp_serde::from_slice(&buf)?;

        for _ in 0..chunk_count {
            let mut chunk_stream = self.connection.accept_uni().await?;
            let sessions_clone = self.sessions.clone();
            let permit = semaphore.clone().acquire_owned().await?;
            let cancel_clone = cancel_token.clone();

            join_set.spawn(async move {
                // To capture ownership and drop on task finish (to decrease semaphore count)
                let _permit = permit;

                tokio::select! {
                    _ = cancel_clone.cancelled() => {
                        return anyhow::Ok(());
                    }
                    res = async {
                        let header_len = chunk_stream.read_u32().await?;
                        let mut header_buf = vec![0u8; header_len as usize];
                        chunk_stream.read_exact(&mut header_buf).await?;
                        let header: ChunkHeader = rmp_serde::from_slice(&header_buf)?;

                        let data_buf = chunk_stream.read_to_end(CHUNK_SIZE as usize).await?;
                        let session = sessions_clone
                            .get(&header.file_id)
                            .ok_or_else(|| anyhow::anyhow!("Invalid file_id from client"))?;

                        session.write_chunk(header, data_buf).await?;

                        anyhow::Ok(())
                    } => res?
                };

                anyhow::Ok(())
            });
        }

        let mut has_error = false;

        // Handle Network errors
        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if !cancel_token.is_cancelled() {
                        eprintln!("Chunk error: {e:?}");
                        has_error = true;
                    }
                }
                Err(e) => {
                    if !cancel_token.is_cancelled() {
                        eprintln!("Task panic: {e:?}");
                        has_error = true;
                    }
                }
            }
        }

        let handles: Vec<_> = self
            .sessions
            .values()
            .filter_map(|s| s.writer_handle.lock().unwrap().take())
            .collect();

        // To close each tx associated with session
        drop(self.sessions);

        // Handle Disk errors
        for handle in handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if !cancel_token.is_cancelled() {
                        eprintln!("Writer failed: {e:?}");
                        has_error = true;
                    }
                }
                Err(e) => {
                    if !cancel_token.is_cancelled() {
                        eprintln!("Writer task panicked: {e:?}");
                        has_error = true;
                    }
                }
            }
        }

        if has_error || cancel_token.is_cancelled() {
            anyhow::bail!("Transfer failed or was cancelled");
        }

        tokio::task::spawn_blocking(move || {
            let _ = self.staging.cleanup();
        })
        .await?;

        self.connection.close(0u32.into(), b"Transfer Complete");

        anyhow::Ok(())
    }
}
