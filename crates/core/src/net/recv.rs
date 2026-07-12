use crate::{
    CHUNK_SIZE, FileId, MAX_CONCURRENT_STREAMS, MAX_REQUEST_SIZE, QUIC_ESTABLISH_CONN_TIMEOUT_SECS,
    QUIC_RECEIVE_WINDOW, QUIC_STREAM_RECEIVE_WINDOW, REQUEST_READ_TIMEOUT_SECS,
    config::Config,
    discovery,
    disk::{IgnitionPayload, ReceiveSession, TransferStaging},
    net::is_connection_error,
    protocol::{
        self, ChunkHeader, ChunkPacket, TransferConsentHandler, TransferError, TransferMode,
        TransferObserver, TransferPayload, TransferRequest,
    },
};
use mdns_sd::ServiceDaemon;
use quinn::{Endpoint, ServerConfig, TransportConfig};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::Path,
    sync::{Arc, Mutex},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinSet;
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

// Server listener
pub struct ReceiverDaemon {
    endpoint: quinn::Endpoint,
    config: Arc<Mutex<Config>>,
    _discovery_daemon: Arc<Mutex<ServiceDaemon>>,
}

impl ReceiverDaemon {
    pub fn new(bind_addr: SocketAddr, config: Arc<Mutex<Config>>) -> anyhow::Result<Self> {
        let server_config = Self::configure_server()?;
        let endpoint = Endpoint::server(server_config, bind_addr)?;
        let _discovery_daemon = {
            let config = config.lock().unwrap();
            discovery::register_service(&config)?
        };

        Ok(Self {
            endpoint,
            config,
            _discovery_daemon: Arc::new(Mutex::new(_discovery_daemon)),
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
        mut reload_rx: tokio::sync::mpsc::Receiver<()>,
    ) {
        let mut active_transfers: JoinSet<anyhow::Result<()>> = JoinSet::new();
        let consented_transfers: Arc<Mutex<HashSet<Uuid>>> = Default::default();

        loop {
            tokio::select! {
                _ = parent_cancel_token.cancelled() =>  {
                    break;
                }

                Some(()) = reload_rx.recv() => {
                    log::info!("Config update detected, reloading mDNS service...");
                    let config = self.config.lock().unwrap().clone();
                    if let Ok(new_daemon) = discovery::register_service(&config) {
                        *self._discovery_daemon.lock().unwrap() = new_daemon;
                    }
                }

                incoming = self.endpoint.accept() => {
                    let Some(incoming) = incoming else { break };
                    self.handle_connection(incoming, &observer, &consent, &parent_cancel_token, &mut active_transfers, consented_transfers.clone());
            }}
        }

        log::info!("Waiting for active transfers to safely flush to disk...");
        while let Some(res) = active_transfers.join_next().await {
            if let Err(e) = res {
                log::error!("Transfer handler task panicked: {e:?}");
            }
        }
    }

    fn handle_connection(
        &self,
        incoming: quinn::Incoming,
        observer: &Arc<dyn TransferObserver>,
        consent: &Arc<dyn TransferConsentHandler>,
        parent_cancel_token: &CancellationToken,
        active_transfers: &mut JoinSet<anyhow::Result<()>>,
        consented_transfers: Arc<Mutex<HashSet<Uuid>>>,
    ) {
        let target_dir_clone = {
            let config = self.config.lock().unwrap();
            config.target_dir.clone()
        };
        let observer_clone = observer.clone();
        let consent_clone = consent.clone();
        // user_cancel_token: given to the GUI/observer — only cancelled by the user explicitly.
        let user_cancel_token = parent_cancel_token.child_token();
        // work_cancel_token: child of user_cancel_token, passed into disk writers.
        // Internal failures (hash mismatch etc.) cancel this without affecting user_cancel_token,
        // so the outer select! can't be fooled into thinking the user cancelled.
        let work_cancel_token = user_cancel_token.child_token();
        let config_clone = self.config.clone();

        while let Some(res) = active_transfers.try_join_next() {
            if let Err(e) = res {
                log::error!("Transfer handler task panicked: {e:?}");
            }
        }

        let handler = async move {
            let connection = match timeout(
                Duration::from_secs(QUIC_ESTABLISH_CONN_TIMEOUT_SECS),
                incoming,
            )
            .await
            {
                Ok(Ok(conn)) => conn,
                Ok(Err(e)) => {
                    log::error!("Quinn connection establishment failed: {e}");
                    return anyhow::Ok(());
                }
                Err(_) => {
                    log::warn!("Connection handshake timed out.");
                    return anyhow::Ok(());
                }
            };

            let peer = connection.remote_address();

            match timeout(
                Duration::from_secs(REQUEST_READ_TIMEOUT_SECS),
                PendingTransfer::read_request(connection.clone()),
            )
            .await
            {
                Ok(Ok(pending)) => {
                    let auto_accept = {
                        let config = config_clone.lock().unwrap();
                        config.auto_accept
                    };

                    let already_consented = {
                        consented_transfers
                            .lock()
                            .unwrap()
                            .contains(&pending.request.transfer_id)
                    };

                    let is_accepted = if auto_accept || already_consented {
                        true
                    } else {
                        consent_clone
                            .request_consent(
                                peer,
                                &pending.request.sender,
                                pending.request.payload.job_name(),
                            )
                            .await
                    };

                    if is_accepted {
                        let mode = {
                            let config = config_clone.lock().unwrap();
                            config.transfer_mode
                        };
                        let transfer_id = pending.request.transfer_id;

                        {
                            consented_transfers.lock().unwrap().insert(transfer_id);
                        }

                        match pending
                            .accept(
                                &target_dir_clone,
                                observer_clone.clone(),
                                transfer_id,
                                work_cancel_token.clone(),
                                mode,
                            )
                            .await
                        {
                            Ok(AcceptResult::File(receiver)) => {
                                observer_clone.on_transfer_started(
                                    transfer_id,
                                    peer,
                                    receiver.total_full_size,
                                    receiver.total_full_size.saturating_sub(receiver.total_size),
                                    &receiver.job_name,
                                    user_cancel_token.clone(),
                                );

                                tokio::select! {
                                    _ = user_cancel_token.cancelled() => {
                                        connection.close(TransferError::Cancelled.to_code().into(), b"");
                                        observer_clone.on_transfer_failed(transfer_id, &TransferError::Cancelled);
                                    }

                                    res = receiver.process_chunks(work_cancel_token.clone()) => {
                                        match res {
                                            Ok(_) => {
                                                observer_clone.on_transfer_complete(transfer_id);
                                                consented_transfers.lock().unwrap().remove(&transfer_id);
                                            }
                                            Err(e) => {
                                                log::error!("Transfer chunk processing error: {e}");
                                                let classification = match tokio::time::timeout(
                                                    std::time::Duration::from_millis(200),
                                                    connection.closed(),
                                                )
                                                .await
                                                {
                                                    Ok(quinn::ConnectionError::ApplicationClosed(app_close)) => {
                                                        let code = u64::from(app_close.error_code) as u32;
                                                        TransferError::from_code(code)
                                                            .unwrap_or(TransferError::ConnectionLoss)
                                                    }
                                                    Ok(_) => TransferError::ConnectionLoss,
                                                    // Timed out or connection not yet closed — fall back
                                                    // to the original error-based classification.
                                                    Err(_) => {
                                                        if is_connection_error(&e) {
                                                            TransferError::ConnectionLoss
                                                        } else {
                                                            TransferError::Other(e.to_string())
                                                        }
                                                    }
                                                };
                                                observer_clone.on_transfer_failed(transfer_id, &classification);
                                            }
                                        }
                                    }
                                }
                            }
                            Ok(AcceptResult::Text) => {
                                observer_clone.on_transfer_complete(transfer_id);
                                let _ = connection.closed().await;

                                {
                                    consented_transfers.lock().unwrap().remove(&transfer_id);
                                }
                            }
                            Err(e) => {
                                let classification = if is_connection_error(&e) {
                                    let close_err = connection.closed().await;
                                    if let quinn::ConnectionError::ApplicationClosed(app_close) =
                                        close_err
                                    {
                                        let code = u64::from(app_close.error_code) as u32;
                                        TransferError::from_code(code)
                                            .unwrap_or(TransferError::ConnectionLoss)
                                    } else {
                                        TransferError::ConnectionLoss
                                    }
                                } else {
                                    TransferError::Other(e.to_string())
                                };
                                observer_clone.on_transfer_failed(transfer_id, &classification);

                                {
                                    consented_transfers.lock().unwrap().remove(&transfer_id);
                                }

                                return anyhow::Ok(());
                            }
                        }
                    } else {
                        log::info!("Transfer from {} was rejected.", peer);
                        pending.reject().await?;
                    }
                }
                Ok(Err(e)) => {
                    log::error!("Handshake manifest read failed! {e}");
                }
                Err(_) => {
                    log::warn!("Timed out waiting for manifest from {peer}");
                }
            };

            anyhow::Ok(())
        };

        active_transfers.spawn(handler);
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

pub enum AcceptResult {
    File(Receiver),
    Text,
}

pub(super) struct PendingTransfer {
    pub(super) connection: quinn::Connection,
    pub(super) send_stream: quinn::SendStream,
    pub(super) request: TransferRequest,
}

impl PendingTransfer {
    pub async fn read_request(connection: quinn::Connection) -> anyhow::Result<Self> {
        log::debug!(
            "Receiving handshake request from {}...",
            connection.remote_address()
        );
        let (send_stream, mut recv_stream) = connection.accept_bi().await?;

        let buf = recv_stream.read_to_end(MAX_REQUEST_SIZE as usize).await?;
        let request: TransferRequest = rmp_serde::from_slice(&buf)?;

        Ok(Self {
            connection,
            request,
            send_stream,
        })
    }

    async fn is_space_available(remaining_size: u64, path: &Path) -> bool {
        let path_clone = path.to_path_buf();

        tokio::task::spawn_blocking(move || {
            // Resolve nearest existing ancestor if the destination directory doesn't exist yet
            let check_path = path_clone
                .ancestors()
                .find(|p| p.exists())
                .unwrap_or(&path_clone);
            // Fail open (if can't check then just gamble on space)
            let available_size = fs4::available_space(check_path).unwrap_or(u64::MAX);
            remaining_size <= available_size
        })
        .await
        .unwrap_or(true)
    }

    pub async fn accept(
        mut self,
        target_dir: &Path,
        observer: Arc<dyn TransferObserver>,
        transfer_id: Uuid,
        cancel_token: CancellationToken,
        mode: TransferMode,
    ) -> anyhow::Result<AcceptResult> {
        match self.request.payload {
            TransferPayload::File(manifest) => {
                let job_name = manifest.job_name.clone();

                let target_dir_clone = target_dir.to_path_buf();
                let (instructions, staging) = tokio::task::spawn_blocking(move || {
                    protocol::manifest::parse(manifest, &target_dir_clone, mode)
                })
                .await??;

                let staging_clone = staging.clone();
                tokio::task::spawn_blocking(move || staging_clone.prepare()).await??;

                let (total_remaining_size, total_full_size, states) = instructions.iter().fold(
                    (0, 0, Vec::new()),
                    |(remaining, full, mut states), ins| {
                        states.push(&ins.state);
                        (
                            remaining + ins.remaining_bytes,
                            full + ins.metadata.size,
                            states,
                        )
                    },
                );

                if !Self::is_space_available(total_remaining_size, target_dir).await {
                    let _ = staging.cleanup();
                    let _ = self.send_stream.write_u8(0).await;
                    let _ = self.send_stream.finish();
                    self.connection.close(0u32.into(), b"DiskFull");
                    anyhow::bail!("No available space for the transfer");
                }

                self.send_stream.write_u8(1).await?;

                let state_buf = rmp_serde::to_vec(&states)?;

                self.send_stream.write_all(&state_buf).await?;
                self.send_stream.finish()?;

                let mut sessions = HashMap::new();

                for ins in instructions.into_iter() {
                    let (tx, rx) =
                        tokio::sync::mpsc::channel::<ChunkPacket>(MAX_CONCURRENT_STREAMS.into());
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

                Ok(AcceptResult::File(Receiver {
                    connection: self.connection,
                    total_size: total_remaining_size,
                    total_full_size,
                    job_name,
                    sessions: Arc::new(sessions),
                    staging: staging.clone(),
                }))
            }
            TransferPayload::Text(content) => {
                self.send_stream.write_u8(1).await?;
                self.send_stream.finish()?;
                observer.on_text_received(
                    self.connection.remote_address(),
                    self.request.sender.display_name,
                    content,
                );
                Ok(AcceptResult::Text)
            }
        }
    }

    pub async fn reject(mut self) -> anyhow::Result<()> {
        self.send_stream.write_u8(0).await?;
        self.send_stream.finish()?;
        self.connection
            .close(TransferError::Rejected.to_code().into(), b"");
        Ok(())
    }
}

pub struct Receiver {
    pub(super) connection: quinn::Connection,
    pub(super) total_size: u64,
    pub(super) total_full_size: u64,
    pub(super) job_name: String,
    pub(super) sessions: Arc<HashMap<FileId, Arc<ReceiveSession>>>,
    pub(super) staging: Arc<TransferStaging>,
}

impl Receiver {
    pub async fn process_chunks(self, cancel_token: CancellationToken) -> anyhow::Result<()> {
        log::info!("Receiver: starting process_chunks");
        let mut join_set = tokio::task::JoinSet::new();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_STREAMS.into()));

        log::debug!("Receiver: waiting to accept uni stream for chunk count");
        let mut recv = self.connection.accept_uni().await?;
        let buf = recv.read_to_end(64).await?;
        let chunk_count: usize = rmp_serde::from_slice(&buf)?;
        log::info!("Receiver: expects to receive {} chunks", chunk_count);

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

        let mut first_error: Option<anyhow::Error> = None;

        // Handle Network errors
        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if !cancel_token.is_cancelled() {
                        log::error!("Chunk error: {e:?}");
                        if first_error.is_none() {
                            first_error = Some(e);
                        }
                    }
                }
                Err(e) => {
                    if !cancel_token.is_cancelled() {
                        log::error!("Task panic: {e:?}");
                        if first_error.is_none() {
                            first_error = Some(anyhow::anyhow!("chunk task panicked: {e}"));
                        }
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
                        log::error!("Writer failed: {e:?}");
                        if first_error.is_none() {
                            first_error = Some(e);
                        }
                    }
                }
                Err(e) => {
                    if !cancel_token.is_cancelled() {
                        log::error!("Writer task panicked: {e:?}");
                        if first_error.is_none() {
                            first_error = Some(anyhow::anyhow!("writer task panicked: {e}"));
                        }
                    }
                }
            }
        }

        if cancel_token.is_cancelled() {
            anyhow::bail!("Transfer cancelled");
        }

        if let Some(e) = first_error {
            return Err(e);
        }

        tokio::task::spawn_blocking(move || {
            let _ = self.staging.cleanup();
        })
        .await?;

        self.connection.close(0u32.into(), b"");

        anyhow::Ok(())
    }
}
