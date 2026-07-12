use crate::{
    ChunkIndex, FileId, MAX_CONCURRENT_STREAMS, MAX_REQUEST_SIZE,
    disk::SendSession,
    net::{connection_manager::ConnectionManager, is_connection_error},
    protocol::{self, SenderInfo, State, TransferObserver, TransferPayload, TransferRequest},
};
use std::{
    collections::HashMap,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, atomic::AtomicBool},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Debug)]
pub struct Sender {
    connection_manager: ConnectionManager,
    sessions: HashMap<FileId, Arc<SendSession>>,
    remote_states: Vec<State>,
    request: TransferRequest,
    pub transfer_id: Uuid,
    pub cancel_token: CancellationToken,
}

#[derive(Debug)]
pub enum SendType<'a> {
    Files(&'a [PathBuf]),
    Text(String),
}

impl Sender {
    pub async fn connect(
        server_addr: SocketAddr,
        send_type: SendType<'_>,
        sender_info: SenderInfo,
        cancel_token: CancellationToken,
        transfer_id: uuid::Uuid,
    ) -> anyhow::Result<Option<Self>> {
        log::info!("Preparing transfer manifest for: {:?}", send_type);

        let (request, sessions) = match send_type {
            SendType::Files(paths) => {
                let res = protocol::manifest::build(paths)?;
                (
                    TransferRequest {
                        transfer_id,
                        payload: TransferPayload::File(res.0),
                        sender: sender_info,
                    },
                    Some(res.1),
                )
            }
            SendType::Text(content) => (
                TransferRequest {
                    transfer_id,
                    payload: TransferPayload::Text(content),
                    sender: sender_info,
                },
                None,
            ),
        };

        let connection_manager = ConnectionManager::connect(server_addr).await?;
        let (mut send, mut recv) = connection_manager.open_bi().await?;
        let buf = rmp_serde::to_vec(&request)?;

        log::debug!("Sending manifest metadata...");
        send.write_all(&buf).await?;
        send.finish()?;

        log::debug!("Waiting for transfer consent response...");
        let is_accepted = match recv.read_u8().await {
            Ok(val) => val,
            Err(e) => {
                if let Some(quinn::ConnectionError::ApplicationClosed(app_close)) =
                    connection_manager.connection.close_reason()
                {
                    let reason = String::from_utf8_lossy(&app_close.reason);
                    log::warn!("Transfer request rejected by remote: {}", reason);
                    anyhow::bail!("The receiver rejected your transfer request: {}", reason);
                }
                return Err(e.into());
            }
        };

        if is_accepted == 0 {
            log::warn!("Transfer request rejected by remote user.");
            anyhow::bail!("The receiver rejected your transfer request.");
        }

        let sessions = match sessions {
            Some(s) => s,
            None => return Ok(None),
        };

        log::info!("Transfer accepted. Reading remote states...");
        let buf = recv.read_to_end(MAX_REQUEST_SIZE as usize).await?;
        let remote_states: Vec<State> = rmp_serde::from_slice(&buf)?;
        log::debug!("Successfully loaded remote transfer state.");

        Ok(Some(Self {
            connection_manager,
            sessions,
            remote_states,
            request,
            cancel_token,
            transfer_id,
        }))
    }

    pub fn get_remaining_bytes(&self) -> u64 {
        let mut total = 0;
        for (file_id, chunk_idx) in self.flatten() {
            if let Some(session) = self.sessions.get(&file_id) {
                total += session.get_chunk_size(chunk_idx);
            }
        }
        total
    }

    pub fn get_total_bytes(&self) -> u64 {
        self.sessions.values().map(|s| s.get_metadata().size).sum()
    }

    pub fn get_bytes_done(&self) -> u64 {
        self.get_total_bytes()
            .saturating_sub(self.get_remaining_bytes())
    }

    pub fn get_connection(&self) -> quinn::Connection {
        self.connection_manager.connection()
    }

    pub fn close_with_error(&self, err: &protocol::TransferError) {
        self.connection_manager.close_with(err.to_code(), b"");
    }

    pub fn close_successfully(&self) {
        self.connection_manager.close_with(0, b"");
    }

    /// Outer loop: stream chunks, reconnect on connection loss, re-derive remaining work.
    pub async fn process_chunks(
        &mut self,
        observer: Arc<dyn TransferObserver>,
        is_paused: Arc<AtomicBool>,
    ) -> anyhow::Result<()> {
        log::info!("Sender: starting process_chunks loop");
        loop {
            // Re-dervied from remote_states bitvec on reconnection
            let task_list = self.flatten();
            log::info!(
                "Sender: flatten() returned {} chunks to transfer",
                task_list.len()
            );
            if log::log_enabled!(log::Level::Debug) {
                log::debug!("Sender: remaining task chunks: {:?}", task_list);
            }

            match self.stream_chunks(task_list, &observer).await {
                Ok(()) => {
                    log::info!("Sender: stream_chunks completed successfully, breaking loop");
                    break;
                }
                Err(e) if self.cancel_token.is_cancelled() => {
                    let err = if is_paused.load(std::sync::atomic::Ordering::Relaxed) {
                        log::info!("Sender: transfer paused (connection loss)");
                        protocol::TransferError::ConnectionLoss
                    } else {
                        log::info!("Sender: transfer cancelled");
                        protocol::TransferError::Cancelled
                    };
                    self.close_with_error(&err);
                    return Err(e);
                }
                Err(e) if is_connection_error(&e) => {
                    log::warn!("Sender: connection error encountered: {:?}. Attempting reconnect...", e);
                    self.reconnect_and_resync(&observer).await?;
                }
                Err(e) => {
                    log::error!("Sender: fatal error in stream_chunks: {:?}", e);
                    return Err(e);
                }
            }
        }

        log::info!("Sender: closing connection gracefully");
        self.connection_manager.close_gracefully().await;
        log::info!("Sender: process_chunks finished successfully");
        Ok(())
    }

    /// Inner dispatch: sends chunk count header, then slides a fixed-size JoinSet window.
    async fn stream_chunks(
        &self,
        tasks: Vec<(FileId, ChunkIndex)>,
        observer: &Arc<dyn TransferObserver>,
    ) -> anyhow::Result<()> {
        // Tell receiver how many chunks to expect in this batch
        log::info!("Sender: opening uni stream to send chunk count ({})", tasks.len());
        let mut header = self.connection_manager.open_uni().await?;
        header.write_all(&rmp_serde::to_vec(&tasks.len())?).await?;
        header.finish()?;
        log::debug!("Sender: chunk count header sent successfully");

        let mut tasks = tasks.into_iter();
        let mut in_flight = JoinSet::new();

        // Fill initial window
        for _ in 0..MAX_CONCURRENT_STREAMS {
            if let Some((fid, cid)) = tasks.next() {
                self.spawn_chunk(&mut in_flight, fid, cid, observer);
            }
        }

        // Slide: as each completes, spawn its replacement
        while let Some(result) = in_flight.join_next().await {
            result??; // JoinError or chunk error → propagate
            if self.cancel_token.is_cancelled() {
                anyhow::bail!("Transfer cancelled or paused");
            }
            if let Some((fid, cid)) = tasks.next() {
                self.spawn_chunk(&mut in_flight, fid, cid, observer);
            }
        }

        Ok(())
    }

    /// Clones Arc'd/Copy data and spawns a single chunk send task.
    fn spawn_chunk(
        &self,
        join_set: &mut JoinSet<anyhow::Result<()>>,
        file_id: FileId,
        chunk_id: ChunkIndex,
        observer: &Arc<dyn TransferObserver>,
    ) {
        let conn = self.connection_manager.connection();
        let session = self.sessions[&file_id].clone();
        let observer = observer.clone();
        let transfer_id = self.transfer_id;
        let cancel = self.cancel_token.clone();

        join_set.spawn(async move {
            tokio::select! {
                _ = cancel.cancelled() => Ok(()),
                result = Self::send_chunk(&conn, &session, transfer_id, chunk_id, &observer) => result,
            }
        });
    }

    async fn send_chunk(
        conn: &quinn::Connection,
        session: &SendSession,
        transfer_id: Uuid,
        chunk_id: ChunkIndex,
        observer: &Arc<dyn TransferObserver>,
    ) -> anyhow::Result<()> {
        let mut stream = conn.open_uni().await?;
        let (header, buf) = session.get_chunk(chunk_id).await?;
        let buf_len = buf.len() as u64;

        let header_bytes = rmp_serde::to_vec(&header)?;
        stream.write_u32(header_bytes.len() as u32).await?;
        stream.write_all(&header_bytes).await?;
        stream.write_all(&buf).await?;
        stream.finish()?;

        observer.on_chunk_transferred(transfer_id, buf_len);
        Ok(())
    }

    /// Reconnect and re-handshake, getting fresh remote bitvec state.
    async fn reconnect_and_resync(
        &mut self,
        observer: &Arc<dyn TransferObserver>,
    ) -> anyhow::Result<()> {
        self.connection_manager
            .reconnect(self.transfer_id, observer.as_ref(), &self.cancel_token)
            .await?;
        self.remote_states = self.resend_manifest().await?;
        Ok(())
    }

    /// Re-sends the stored TransferRequest (same UUID) and reads fresh bitvec from receiver.
    async fn resend_manifest(&self) -> anyhow::Result<Vec<State>> {
        let (mut send, mut recv) = self.connection_manager.open_bi().await?;
        let buf = rmp_serde::to_vec(&self.request)?;
        send.write_all(&buf).await?;
        send.finish()?;

        // Read consent byte (receiver auto-accepts on UUID match)
        let accepted = recv.read_u8().await?;
        if accepted == 0 {
            anyhow::bail!("Receiver rejected reconnect");
        }

        let buf = recv.read_to_end(MAX_REQUEST_SIZE as usize).await?;
        Ok(rmp_serde::from_slice(&buf)?)
    }

    fn flatten(&self) -> Vec<(FileId, ChunkIndex)> {
        let mut arr = Vec::new();

        for (file_id, session) in &self.sessions {
            for chunk_idx in 0..session.get_total_chunks() {
                if self
                    .remote_states
                    .get(*file_id)
                    .and_then(|s| s.0.get(chunk_idx))
                    .is_some_and(|v| *v)
                {
                    continue;
                }

                arr.push((*file_id, chunk_idx as u64));
            }
        }

        arr
    }
}
