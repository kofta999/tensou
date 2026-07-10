use crate::{
    ChunkIndex, FileId, MAX_CONCURRENT_STREAMS, MAX_METADATA_SIZE,
    crypto::SkipServerVerification,
    disk::SendSession,
    protocol::{self, SenderInfo, State, TransferObserver, TransferPayload, TransferRequest},
};
use quinn::{ClientConfig, Endpoint, crypto::rustls::QuicClientConfig};
use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub struct Sender {
    connection: quinn::Connection,
    sessions: HashMap<FileId, Arc<SendSession>>,
    remote_states: Vec<State>,
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
    ) -> anyhow::Result<Option<Self>> {
        log::info!("Preparing transfer manifest for: {:?}", send_type);

        let (request, sessions) = match send_type {
            SendType::Files(paths) => {
                let res = protocol::manifest::build(paths)?;
                (
                    TransferRequest {
                        payload: TransferPayload::File(res.0),
                        sender: sender_info,
                    },
                    Some(res.1),
                )
            }
            SendType::Text(content) => (
                TransferRequest {
                    payload: TransferPayload::Text(content),
                    sender: sender_info,
                },
                None,
            ),
        };

        let client_cfg = Self::configure_client()?;
        let bind_addr: SocketAddr = "0.0.0.0:0".parse()?;
        let mut endpoint = Endpoint::client(bind_addr)?;
        endpoint.set_default_client_config(client_cfg);

        log::info!("Connecting to remote receiver at {}...", server_addr);
        let connection = endpoint.connect(server_addr, "localhost")?.await?;
        log::debug!("QUIC connection established with {}", server_addr);

        let (mut send, mut recv) = connection.open_bi().await?;
        let buf = rmp_serde::to_vec(&request)?;

        log::debug!("Sending manifest metadata...");
        send.write_all(&buf).await?;
        send.finish()?;

        log::debug!("Waiting for transfer consent response...");
        let is_accepted = match recv.read_u8().await {
            Ok(val) => val,
            Err(e) => {
                if let Some(quinn::ConnectionError::ApplicationClosed(app_close)) =
                    connection.close_reason()
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
        let buf = recv.read_to_end(MAX_METADATA_SIZE as usize).await?;
        let remote_states: Vec<State> = rmp_serde::from_slice(&buf)?;
        log::debug!("Successfully loaded remote transfer state.");

        Ok(Some(Self {
            connection,
            sessions,
            remote_states,
            cancel_token,
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

    pub async fn process_chunks(self, observer: Arc<dyn TransferObserver>) -> anyhow::Result<()> {
        let task_list = self.flatten();
        let chunk_count = task_list.len();

        log::debug!("Preparing to transmit {} chunks...", chunk_count);

        let mut send = self.connection.open_uni().await?;
        send.write_all(&rmp_serde::to_vec(&chunk_count)?).await?;
        send.finish()?;

        let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_STREAMS.into()));
        let mut join_set = tokio::task::JoinSet::new();

        for (file_id, chunk_id) in task_list {
            let permit = semaphore.clone().acquire_owned().await?;
            let session = self
                .sessions
                .get(&file_id)
                .expect("file_id from flatten() missing in sessions");
            let token_clone = self.cancel_token.clone();
            let session_clone = session.clone();
            let conn_clone = self.connection.clone();
            let observer_clone = observer.clone();

            join_set.spawn(async move {
                tokio::select! {
                    _ = token_clone.cancelled() => {
                        return anyhow::Ok(())
                    }

                    stream_result = async {
                        let mut stream = conn_clone.clone().open_uni().await?;
                        let _permit = permit;

                        let (header, buf) = session_clone.get_chunk(chunk_id).await?;
                        let buf_len = buf.len() as u64;

                        let header_bytes = rmp_serde::to_vec(&header)?;
                        let header_len = header_bytes.len() as u32;

                        stream.write_u32(header_len).await?;
                        stream.write_all(&header_bytes).await?;

                        stream.write_all(&buf).await?;
                        drop(buf);

                        stream.finish()?;

                        observer_clone.on_chunk_transferred(None, buf_len);

                        anyhow::Ok(())
                    } => {
                        stream_result?
                    }
                }

                anyhow::Ok(())
            });
        }

        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    log::error!("Chunk transmission failed: {e:?}");
                    return Err(e);
                }
                Err(e) => {
                    log::error!("Chunk task panicked: {e:?}");
                    return Err(e.into());
                }
            }
        }

        if self.cancel_token.is_cancelled() {
            log::info!("Transfer cancelled by sender.");
            self.connection.close(1u32.into(), b"Cancelled by sender");
        } else {
            log::info!("All chunks transmitted successfully. Closing connection...");
            self.connection.closed().await;
        }

        Ok(())
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
