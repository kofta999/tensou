use crate::protocol::{ChunkHeader, Metadata};
use std::path::{Path, PathBuf};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};

#[derive(Debug)]
pub struct SendSession {
    metadata: Metadata,
    total_chunks: usize,
    full_path: PathBuf,
}

impl SendSession {
    pub fn new(metadata: Metadata, full_path: &Path) -> anyhow::Result<Self> {
        let total_chunks: usize = metadata.size.div_ceil(metadata.chunk_size).try_into()?;

        Ok(Self {
            metadata,
            total_chunks,
            full_path: full_path.to_path_buf(),
        })
    }

    pub fn get_metadata(&self) -> Metadata {
        self.metadata.clone()
    }

    pub fn get_total_chunks(&self) -> usize {
        self.total_chunks
    }

    pub fn get_chunk_size(&self, index: u64) -> u64 {
        self.metadata.get_chunk_size(index)
    }

    pub async fn get_chunk(&self, index: u64) -> anyhow::Result<(ChunkHeader, Vec<u8>)> {
        let offset = index * self.metadata.chunk_size;
        let mut buf = self.get_read_buffer(index);

        let mut fd = File::open(&self.full_path).await?;
        fd.seek(std::io::SeekFrom::Start(offset)).await?;
        fd.read_exact(&mut buf).await?;

        Ok((
            ChunkHeader {
                file_id: self.metadata.file_id,
                index,
                hash: ChunkHeader::hash_chunk(&buf),
            },
            buf,
        ))
    }

    fn get_read_buffer(&self, index: u64) -> Vec<u8> {
        vec![0u8; self.metadata.get_chunk_size(index) as usize]
    }
}
