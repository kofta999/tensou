use bitvec::{bitvec, order::Lsb0, vec::BitVec};
use std::{
    fs::{self, File, OpenOptions},
    os::unix::fs::{FileExt, MetadataExt},
    path::PathBuf,
};

#[derive(Clone)]
pub struct Metadata {
    pub filename: PathBuf,
    pub size: u64,
    pub chunk_size: u64,
}

#[derive(Debug)]
pub struct ChunkPacket {
    pub index: u64,
    pub hash: [u8; 32],
    pub bytes: Vec<u8>,
}

pub struct State(pub BitVec<u8, Lsb0>);

pub struct SendSession {
    metadata: Metadata,
    file_fd: File,
    total_chunks: usize,
}

impl SendSession {
    pub fn new(filename: &str, chunk_size: u64) -> anyhow::Result<Self> {
        let metadata = fs::metadata(filename)?;
        let size = metadata.size();
        let total_chunks: usize = ((size + chunk_size - 1) / chunk_size).try_into()?;
        let file_fd = OpenOptions::new().read(true).open(filename)?;

        Ok(Self {
            metadata: Metadata {
                filename: PathBuf::from(filename),
                size,
                chunk_size,
            },
            total_chunks,
            file_fd,
        })
    }

    pub fn get_metadata(&self) -> Metadata {
        self.metadata.clone()
    }

    pub fn get_total_chunks(&self) -> usize {
        self.total_chunks
    }

    pub fn get_chunk(&mut self, index: u64) -> anyhow::Result<ChunkPacket> {
        let offset = index * self.metadata.chunk_size;
        let mut buf = self.get_read_buffer(offset);

        self.file_fd.read_exact_at(&mut buf, offset)?;

        Ok(ChunkPacket {
            index,
            hash: Self::hash_chunk(&buf),
            bytes: buf.to_vec(),
        })
    }

    fn get_read_buffer(&self, offset: u64) -> Vec<u8> {
        let diff = self.metadata.size - offset;

        let size = if diff < self.metadata.chunk_size {
            diff
        } else {
            self.metadata.chunk_size
        };

        vec![0u8; size as usize]
    }

    fn hash_chunk(chunk: &[u8]) -> [u8; 32] {
        blake3::hash(chunk).into()
    }
}

pub struct ReceiveSession {
    metadata: Metadata,
    state: State,
    state_file_path: PathBuf,
    part_file_path: PathBuf,
    file_fd: File,
    total_chunks: usize,
}

impl ReceiveSession {
    pub fn new(metadata: Metadata) -> anyhow::Result<Self> {
        let total_chunks: usize =
            ((metadata.size + metadata.chunk_size - 1) / metadata.chunk_size).try_into()?;

        let mut state_file_path = metadata.filename.clone();
        state_file_path.add_extension("state");

        let mut part_file_path = metadata.filename.clone();
        part_file_path.add_extension("part");

        let state = if state_file_path.exists() {
            let state_bytes = fs::read(&state_file_path)?;

            let mut bitvec: BitVec<u8, Lsb0> = BitVec::from_vec(state_bytes);
            bitvec.truncate(total_chunks);

            State(bitvec)
        } else {
            let state = State(bitvec![u8, Lsb0; 0; total_chunks]);
            fs::write(&state_file_path, state.0.as_raw_slice())?;

            Self::allocate_sparse_file(&part_file_path, metadata.size)?;

            state
        };

        let file_fd = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&part_file_path)?;

        Ok(Self {
            state,
            state_file_path,
            part_file_path,
            file_fd,
            metadata,
            total_chunks,
        })
    }

    fn allocate_sparse_file(path: &PathBuf, size: u64) -> anyhow::Result<()> {
        let file = File::create(&path)?;
        file.set_len(size as u64)?;

        Ok(())
    }

    pub fn save_state(&self) -> anyhow::Result<()> {
        fs::write(&self.state_file_path, self.state.0.as_raw_slice())?;
        Ok(())
    }

    pub fn write_chunk(&mut self, packet: ChunkPacket) -> anyhow::Result<bool> {
        if packet.hash == Self::hash_chunk(&packet.bytes) {
            let offset = packet.index * self.metadata.chunk_size;

            self.file_fd.write_all_at(&packet.bytes, offset)?;

            self.state.0.set(packet.index as usize, true);

            // TODO: handle saving in a batch outside the chunk loop to not do an extra disk write every 4MB
            self.save_state()?;

            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn is_complete(&self) -> bool {
        self.state.0.all()
    }

    /// Must be called if is_complete == true
    pub fn commit(&mut self) -> anyhow::Result<()> {
        // No need to close file as it should implement Drop
        fs::remove_file(&self.state_file_path)?;

        fs::rename(&self.part_file_path, &self.metadata.filename)?;

        Ok(())
    }

    fn hash_chunk(chunk: &[u8]) -> [u8; 32] {
        blake3::hash(chunk).into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;
    use tempfile::tempdir;

    #[test]
    fn test_full_local_transfer() -> anyhow::Result<()> {
        // 1. Setup: Create a temporary directory
        let dir = tempdir()?;
        let source_path = dir.path().join("source.bin");
        let received_path = dir.path().join("source.rcv");

        let chunk_size = 4 * 1024 * 1024;

        // 2. Mock Data: Create a file with exactly 10MB of random data
        // (Write logic to fill `source_path` with 10MB of bytes)
        let mut buffer = vec![0u8; 10 * 1024 * 1024];
        rand::rng().fill_bytes(&mut buffer);
        fs::write(&source_path, &buffer)?;

        // 3. Initialize: Create your SendSession (chunk size 4MB)
        // (Write logic to instantiate SendSession)
        let mut send_session = SendSession::new(source_path.to_str().unwrap(), chunk_size)?;

        // 4. Initialize: Create your ReceiveSession using the sender's metadata
        // Note: Change the receiver's target path so it doesn't overwrite the source!
        // (Write logic to instantiate ReceiveSession)
        let metadata = Metadata {
            filename: received_path.clone(),
            size: send_session.get_metadata().size,
            chunk_size,
        };
        let mut receive_session = ReceiveSession::new(metadata)?;

        // 5. The Loop:
        // Iterate through the total number of chunks.
        // For each chunk: get_chunk from sender -> write_chunk to receiver.
        // Assert that write_chunk returns true (hash matched).

        for i in 0..send_session.get_total_chunks() {
            let chunk = send_session.get_chunk(i as u64)?;
            assert!(receive_session.write_chunk(chunk)?);
        }

        // 6. The Commit:
        // Assert that receive_session.is_complete() is true.
        // Call receive_session.commit()

        assert!(receive_session.is_complete());
        receive_session.commit()?;

        // 7. The Final Verification:
        // Read `source.bin` and the final received file into memory.
        // Assert that they are exactly equal.

        assert!(file_diff::diff(
            source_path.to_str().unwrap(),
            received_path.to_str().unwrap()
        ));

        Ok(())
    }
}
