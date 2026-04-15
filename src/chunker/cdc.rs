//! Content-Defined Chunking (CDC) implementation using FastCDC.
//!
//! Unlike fixed-size chunking, CDC places cut points based on the content
//! itself, so small insertions or deletions only affect nearby chunk
//! boundaries.  This gives much better deduplication for data that shifts.
//!
//! FastCDC can do this on a stream without reading the entire thing, but I need to implement that
//! TODO implement FastCDC without buffering entire file
use std::pin::Pin;

use bytes::Bytes;
use fastcdc::v2020::FastCDC;
use futures::Stream;
use futures::stream::try_unfold;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::{
    error::{Error, Result},
    types::Chunk,
};

use super::Chunker;

pub struct CdcChunker {
    min_size: u32,
    avg_size: u32,
    max_size: u32,
}

impl CdcChunker {
    pub fn new(min_size: u32, avg_size: u32, max_size: u32) -> Result<Self> {
        if min_size == 0 || avg_size == 0 || max_size == 0 {
            return Err(Error::Internal("chunk sizes must be > 0".to_string()));
        }
        if min_size > avg_size || avg_size > max_size {
            return Err(Error::Internal(
                "chunk sizes must satisfy min <= avg <= max".to_string(),
            ));
        }
        Ok(Self {
            min_size,
            avg_size,
            max_size,
        })
    }
}

impl Chunker for CdcChunker {
    fn chunk<'a>(
        &'a self,
        stream: &'a mut (dyn AsyncRead + Unpin + Send),
    ) -> Pin<Box<dyn Stream<Item = Result<Chunk>> + Send + 'a>> {
        Box::pin(try_unfold(
            (Some(stream), Vec::<Chunk>::new(), 0usize),
            move |(mut stream_opt, mut chunks, idx)| async move {
                if chunks.is_empty()
                    && let Some(stream) = stream_opt.take()
                {
                    let mut buf = Vec::new();
                    stream
                        .read_to_end(&mut buf)
                        .await
                        .map_err(|err| Error::Internal(format!("failed reading stream: {err}")))?;

                    let cdc = FastCDC::new(&buf, self.min_size, self.avg_size, self.max_size);
                    chunks = cdc
                        .map(|entry| {
                            let data = Bytes::copy_from_slice(
                                &buf[entry.offset..entry.offset + entry.length],
                            );
                            let hash = blake3::hash(&data);
                            Chunk {
                                hash: *hash.as_bytes(),
                                data,
                                offset: entry.offset as u64,
                                length: entry.length as u32,
                            }
                        })
                        .collect();
                }

                if idx >= chunks.len() {
                    return Ok(None);
                }

                let chunk = chunks[idx].clone();
                Ok(Some((chunk, (None, chunks, idx + 1))))
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;
    use tokio::io::BufReader;

    use crate::{chunker::Chunker, types::Chunk};

    use super::CdcChunker;

    const MIN: u32 = 64;
    const AVG: u32 = 256;
    const MAX: u32 = 1024;

    #[tokio::test]
    async fn chunk_should_cover_entire_input() {
        let chunker = CdcChunker::new(MIN, AVG, MAX).unwrap();
        let payload: Vec<u8> = (0..2048).map(|i| (i % 251) as u8).collect();
        let mut reader = BufReader::new(payload.as_slice());

        let chunks: Vec<Chunk> = chunker.chunk(&mut reader).try_collect().await.unwrap();

        let total: u64 = chunks.iter().map(|c| c.length as u64).sum();
        assert_eq!(total, payload.len() as u64);
    }

    #[tokio::test]
    async fn chunk_offsets_should_be_contiguous() {
        let chunker = CdcChunker::new(MIN, AVG, MAX).unwrap();
        let payload: Vec<u8> = (0..4096).map(|i| (i % 199) as u8).collect();
        let mut reader = BufReader::new(payload.as_slice());

        let chunks: Vec<Chunk> = chunker.chunk(&mut reader).try_collect().await.unwrap();

        for (i, pair) in chunks.windows(2).enumerate() {
            assert_eq!(
                pair[0].offset + pair[0].length as u64,
                pair[1].offset,
                "gap between chunk {i} and {i}+1"
            );
        }
    }

    #[tokio::test]
    async fn chunk_should_be_deterministic() {
        let chunker = CdcChunker::new(MIN, AVG, MAX).unwrap();
        let payload: Vec<u8> = (0..2048).map(|i| (i % 137) as u8).collect();

        let mut reader_a = BufReader::new(payload.as_slice());
        let a: Vec<Chunk> = chunker.chunk(&mut reader_a).try_collect().await.unwrap();

        let mut reader_b = BufReader::new(payload.as_slice());
        let b: Vec<Chunk> = chunker.chunk(&mut reader_b).try_collect().await.unwrap();

        assert_eq!(a.len(), b.len());
        for (ca, cb) in a.iter().zip(b.iter()) {
            assert_eq!(ca.hash, cb.hash);
            assert_eq!(ca.offset, cb.offset);
            assert_eq!(ca.length, cb.length);
        }
    }

    #[tokio::test]
    async fn chunk_respects_size_bounds() {
        let chunker = CdcChunker::new(MIN, AVG, MAX).unwrap();
        // large enough payload that no chunk is the "last remainder" edge case
        let payload: Vec<u8> = (0..16384).map(|i| (i % 251) as u8).collect();
        let mut reader = BufReader::new(payload.as_slice());

        let chunks: Vec<Chunk> = chunker.chunk(&mut reader).try_collect().await.unwrap();

        // all chunks except possibly the last must be >= min_size
        for chunk in &chunks[..chunks.len().saturating_sub(1)] {
            assert!(
                chunk.length >= MIN,
                "chunk length {} < min {}",
                chunk.length,
                MIN
            );
        }
        // all chunks must be <= max_size
        for chunk in &chunks {
            assert!(
                chunk.length <= MAX,
                "chunk length {} > max {}",
                chunk.length,
                MAX
            );
        }
    }

    #[tokio::test]
    async fn chunk_single_byte_input() {
        let chunker = CdcChunker::new(MIN, AVG, MAX).unwrap();
        let payload = vec![42u8];
        let mut reader = BufReader::new(payload.as_slice());

        let chunks: Vec<Chunk> = chunker.chunk(&mut reader).try_collect().await.unwrap();

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].length, 1);
        assert_eq!(chunks[0].offset, 0);
    }

    #[tokio::test]
    async fn chunk_empty_input() {
        let chunker = CdcChunker::new(MIN, AVG, MAX).unwrap();
        let payload: Vec<u8> = vec![];
        let mut reader = BufReader::new(payload.as_slice());

        let chunks: Vec<Chunk> = chunker.chunk(&mut reader).try_collect().await.unwrap();

        assert!(chunks.is_empty());
    }

    #[test]
    fn new_rejects_zero_sizes() {
        assert!(CdcChunker::new(0, 256, 1024).is_err());
        assert!(CdcChunker::new(64, 0, 1024).is_err());
        assert!(CdcChunker::new(64, 256, 0).is_err());
    }

    #[test]
    fn new_rejects_invalid_ordering() {
        assert!(CdcChunker::new(512, 256, 1024).is_err());
        assert!(CdcChunker::new(64, 2048, 1024).is_err());
    }
}
