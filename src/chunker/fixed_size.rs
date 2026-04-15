//! Fixed-size chunker implementation.
//!
//! Splits the input into `chunk_size`-byte pieces (the last chunk may be smaller,
//! you theoretically could end up a tiny 1 byte chunk at the end but good enoug for now).
//! Each chunk is hashed with BLAKE3.
//!
//! Chunks are yielded one at a time via a `Stream`, so memory for previous
//! chunks is freed as soon as the consumer drops them.

use std::pin::Pin;

use bytes::BytesMut;
use futures::Stream;
use futures::stream::try_unfold;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::{
    error::{Error, Result},
    types::Chunk,
};

use super::Chunker;

pub struct FixedSizeChunker {
    chunk_size: usize,
}

impl FixedSizeChunker {
    pub fn new(chunk_size: usize) -> Result<Self> {
        if chunk_size == 0 {
            return Err(Error::Internal("chunk_size must be > 0".to_string()));
        }
        Ok(Self { chunk_size })
    }
}

impl Chunker for FixedSizeChunker {
    fn chunk<'a>(
        &'a self,
        stream: &'a mut (dyn AsyncRead + Unpin + Send),
    ) -> Pin<Box<dyn Stream<Item = Result<Chunk>> + Send + 'a>> {
        Box::pin(try_unfold(
            (stream, 0u64),
            move |(stream, offset)| async move {
                let mut buffer = BytesMut::with_capacity(self.chunk_size);

                while buffer.len() < self.chunk_size {
                    let read = stream
                        .read_buf(&mut buffer)
                        .await
                        .map_err(|err| Error::Internal(format!("failed reading stream: {err}")))?;
                    if read == 0 {
                        break;
                    }
                }

                if buffer.is_empty() {
                    return Ok(None);
                }

                let data = buffer.freeze();
                let hash = blake3::hash(&data);
                let len = data.len();
                let chunk = Chunk {
                    hash: *hash.as_bytes(),
                    data,
                    offset,
                    length: len as u32,
                };
                Ok(Some((chunk, (stream, offset + len as u64))))
            },
        ))
    }
}
