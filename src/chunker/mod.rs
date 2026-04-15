//! Splits byte streams into content-addressed chunks
//!
//! This blog here is an amazing place to start : https://joshleeb.com/posts/content-defined-chunking.html
//!
//! Defines the interface for breaking large binary blobs in smaller
//! content addressable chunks

mod cdc;
mod fixed_size;

pub use self::cdc::CdcChunker;
pub use self::fixed_size::FixedSizeChunker;

use std::pin::Pin;

use futures::Stream;
use tokio::io::AsyncRead;

use crate::{error::Result, types::Chunk};

pub trait Chunker: Send + Sync {
    fn chunk<'a>(
        &'a self,
        stream: &'a mut (dyn AsyncRead + Unpin + Send),
    ) -> Pin<Box<dyn Stream<Item = Result<Chunk>> + Send + 'a>>;
}
