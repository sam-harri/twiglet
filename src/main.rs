use std::{net::SocketAddr, sync::Arc};

use axum::{http::Method, middleware};
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, fmt};
use twiglet_engine::{
    ancestry_resolver::AncestryResolver,
    chunk_store::{ChunkStore, LocalFsChunkStore, S3ChunkStore},
    chunker::FixedSizeChunker,
    config::{AppConfig, ChunkerConfig, MetastoreConfig, StorageConfig},
    engine::Engine,
    error::Error,
    http,
    lsn::LazyAtomicLsnGenerator,
    metastore::{MetadataStore, RocksDbMetadataStore},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .init();

    let config = AppConfig::from_env()?;

    info!(
        host = %config.server.host,
        port = config.server.port,
        "loaded server config"
    );

    info!(path = %match &config.metastore {
        MetastoreConfig::RocksDb(c) => &c.path,
    }, "opening metadata store");
    let metadata: Arc<dyn MetadataStore> = match config.metastore {
        MetastoreConfig::RocksDb(c) => Arc::new(RocksDbMetadataStore::try_from(c)?),
    };
    info!("metadata store ready");

    let chunk_store: Arc<dyn ChunkStore> = match config.storage {
        StorageConfig::Local(c) => {
            info!(path = %c.path, "using local filesystem chunk store");
            Arc::new(LocalFsChunkStore::from(c))
        }
        StorageConfig::S3(c) => {
            let endpoint = c.endpoint.as_deref().unwrap_or("AWS");
            info!(endpoint, bucket = %c.bucket, "using S3 chunk store");
            Arc::new(S3ChunkStore::try_from(c)?)
        }
    };

    let chunker: Arc<dyn twiglet_engine::chunker::Chunker> = match config.chunker {
        ChunkerConfig::Fixed(c) => {
            info!(chunk_size_bytes = c.chunk_size_bytes, "using fixed-size chunker");
            Arc::new(FixedSizeChunker::new(c.chunk_size_bytes)?)
        }
        ChunkerConfig::Cdc(_) => {
            return Err(Error::Internal("CDC chunker is not yet implemented".into()).into());
        }
    };

    if config.auth.username == "twigletadmin" || config.auth.password == "twigletadmin" {
        warn!(
            "using default admin credentials — set TWIGLET_AUTH__USERNAME and TWIGLET_AUTH__PASSWORD"
        );
    }

    let lsn = Arc::new(LazyAtomicLsnGenerator::new(Arc::clone(&metadata)));

    let resolver = Arc::new(AncestryResolver::new(Arc::clone(&metadata)));

    let engine = Arc::new(Engine::new(lsn, chunker, chunk_store, metadata, resolver));
    info!("engine ready");

    let auth_layer = middleware::from_fn_with_state(
        http::auth::BasicAuthConfig::from(config.auth),
        http::auth::basic_auth,
    );

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers(Any);

    let app = axum::Router::new()
        .merge(http::docs_router())
        .merge(http::router(engine).layer(auth_layer))
        .layer(TraceLayer::new_for_http())
        .layer(cors);

    let addr: SocketAddr = format!("{}:{}", config.server.host, config.server.port).parse()?;

    let listener = tokio::net::TcpListener::bind(addr).await?;

    info!(address = %listener.local_addr()?, "twiglet engine listening");

    axum::serve(listener, app).await?;

    Ok(())
}
