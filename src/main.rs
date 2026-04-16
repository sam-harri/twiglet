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
    config::Config,
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

    let config = Config::from_env()?;

    info!(
        host = %config.host,
        port = config.port,
        chunk_store = %config.chunk_store_type,
        storage_bucket = %config.storage_bucket,
        rocksdb_path = %config.rocksdb_path,
        chunk_size_bytes = config.chunk_size_bytes,
        "loaded config"
    );

    info!(path = %config.rocksdb_path, "opening RocksDB metadata store");
    let metadata: Arc<dyn MetadataStore> = Arc::new(RocksDbMetadataStore::open(
        &config.rocksdb_path,
        config.block_cache_mb,
        config.rate_limit_mb_sec,
    )?);
    info!("metadata store ready");

    info!(backend = %config.chunk_store_type, bucket = %config.storage_bucket, "initialising chunk store");
    let chunk_store: Arc<dyn ChunkStore> = match config.chunk_store_type.as_str() {
        "s3" => {
            let endpoint = config.storage_endpoint.as_deref().unwrap_or("AWS");
            info!(endpoint, "using S3 chunk store");
            Arc::new(S3ChunkStore::from_config(&config)?)
        }
        "local" => {
            info!(path = %config.storage_bucket, "using local filesystem chunk store");
            Arc::new(LocalFsChunkStore::new(&config.storage_bucket))
        }
        other => {
            return Err(Error::Internal(format!(
                "unknown chunk_store_type: {other:?}, expected \"s3\" or \"local\""
            ))
            .into());
        }
    };

    let chunker = Arc::new(FixedSizeChunker::new(config.chunk_size_bytes)?);
    info!(chunk_size_bytes = config.chunk_size_bytes, "chunker ready");

    if config.admin_username == "twigletadmin" || config.admin_password == "twigletadmin" {
        warn!(
            "using default admin credentials — set TWIGLET_ADMIN_USERNAME and TWIGLET_ADMIN_PASSWORD"
        );
    }

    let lsn = Arc::new(LazyAtomicLsnGenerator::new(Arc::clone(&metadata)));

    let resolver = Arc::new(AncestryResolver::new(Arc::clone(&metadata)));

    let engine = Arc::new(Engine::new(lsn, chunker, chunk_store, metadata, resolver));
    info!("engine ready");

    let auth_layer = middleware::from_fn_with_state(
        http::auth::BasicAuthConfig {
            username: config.admin_username.clone(),
            password: config.admin_password.clone(),
        },
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

    let addr: SocketAddr = format!("{}:{}", config.host, config.port).parse()?;

    let listener = tokio::net::TcpListener::bind(addr).await?;

    info!(address = %listener.local_addr()?, "twiglet engine listening");

    axum::serve(listener, app).await?;

    Ok(())
}
