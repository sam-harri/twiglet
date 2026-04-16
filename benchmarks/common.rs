#![allow(dead_code)]
use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    http::{Method, Request, Response, StatusCode, header},
};
use http_body_util::BodyExt as _;
use serde_json::{Value, json};
use tempfile::TempDir;
use tower::ServiceExt as _;
use twiglet_engine::{
    ancestry_resolver::AncestryResolver,
    chunk_store::{ChunkStore, LocalFsChunkStore},
    chunker::FixedSizeChunker,
    config::{LocalStorageConfig, RocksDbMetastoreConfig},
    engine::Engine,
    http,
    lsn::LazyAtomicLsnGenerator,
    metastore::{MetadataStore, RocksDbMetadataStore},
};

pub struct Ctx {
    app: Router,
    #[allow(dead_code)]
    _rocks_dir: TempDir,
    #[allow(dead_code)]
    _chunks_dir: TempDir,
}

impl Ctx {
    pub async fn new() -> Self {
        let rocks_dir = tempfile::tempdir().unwrap();
        let chunks_dir = tempfile::tempdir().unwrap();

        let metadata: Arc<dyn MetadataStore> = Arc::new(
            RocksDbMetadataStore::try_from(RocksDbMetastoreConfig {
                path: rocks_dir.path().to_str().unwrap().to_string(),
                block_cache_mb: 64,
                rate_limit_mb_sec: 10,
            })
            .unwrap(),
        );
        let chunk_store: Arc<dyn ChunkStore> = Arc::new(LocalFsChunkStore::from(LocalStorageConfig {
            path: chunks_dir.path().to_str().unwrap().to_string(),
        }));
        let chunker = Arc::new(FixedSizeChunker::new(4_194_304).unwrap());
        let lsn = Arc::new(LazyAtomicLsnGenerator::new(Arc::clone(&metadata)));
        let resolver = Arc::new(AncestryResolver::new(Arc::clone(&metadata)));
        let engine = Arc::new(Engine::new(lsn, chunker, chunk_store, metadata, resolver));

        Self {
            app: http::router(engine),
            _rocks_dir: rocks_dir,
            _chunks_dir: chunks_dir,
        }
    }

    pub async fn raw(&self, req: Request<Body>) -> Response<Body> {
        self.app.clone().oneshot(req).await.unwrap()
    }

    pub async fn create_project(&self) -> (String, String) {
        let resp = self
            .raw(
                Request::builder()
                    .method(Method::POST)
                    .uri("/projects")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await;
        assert_eq!(resp.status(), StatusCode::CREATED);
        let j = body_json(resp).await;
        (
            j["project"]["project_id"].as_str().unwrap().to_string(),
            j["project"]["default_branch_id"]
                .as_str()
                .unwrap()
                .to_string(),
        )
    }

    pub async fn fork_branch(&self, project: &str, source: &str) -> (String, u64) {
        let resp = self
            .raw(json_req(
                Method::POST,
                &format!("/projects/{project}/branches"),
                json!({"source_branch_id": source}),
            ))
            .await;
        assert_eq!(resp.status(), StatusCode::CREATED);
        let j = body_json(resp).await;
        let branch_id = j["branch"]["branch_id"].as_str().unwrap().to_string();
        let fork_lsn = j["branch"]["fork_lsn"].as_u64().unwrap();
        (branch_id, fork_lsn)
    }

    pub async fn put(&self, project: &str, branch: &str, path: &str, body: &[u8]) -> u64 {
        let resp = self
            .raw(bytes_req(
                Method::PUT,
                &format!("/projects/{project}/branches/{branch}/objects/{path}"),
                body,
                "application/octet-stream",
            ))
            .await;
        assert!(
            resp.status().is_success(),
            "PUT {path} failed: {}",
            resp.status()
        );
        let j = body_json(resp).await;
        j["lsn"].as_u64().expect("lsn in PUT response")
    }

    pub async fn get(&self, project: &str, branch: &str, path: &str) -> (StatusCode, Vec<u8>) {
        let resp = self
            .raw(
                Request::builder()
                    .method(Method::GET)
                    .uri(format!(
                        "/projects/{project}/branches/{branch}/objects/{path}"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await;
        let status = resp.status();
        let bytes = body_bytes(resp).await;
        (status, bytes)
    }
}

pub fn json_req(method: Method, uri: &str, payload: Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
        .unwrap()
}

pub fn bytes_req(method: Method, uri: &str, bytes: &[u8], content_type: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, content_type)
        .body(Body::from(bytes.to_vec()))
        .unwrap()
}

pub async fn body_json(response: Response<Body>) -> Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

pub async fn body_bytes(response: Response<Body>) -> Vec<u8> {
    response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec()
}
