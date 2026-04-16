use std::sync::Arc;

use axum::{
    Json, Router,
    routing::{delete, get, post, put},
};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::engine::Engine;

pub mod auth;
pub mod branches;
pub mod objects;
pub mod openapi;
pub mod projects;
pub mod snapshots;

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

pub fn docs_router() -> Router {
    Router::new()
        .route("/health", get(health))
        .merge(SwaggerUi::new("/swagger-ui").url("/openapi.json", openapi::ApiDoc::openapi()))
}

pub fn router(engine: Arc<Engine>) -> Router {
    Router::new()
        .route("/projects", post(projects::create))
        .route("/projects", get(projects::list))
        .route("/projects/{project_id}", get(projects::get))
        .route("/projects/{project_id}", delete(projects::delete))
        .route("/projects/{project_id}/branches", post(branches::create))
        .route("/projects/{project_id}/branches", get(branches::list))
        .route(
            "/projects/{project_id}/branches/{branch_id}",
            get(branches::get),
        )
        .route(
            "/projects/{project_id}/branches/{branch_id}",
            delete(branches::delete),
        )
        .route(
            "/projects/{project_id}/branches/{branch_id}/fork",
            post(branches::fork),
        )
        .route(
            "/projects/{project_id}/branches/{branch_id}/restore",
            post(branches::restore),
        )
        .route(
            "/projects/{project_id}/branches/{branch_id}/reset",
            post(branches::reset_from_parent),
        )
        .route(
            "/projects/{project_id}/branches/{branch_id}/merge",
            post(branches::merge),
        )
        .route(
            "/projects/{project_id}/branches/{branch_id}/snapshots",
            post(snapshots::create),
        )
        .route(
            "/projects/{project_id}/branches/{branch_id}/snapshots",
            get(snapshots::list),
        )
        .route(
            "/projects/{project_id}/branches/{branch_id}/snapshots/{snapshot_name}",
            get(snapshots::get),
        )
        .route(
            "/projects/{project_id}/branches/{branch_id}/snapshots/{snapshot_name}",
            delete(snapshots::delete),
        )
        .route(
            "/projects/{project_id}/branches/{branch_id}/objects",
            get(objects::list),
        )
        .route(
            "/projects/{project_id}/branches/{branch_id}/objects/{*path}",
            put(objects::put),
        )
        .route(
            "/projects/{project_id}/branches/{branch_id}/objects/{*path}",
            get(objects::get),
        )
        .route(
            "/projects/{project_id}/branches/{branch_id}/objects/{*path}",
            delete(objects::delete),
        )
        .with_state(engine)
}
