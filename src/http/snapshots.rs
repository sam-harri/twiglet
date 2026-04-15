use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{
    engine::Engine,
    error::{Error, ErrorEnvelope, Result},
    types::{Page, SnapshotRecord},
};

#[derive(Serialize, ToSchema)]
pub struct SnapshotResponse {
    pub snapshot: SnapshotRecord,
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ListQuery {
    /// Opaque pagination cursor from a previous response
    pub cursor: Option<String>,
    /// Maximum number of items to return (1–1000, default 20)
    pub limit: Option<usize>,
}

/// Create a snapshot of the current branch state.
///
/// Captures the current project LSN as the snapshot point. The snapshot can
/// be used to restore the branch to this exact state later.
#[utoipa::path(
    post,
    path = "/projects/{project_id}/branches/{branch_id}/snapshots",
    operation_id = "createSnapshot",
    tag = "snapshots",
    params(
        ("project_id" = String, Path, description = "Project ID"),
        ("branch_id" = String, Path, description = "Branch ID"),
    ),
    responses(
        (status = 201, description = "Snapshot created", body = SnapshotResponse),
        (status = 404, description = "Project or branch not found", body = ErrorEnvelope),
    ),
    security(("basicAuth" = []))
)]
pub async fn create(
    State(engine): State<Arc<Engine>>,
    Path((project_id, branch_id)): Path<(String, String)>,
) -> Result<(StatusCode, Json<SnapshotResponse>)> {
    let snapshot = engine.create_snapshot(&project_id, &branch_id).await?;
    Ok((StatusCode::CREATED, Json(SnapshotResponse { snapshot })))
}

/// List snapshots for a branch with cursor-based pagination.
#[utoipa::path(
    get,
    path = "/projects/{project_id}/branches/{branch_id}/snapshots",
    operation_id = "listSnapshots",
    tag = "snapshots",
    params(
        ("project_id" = String, Path, description = "Project ID"),
        ("branch_id" = String, Path, description = "Branch ID"),
        ListQuery,
    ),
    responses(
        (status = 200, description = "Paginated list of snapshots", body = Page<SnapshotRecord>),
        (status = 404, description = "Project or branch not found", body = ErrorEnvelope),
    ),
    security(("basicAuth" = []))
)]
pub async fn list(
    State(engine): State<Arc<Engine>>,
    Path((project_id, branch_id)): Path<(String, String)>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Page<SnapshotRecord>>> {
    let limit = query.limit.unwrap_or(20);
    if limit > 1000 {
        return Err(Error::InvalidInput(
            "limit must not exceed 1000".to_string(),
        ));
    }
    let page = engine
        .list_snapshots(
            &project_id,
            &branch_id,
            query.cursor.as_deref(),
            limit.max(1),
        )
        .await?;
    Ok(Json(page))
}

/// Get a snapshot by name.
#[utoipa::path(
    get,
    path = "/projects/{project_id}/branches/{branch_id}/snapshots/{snapshot_name}",
    operation_id = "getSnapshot",
    tag = "snapshots",
    params(
        ("project_id" = String, Path, description = "Project ID"),
        ("branch_id" = String, Path, description = "Branch ID"),
        ("snapshot_name" = String, Path, description = "Snapshot ID"),
    ),
    responses(
        (status = 200, description = "Snapshot found", body = SnapshotResponse),
        (status = 404, description = "Project, branch, or snapshot not found", body = ErrorEnvelope),
    ),
    security(("basicAuth" = []))
)]
pub async fn get(
    State(engine): State<Arc<Engine>>,
    Path((project_id, branch_id, snapshot_name)): Path<(String, String, String)>,
) -> Result<Json<SnapshotResponse>> {
    let snapshot = engine
        .get_snapshot(&project_id, &branch_id, &snapshot_name)
        .await?;
    Ok(Json(SnapshotResponse { snapshot }))
}

/// Delete a snapshot.
#[utoipa::path(
    delete,
    path = "/projects/{project_id}/branches/{branch_id}/snapshots/{snapshot_name}",
    operation_id = "deleteSnapshot",
    tag = "snapshots",
    params(
        ("project_id" = String, Path, description = "Project ID"),
        ("branch_id" = String, Path, description = "Branch ID"),
        ("snapshot_name" = String, Path, description = "Snapshot ID"),
    ),
    responses(
        (status = 204, description = "Snapshot deleted"),
        (status = 404, description = "Project, branch, or snapshot not found", body = ErrorEnvelope),
    ),
    security(("basicAuth" = []))
)]
pub async fn delete(
    State(engine): State<Arc<Engine>>,
    Path((project_id, branch_id, snapshot_name)): Path<(String, String, String)>,
) -> Result<StatusCode> {
    engine
        .delete_snapshot(&project_id, &branch_id, &snapshot_name)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
