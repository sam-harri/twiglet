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
    types::{BranchInfo, Page},
};

#[derive(Deserialize, ToSchema)]
pub struct CreateBranchRequest {
    pub source_branch_id: String,
}

#[derive(Serialize, ToSchema)]
pub struct BranchResponse {
    pub branch: BranchInfo,
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ListQuery {
    /// Opaque pagination cursor from a previous response
    pub cursor: Option<String>,
    /// Maximum number of items to return (1–1000, default 20)
    pub limit: Option<usize>,
}

/// Exactly one of `lsn` or `snapshot_id` must be provided.
/// TODO could probably be an Enum?
#[derive(Deserialize, ToSchema)]
pub struct ForkRequest {
    /// LSN to fork at. Must be ≤ the source branch's current head LSN.
    pub lsn: Option<u64>,
    /// Snapshot ID to fork at. The snapshot's LSN will be used.
    pub snapshot_id: Option<String>,
}

/// Request body for `restore` (rewind a branch in-place to a specific LSN).
///
/// Exactly one of `lsn` or `snapshot_id` must be provided.
/// A backup branch is automatically created before the rewind.
#[derive(Deserialize, ToSchema)]
pub struct RestoreRequest {
    /// LSN to restore to. Must be ≤ the branch's current head LSN.
    pub lsn: Option<u64>,
    /// Snapshot ID to restore to. The snapshot's LSN will be used.
    pub snapshot_id: Option<String>,
}

/// Response returned after a successful in-place restore.
#[derive(Serialize, ToSchema)]
pub struct RestoreResponse {
    /// The restored branch (same branch_id, rewound to the requested LSN)
    pub branch: BranchInfo,
    /// Backup branch created to preserve the pre-restore state
    pub backup_branch: BranchInfo,
}

/// Fork a new branch from an existing branch.
///
/// The new branch inherits all objects visible on the source branch at the
/// current LSN. Writes to the new branch are independent of the source.
#[utoipa::path(
    post,
    path = "/projects/{project_id}/branches",
    operation_id = "createBranch",
    tag = "branches",
    params(
        ("project_id" = String, Path, description = "Project ID")
    ),
    request_body = CreateBranchRequest,
    responses(
        (status = 201, description = "Branch created", body = BranchResponse),
        (status = 404, description = "Project or source branch not found", body = ErrorEnvelope),
    ),
    security(("basicAuth" = []))
)]
pub async fn create(
    State(engine): State<Arc<Engine>>,
    Path(project_id): Path<String>,
    Json(req): Json<CreateBranchRequest>,
) -> Result<(StatusCode, Json<BranchResponse>)> {
    let branch = engine
        .create_branch(&project_id, &req.source_branch_id)
        .await?;
    Ok((StatusCode::CREATED, Json(BranchResponse { branch })))
}

/// List branches with cursor-based pagination.
#[utoipa::path(
    get,
    path = "/projects/{project_id}/branches",
    operation_id = "listBranches",
    tag = "branches",
    params(
        ("project_id" = String, Path, description = "Project ID"),
        ListQuery,
    ),
    responses(
        (status = 200, description = "Paginated list of branches", body = Page<BranchInfo>),
        (status = 404, description = "Project not found", body = ErrorEnvelope),
    ),
    security(("basicAuth" = []))
)]
pub async fn list(
    State(engine): State<Arc<Engine>>,
    Path(project_id): Path<String>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Page<BranchInfo>>> {
    let limit = query.limit.unwrap_or(20);
    if limit > 1000 {
        return Err(Error::InvalidInput(
            "limit must not exceed 1000".to_string(),
        ));
    }
    let page = engine
        .list_branches(&project_id, query.cursor.as_deref(), limit.max(1))
        .await?;
    Ok(Json(page))
}

/// Get a branch by ID.
#[utoipa::path(
    get,
    path = "/projects/{project_id}/branches/{branch_id}",
    operation_id = "getBranch",
    tag = "branches",
    params(
        ("project_id" = String, Path, description = "Project ID"),
        ("branch_id" = String, Path, description = "Branch ID"),
    ),
    responses(
        (status = 200, description = "Branch found", body = BranchResponse),
        (status = 404, description = "Project or branch not found", body = ErrorEnvelope),
    ),
    security(("basicAuth" = []))
)]
pub async fn get(
    State(engine): State<Arc<Engine>>,
    Path((project_id, branch_id)): Path<(String, String)>,
) -> Result<Json<BranchResponse>> {
    let branch = engine.get_branch(&project_id, &branch_id).await?;
    Ok(Json(BranchResponse { branch }))
}

/// Reset a branch to its parent's current state.
///
/// Creates a new internal node forked from the branch's parent at the current
/// project LSN, then atomically updates the branch's handle to point at it.
/// The same `branch_id` is preserved. Old writes on the branch are discarded
/// (their node is orphaned). Returns 422 if the branch has no parent.
#[utoipa::path(
    post,
    path = "/projects/{project_id}/branches/{branch_id}/reset",
    operation_id = "resetBranchFromParent",
    tag = "branches",
    params(
        ("project_id" = String, Path, description = "Project ID"),
        ("branch_id" = String, Path, description = "Branch to reset"),
    ),
    responses(
        (status = 200, description = "Branch reset", body = BranchResponse),
        (status = 404, description = "Project or branch not found", body = ErrorEnvelope),
        (status = 422, description = "Branch has no parent (is the root branch)", body = ErrorEnvelope),
    ),
    security(("basicAuth" = []))
)]
pub async fn reset_from_parent(
    State(engine): State<Arc<Engine>>,
    Path((project_id, branch_id)): Path<(String, String)>,
) -> Result<Json<BranchResponse>> {
    let branch = engine.reset_from_parent(&project_id, &branch_id).await?;
    Ok(Json(BranchResponse { branch }))
}

/// Response returned after a successful merge.
#[derive(Serialize, ToSchema)]
pub struct MergeResponse {
    /// Updated state of the target branch
    pub branch: BranchInfo,
    /// Number of object records copied from source onto target
    pub objects_merged: u64,
}

/// Merge a source branch into its direct parent.
///
/// Replays all object writes that `source_branch_id` made since it was forked
/// Merge a source branch into its direct parent.
///
/// Replays all object writes that `branch_id` made since it was forked onto its
/// parent branch. The source must have no children of its own. After a successful
/// merge the source branch is unchanged.
///
/// All copies are applied atomically (all-or-nothing). Tombstones are
/// propagated, so deletes on the source branch will take effect on the parent.
#[utoipa::path(
    post,
    path = "/projects/{project_id}/branches/{branch_id}/merge",
    operation_id = "mergeBranch",
    tag = "branches",
    params(
        ("project_id" = String, Path, description = "Project ID"),
        ("branch_id" = String, Path, description = "Source branch ID to merge from"),
    ),
    responses(
        (status = 200, description = "Merge successful", body = MergeResponse),
        (status = 404, description = "Project or branch not found", body = ErrorEnvelope),
        (status = 409, description = "Source has children or a concurrent merge is in progress", body = ErrorEnvelope),
        (status = 422, description = "Source branch has no parent", body = ErrorEnvelope),
    ),
    security(("basicAuth" = []))
)]
pub async fn merge(
    State(engine): State<Arc<Engine>>,
    Path((project_id, source_branch_id)): Path<(String, String)>,
) -> Result<Json<MergeResponse>> {
    let (branch, objects_merged) = engine.merge(&project_id, &source_branch_id).await?;
    Ok(Json(MergeResponse {
        branch,
        objects_merged,
    }))
}

/// Fork a new child branch from an existing branch at a specific LSN.
///
/// The source branch is left completely untouched. The new branch sees only
/// history up to `lsn` — writes to it advance from there.
/// Provide exactly one of `lsn` or `snapshot_id`.
#[utoipa::path(
    post,
    path = "/projects/{project_id}/branches/{branch_id}/fork",
    operation_id = "forkBranchAtLsn",
    tag = "branches",
    params(
        ("project_id" = String, Path, description = "Project ID"),
        ("branch_id" = String, Path, description = "Branch to fork from"),
    ),
    request_body = ForkRequest,
    responses(
        (status = 201, description = "Forked branch created", body = BranchResponse),
        (status = 400, description = "Invalid LSN or missing/conflicting fork target", body = ErrorEnvelope),
        (status = 404, description = "Project, branch, or snapshot not found", body = ErrorEnvelope),
    ),
    security(("basicAuth" = []))
)]
pub async fn fork(
    State(engine): State<Arc<Engine>>,
    Path((project_id, branch_id)): Path<(String, String)>,
    Json(req): Json<ForkRequest>,
) -> Result<(StatusCode, Json<BranchResponse>)> {
    let lsn = resolve_lsn(&engine, &project_id, &branch_id, req.lsn, req.snapshot_id).await?;
    let branch = engine.fork_at_lsn(&project_id, &branch_id, lsn).await?;
    Ok((StatusCode::CREATED, Json(BranchResponse { branch })))
}

/// Rewind a branch in-place to a previous LSN, preserving its current state in a backup branch.
///
/// The same `branch_id` is kept; its internal node is atomically swapped to a new node
/// capped at `lsn`. A backup branch is automatically created as a child of `branch_id`
/// pointing at the pre-restore node, so no history is lost.
///
/// The backup branch prevents `branch_id` from being deleted until the backup is
/// explicitly removed. Provide exactly one of `lsn` or `snapshot_id`.
#[utoipa::path(
    post,
    path = "/projects/{project_id}/branches/{branch_id}/restore",
    operation_id = "restoreBranch",
    tag = "branches",
    params(
        ("project_id" = String, Path, description = "Project ID"),
        ("branch_id" = String, Path, description = "Branch to restore"),
    ),
    request_body = RestoreRequest,
    responses(
        (status = 200, description = "Branch restored; backup branch created", body = RestoreResponse),
        (status = 400, description = "Invalid LSN or missing/conflicting restore target", body = ErrorEnvelope),
        (status = 404, description = "Project, branch, or snapshot not found", body = ErrorEnvelope),
    ),
    security(("basicAuth" = []))
)]
pub async fn restore(
    State(engine): State<Arc<Engine>>,
    Path((project_id, branch_id)): Path<(String, String)>,
    Json(req): Json<RestoreRequest>,
) -> Result<Json<RestoreResponse>> {
    let lsn = resolve_lsn(&engine, &project_id, &branch_id, req.lsn, req.snapshot_id).await?;
    let (branch, backup_branch) = engine.restore(&project_id, &branch_id, lsn).await?;
    Ok(Json(RestoreResponse {
        branch,
        backup_branch,
    }))
}

/// Resolve a point-in-time target from either a raw LSN or a snapshot ID.
async fn resolve_lsn(
    engine: &Engine,
    project_id: &str,
    branch_id: &str,
    lsn: Option<u64>,
    snapshot_id: Option<String>,
) -> Result<u64> {
    match (lsn, snapshot_id) {
        (Some(lsn), None) => Ok(lsn),
        (None, Some(snapshot_id)) => {
            let snap = engine
                .get_snapshot(project_id, branch_id, &snapshot_id)
                .await?;
            Ok(snap.lsn)
        }
        _ => Err(Error::InvalidInput(
            "exactly one of lsn or snapshot_id must be provided".to_string(),
        )),
    }
}
