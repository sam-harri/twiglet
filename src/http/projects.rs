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
    types::{Page, Project},
};

#[derive(Serialize, ToSchema)]
pub struct ProjectResponse {
    pub project: Project,
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ListQuery {
    /// Opaque pagination cursor from a previous response
    pub cursor: Option<String>,
    /// Maximum number of items to return (1–1000, default 20)
    pub limit: Option<usize>,
}

/// Create a new project.
#[utoipa::path(
    post,
    path = "/projects",
    operation_id = "createProject",
    tag = "projects",
    responses(
        (status = 201, description = "Project created", body = ProjectResponse),
        (status = 409, description = "Project already exists", body = ErrorEnvelope),
    ),
    security(("basicAuth" = []))
)]
pub async fn create(
    State(engine): State<Arc<Engine>>,
) -> Result<(StatusCode, Json<ProjectResponse>)> {
    let project = engine.create_project().await?;
    Ok((StatusCode::CREATED, Json(ProjectResponse { project })))
}

/// List projects with cursor-based pagination.
#[utoipa::path(
    get,
    path = "/projects",
    operation_id = "listProjects",
    tag = "projects",
    params(ListQuery),
    responses(
        (status = 200, description = "Paginated list of projects", body = Page<Project>),
    ),
    security(("basicAuth" = []))
)]
pub async fn list(
    State(engine): State<Arc<Engine>>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Page<Project>>> {
    let limit = query.limit.unwrap_or(20);
    if limit > 1000 {
        return Err(Error::InvalidInput(
            "limit must not exceed 1000".to_string(),
        ));
    }
    let page = engine
        .list_projects(query.cursor.as_deref(), limit.max(1))
        .await?;
    Ok(Json(page))
}

/// Get a project by ID.
#[utoipa::path(
    get,
    path = "/projects/{project_id}",
    operation_id = "getProject",
    tag = "projects",
    params(
        ("project_id" = String, Path, description = "Project ID")
    ),
    responses(
        (status = 200, description = "Project found", body = ProjectResponse),
        (status = 404, description = "Project not found", body = ErrorEnvelope),
    ),
    security(("basicAuth" = []))
)]
pub async fn get(
    State(engine): State<Arc<Engine>>,
    Path(project_id): Path<String>,
) -> Result<Json<ProjectResponse>> {
    let project = engine.get_project(&project_id).await?;
    Ok(Json(ProjectResponse { project }))
}

/// Delete a project (soft-delete).
#[utoipa::path(
    delete,
    path = "/projects/{project_id}",
    operation_id = "deleteProject",
    tag = "projects",
    params(
        ("project_id" = String, Path, description = "Project ID")
    ),
    responses(
        (status = 204, description = "Project deleted"),
        (status = 404, description = "Project not found", body = ErrorEnvelope),
    ),
    security(("basicAuth" = []))
)]
pub async fn delete(
    State(engine): State<Arc<Engine>>,
    Path(project_id): Path<String>,
) -> Result<StatusCode> {
    engine.delete_project(&project_id).await?;
    Ok(StatusCode::NO_CONTENT)
}
