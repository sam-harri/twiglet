use std::sync::Arc;

use axum::{
    Json,
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
};
use futures::TryStreamExt;
use serde::Deserialize;
use tokio_util::io::StreamReader;
use utoipa::IntoParams;

use crate::{
    engine::Engine,
    error::{Error, ErrorEnvelope, Result},
    types::{GetObjectResponse, ObjectListResponse, PutObjectResponse},
};

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct GetQuery {
    /// Return the object as it existed at this LSN. Omit for the current state.
    pub at_lsn: Option<u64>,
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ListQuery {
    /// Filter to objects whose path starts with this prefix
    pub prefix: Option<String>,
    /// Opaque pagination cursor from a previous response
    pub cursor: Option<String>,
    /// Maximum number of objects to return (1–1000, default 100)
    pub limit: Option<usize>,
    /// Return the listing as it existed at this LSN. Omit for the current state.
    pub at_lsn: Option<u64>,
}

/// Write an object to a branch.
///
/// Creates or overwrites the object at the given path. The request body is
/// the raw object content. Set `Content-Type` to the object's MIME type.
///
/// Returns `201 Created` for a new object and `200 OK` for an overwrite.
#[utoipa::path(
    put,
    path = "/projects/{project_id}/branches/{branch_id}/objects/{path}",
    operation_id = "putObject",
    tag = "objects",
    params(
        ("project_id" = String, Path, description = "Project ID"),
        ("branch_id" = String, Path, description = "Branch ID"),
        ("path" = String, Path, description = "Object path within the branch"),
    ),
    request_body(
        content = String,
        content_type = "application/octet-stream",
        description = "Raw object content"
    ),
    responses(
        (status = 201, description = "Object created", body = PutObjectResponse),
        (status = 200, description = "Object overwritten", body = PutObjectResponse),
        (status = 404, description = "Project or branch not found", body = ErrorEnvelope),
    ),
    security(("basicAuth" = []))
)]
pub async fn put(
    State(engine): State<Arc<Engine>>,
    Path((project_id, branch_id, path)): Path<(String, String, String)>,
    headers: HeaderMap,
    body: Body,
) -> Result<(StatusCode, Json<PutObjectResponse>)> {
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream");

    let data_stream = TryStreamExt::map_err(body.into_data_stream(), std::io::Error::other);
    let mut reader = StreamReader::new(data_stream);

    let response = engine
        .put_object(&project_id, &branch_id, &path, &mut reader, content_type)
        .await?;

    let status = if response.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(response)))
}

/// Download an object from a branch.
///
/// Returns the raw object bytes with `Content-Type` and `Content-Length` headers.
/// Use `HEAD` on the same path to retrieve these headers without the body.
#[utoipa::path(
    get,
    path = "/projects/{project_id}/branches/{branch_id}/objects/{path}",
    operation_id = "getObject",
    tag = "objects",
    params(
        ("project_id" = String, Path, description = "Project ID"),
        ("branch_id" = String, Path, description = "Branch ID"),
        ("path" = String, Path, description = "Object path within the branch"),
        GetQuery,
    ),
    responses(
        (status = 200, description = "Object content", content_type = "application/octet-stream"),
        (status = 404, description = "Object not found", body = ErrorEnvelope),
    ),
    security(("basicAuth" = []))
)]
pub async fn get(
    State(engine): State<Arc<Engine>>,
    Path((project_id, branch_id, path)): Path<(String, String, String)>,
    Query(query): Query<GetQuery>,
) -> Result<impl IntoResponse> {
    let Some(GetObjectResponse { meta, stream }) = engine
        .get_object(&project_id, &branch_id, &path, query.at_lsn)
        .await?
    else {
        return Err(Error::ObjectNotFound);
    };

    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_str(&meta.content_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    headers.insert(
        axum::http::header::CONTENT_LENGTH,
        HeaderValue::from(meta.size),
    );

    Ok((StatusCode::OK, headers, Body::from_stream(stream)))
}

/// Delete an object from a branch.
///
/// Writes a tombstone at the current LSN, making the object invisible on
/// this branch without affecting ancestor branches.
#[utoipa::path(
    delete,
    path = "/projects/{project_id}/branches/{branch_id}/objects/{path}",
    operation_id = "deleteObject",
    tag = "objects",
    params(
        ("project_id" = String, Path, description = "Project ID"),
        ("branch_id" = String, Path, description = "Branch ID"),
        ("path" = String, Path, description = "Object path within the branch"),
    ),
    responses(
        (status = 204, description = "Object deleted"),
        (status = 404, description = "Project or branch not found", body = ErrorEnvelope),
    ),
    security(("basicAuth" = []))
)]
pub async fn delete(
    State(engine): State<Arc<Engine>>,
    Path((project_id, branch_id, path)): Path<(String, String, String)>,
) -> Result<StatusCode> {
    engine.delete_object(&project_id, &branch_id, &path).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// List objects on a branch.
///
/// Supports S3-style prefix/delimiter filtering for virtual directory trees.
/// Objects are resolved through the branch ancestry chain — objects from
/// parent branches are visible unless overwritten or deleted on this branch.
#[utoipa::path(
    get,
    path = "/projects/{project_id}/branches/{branch_id}/objects",
    operation_id = "listObjects",
    tag = "objects",
    params(
        ("project_id" = String, Path, description = "Project ID"),
        ("branch_id" = String, Path, description = "Branch ID"),
        ListQuery,
    ),
    responses(
        (status = 200, description = "Object listing", body = ObjectListResponse),
        (status = 404, description = "Project or branch not found", body = ErrorEnvelope),
    ),
    security(("basicAuth" = []))
)]
pub async fn list(
    State(engine): State<Arc<Engine>>,
    Path((project_id, branch_id)): Path<(String, String)>,
    Query(query): Query<ListQuery>,
) -> Result<Json<ObjectListResponse>> {
    let limit = query.limit.unwrap_or(100);
    if limit > 1000 {
        return Err(Error::InvalidInput(
            "limit must not exceed 1000".to_string(),
        ));
    }
    let response = engine
        .list_objects(
            &project_id,
            &branch_id,
            query.prefix.as_deref(),
            query.cursor.as_deref(),
            limit.max(1),
            query.at_lsn,
        )
        .await?;

    Ok(Json(response))
}
