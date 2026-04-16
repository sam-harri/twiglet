use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use utoipa::ToSchema;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Project not found")]
    ProjectNotFound,
    #[error("Project already exists")]
    ProjectAlreadyExists,
    #[error("Branch not found")]
    BranchNotFound,
    #[error("Object not found")]
    ObjectNotFound,
    #[error("Snapshot not found")]
    SnapshotNotFound,
    #[error("Source branch is not a direct child of target")]
    NotDirectChild,
    #[error("Branch has children")]
    BranchHasChildren,
    #[error("Cannot delete the default branch")]
    CannotDeleteDefaultBranch,
    #[error("Branch has no parent (is the root branch)")]
    BranchIsRoot,
    #[error("Branch mutation already in progress")]
    Conflict,
    #[error("Invalid cursor")]
    InvalidCursor,
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    #[error("Invalid restore LSN")]
    InvalidRestoreLsn,
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("Chunk store error: {0}")]
    ChunkStore(String),
    #[error("Internal error: {0}")]
    Internal(String),
}

#[derive(Serialize, ToSchema)]
pub struct ErrorEnvelope {
    pub error: ErrorBody,
}

#[derive(Serialize, ToSchema)]
pub struct ErrorBody {
    /// Machine-readable error code, e.g. `PROJECT_NOT_FOUND`
    pub code: &'static str,
    /// Human-readable description of the error
    pub message: String,
}

impl Error {
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::ProjectNotFound
            | Self::BranchNotFound
            | Self::ObjectNotFound
            | Self::SnapshotNotFound => StatusCode::NOT_FOUND,
            Self::ProjectAlreadyExists
            | Self::BranchHasChildren
            | Self::CannotDeleteDefaultBranch
            | Self::Conflict => StatusCode::CONFLICT,
            Self::NotDirectChild | Self::BranchIsRoot => StatusCode::UNPROCESSABLE_ENTITY,
            Self::InvalidCursor | Self::InvalidInput(_) | Self::InvalidRestoreLsn => {
                StatusCode::BAD_REQUEST
            }
            Self::Storage(_) | Self::ChunkStore(_) | Self::Internal(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            Self::ProjectNotFound => "PROJECT_NOT_FOUND",
            Self::ProjectAlreadyExists => "PROJECT_ALREADY_EXISTS",
            Self::BranchNotFound => "BRANCH_NOT_FOUND",

            Self::ObjectNotFound => "OBJECT_NOT_FOUND",
            Self::SnapshotNotFound => "SNAPSHOT_NOT_FOUND",
            Self::NotDirectChild => "NOT_DIRECT_CHILD",
            Self::BranchIsRoot => "BRANCH_IS_ROOT",
            Self::BranchHasChildren => "BRANCH_HAS_CHILDREN",
            Self::CannotDeleteDefaultBranch => "CANNOT_DELETE_DEFAULT_BRANCH",
            Self::Conflict => "CONFLICT",
            Self::InvalidCursor => "INVALID_CURSOR",
            Self::InvalidInput(_) => "INVALID_INPUT",
            Self::InvalidRestoreLsn => "INVALID_RESTORE_LSN",
            Self::Storage(_) => "STORAGE_ERROR",
            Self::ChunkStore(_) => "CHUNK_STORE_ERROR",
            Self::Internal(_) => "INTERNAL_ERROR",
        }
    }
}

impl From<bincode::Error> for Error {
    fn from(e: bincode::Error) -> Self {
        Error::Storage(e.to_string())
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let body = ErrorEnvelope {
            error: ErrorBody {
                code: self.code(),
                message: self.to_string(),
            },
        };
        (status, Json(body)).into_response()
    }
}
