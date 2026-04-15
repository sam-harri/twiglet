use utoipa::{
    Modify, OpenApi,
    openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme},
};

use crate::{
    error::{ErrorBody, ErrorEnvelope},
    http::{branches, objects, projects, snapshots},
    types::{
        BranchInfo, ObjectListEntry, ObjectListResponse, Page, Project, PutObjectResponse,
        SnapshotRecord,
    },
};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Twiglet Engine API",
        description = "Branching object storage with point-in-time restore",
        version = "0.1.0",
    ),
    paths(
        projects::create,
        projects::list,
        projects::get,
        projects::delete,
        branches::create,
        branches::list,
        branches::get,
        branches::delete,
        branches::fork,
        branches::restore,
        branches::reset_from_parent,
        branches::merge,
        snapshots::create,
        snapshots::list,
        snapshots::get,
        snapshots::delete,
        objects::put,
        objects::get,
        objects::delete,
        objects::list,
    ),
    components(schemas(
        Project,
        BranchInfo,
        SnapshotRecord,
        ObjectListEntry,
        ObjectListResponse,
        PutObjectResponse,
        Page<Project>,
        Page<BranchInfo>,
        Page<SnapshotRecord>,
        projects::ProjectResponse,
        branches::CreateBranchRequest,
        branches::BranchResponse,
        branches::ForkRequest,
        branches::RestoreRequest,
        branches::RestoreResponse,
        branches::MergeRequest,
        branches::MergeResponse,
        snapshots::SnapshotResponse,
        ErrorEnvelope,
        ErrorBody,
    )),
    modifiers(&SecurityAddon),
    tags(
        (name = "projects", description = "Project management"),
        (name = "branches", description = "Branch management and time-travel"),
        (name = "snapshots", description = "Snapshot management"),
        (name = "objects", description = "Object storage operations"),
    )
)]
pub struct ApiDoc;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "basicAuth",
            SecurityScheme::Http(HttpBuilder::new().scheme(HttpAuthScheme::Basic).build()),
        );
    }
}
