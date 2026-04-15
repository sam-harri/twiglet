/// Tests the single-branch mutation log from @mutation-log in the report:
///
/// Shoutout Claude for these ASCII diagrams
///
///  put foo.txt      put bar.pdf      put bax.png    delete foo.txt
///    LSN-1            LSN-2            LSN-3            LSN-4
///      |                |                |                |
///  ────●────────────────●────────────────●────────────────●────▶
///
/// Covers tombstone semantics, point-in-time reads (GET@LSN), and overwrite
/// resolution on a root branch with no ancestry to walk
use axum::http::StatusCode;

use crate::common::Ctx;

#[tokio::test]
async fn single_branch_tombstone_hides_object() {
    let ctx = Ctx::new().await;
    let (project, branch) = ctx.create_project().await;

    ctx.put(&project, &branch, "foo.txt", b"foo-v1").await;
    ctx.put(&project, &branch, "bar.pdf", b"bar").await;
    ctx.put(&project, &branch, "bax.png", b"bax").await;
    ctx.delete_object(&project, &branch, "foo.txt").await;
    let (status, _) = ctx.get(&project, &branch, "foo.txt").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "tombstone must hide foo.txt");

    let (status, body) = ctx.get(&project, &branch, "bar.pdf").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"bar");

    let (status, _) = ctx.get(&project, &branch, "bax.png").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn get_at_lsn_returns_version_visible_at_that_point() {
    let ctx = Ctx::new().await;
    let (project, branch) = ctx.create_project().await;

    let lsn1 = ctx.put(&project, &branch, "foo.txt", b"foo-v1").await;
    let lsn2 = ctx.put(&project, &branch, "bar.pdf", b"bar").await;
    let lsn3 = ctx.put(&project, &branch, "bax.png", b"bax").await;
    ctx.delete_object(&project, &branch, "foo.txt").await; // lsn4

    let (status, body) = ctx.get_at_lsn(&project, &branch, "foo.txt", lsn3).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "foo.txt must be visible at lsn3 (before delete)"
    );
    assert_eq!(body, b"foo-v1");

    let (status, _) = ctx.get_at_lsn(&project, &branch, "bar.pdf", lsn1).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "bar.pdf must not exist at lsn1"
    );

    let (status, _) = ctx.get_at_lsn(&project, &branch, "bax.png", lsn2).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "bax.png must not exist at lsn2"
    );

    let (status, _) = ctx.get_at_lsn(&project, &branch, "foo.txt", 0).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "nothing visible before any write"
    );
}

#[tokio::test]
async fn overwrite_resolves_to_latest_version() {
    let ctx = Ctx::new().await;
    let (project, branch) = ctx.create_project().await;

    let lsn_v1 = ctx.put(&project, &branch, "foo.txt", b"foo-v1").await;
    ctx.put(&project, &branch, "foo.txt", b"foo-v2").await;

    let (status, body) = ctx.get(&project, &branch, "foo.txt").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"foo-v2", "latest version must be returned");

    let (status, body) = ctx.get_at_lsn(&project, &branch, "foo.txt", lsn_v1).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"foo-v1", "v1 must be visible at lsn_v1");
}
