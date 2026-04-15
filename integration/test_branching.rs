/// Tests the branched mutation log from @branch-log in the report:
///
///  put foo.txt    put bar.pdf    put bax.png  delete foo.txt
///    LSN-1          LSN-2          LSN-3          LSN-4
///      |              |              |              |
///  ────●──────────────●──────────────●──────────────●────▶  main
///                     |
///                     └──────────────●──────────────●────▶  dev
///                                    |              |
///                               put foo.txt     put baz.md
///                                 LSN-3           LSN-4
///
/// Covers ancestor inheritance up to fork_lsn, post-fork isolation in both
/// directions, child overwrite semantics, and fork_at_lsn snapshot creation.
use axum::http::StatusCode;

use crate::common::Ctx;

async fn build_branch_graph(ctx: &Ctx) -> (String, String, String, u64, u64, u64) {
    let (project, main) = ctx.create_project().await;

    let lsn1 = ctx.put(&project, &main, "foo.txt", b"foo-v1").await;
    let lsn2 = ctx.put(&project, &main, "bar.pdf", b"bar").await;

    let (dev, fork_lsn) = ctx.fork_branch(&project, &main).await;

    ctx.put(&project, &main, "bax.png", b"bax").await;
    ctx.delete_object(&project, &main, "foo.txt").await;

    ctx.put(&project, &dev, "foo.txt", b"foo-v2").await;
    ctx.put(&project, &dev, "baz.md", b"baz").await;

    (project, main, dev, lsn1, lsn2, fork_lsn)
}

#[tokio::test]
async fn child_inherits_parent_history_up_to_fork_lsn() {
    let ctx = Ctx::new().await;
    let (project, _main, dev, lsn1, _lsn2, _fork_lsn) = build_branch_graph(&ctx).await;

    let (status, body) = ctx.get(&project, &dev, "bar.pdf").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "bar.pdf must be inherited from main"
    );
    assert_eq!(body, b"bar");

    let (status, body) = ctx.get(&project, &dev, "foo.txt").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "foo.txt must resolve to dev's version"
    );
    assert_eq!(body, b"foo-v2");

    let (status, body) = ctx.get_at_lsn(&project, &dev, "foo.txt", lsn1).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "foo.txt at lsn1 must be foo-v1 from main"
    );
    assert_eq!(body, b"foo-v1");
}

#[tokio::test]
async fn parent_and_child_do_not_share_post_fork_writes() {
    let ctx = Ctx::new().await;
    let (project, main, dev, _lsn1, _lsn2, _fork_lsn) = build_branch_graph(&ctx).await;

    let (status, _) = ctx.get(&project, &dev, "bax.png").await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "post-fork main write must not bleed into dev"
    );

    let (status, _) = ctx.get(&project, &main, "baz.md").await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "dev-only write must not appear on main"
    );

    let (status, body) = ctx.get(&project, &dev, "foo.txt").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "main's delete must not cross into dev"
    );
    assert_eq!(body, b"foo-v2");
}

#[tokio::test]
async fn child_overwrites_parent_object_without_affecting_parent() {
    let ctx = Ctx::new().await;
    let (project, main, _dev, _lsn1, _lsn2, _fork_lsn) = build_branch_graph(&ctx).await;

    let (status, _) = ctx.get(&project, &main, "foo.txt").await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "main's foo.txt is still deleted"
    );

    let (status, _) = ctx.get(&project, &main, "baz.md").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn fork_at_lsn_creates_snapshot_at_given_point() {
    let ctx = Ctx::new().await;
    let (project, main) = ctx.create_project().await;

    let lsn1 = ctx.put(&project, &main, "foo.txt", b"foo-v1").await;
    let _lsn2 = ctx.put(&project, &main, "bar.pdf", b"bar").await;

    let snap = ctx.fork_at_lsn(&project, &main, lsn1).await;

    let (status, body) = ctx.get(&project, &snap, "foo.txt").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "foo.txt was written at lsn1 — must be visible"
    );
    assert_eq!(body, b"foo-v1");

    let (status, _) = ctx.get(&project, &snap, "bar.pdf").await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "bar.pdf written after lsn1 — must not appear in snapshot"
    );
}
