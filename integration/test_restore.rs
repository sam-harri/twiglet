/// Tests restore semantics from the @after-restore figures in the report.
///
/// Initial state (same as branching):
///
///  put foo.txt  put bar.pdf  put bax.png  delete foo.txt
///    LSN-1        LSN-2        LSN-3          LSN-4
///      |            |            |              |
///  ────●────────────●────────────●──────────────●────▶  main
///                   |
///                   └────────────●──────────────●────▶  dev
///                                |              |
///                           put foo.txt     put baz.md
///                             LSN-3           LSN-4
///
/// After restore(dev, LSN-3) — read resolution view:
///
///  put foo.txt  put bar.pdf  put bax.png  delete foo.txt
///    LSN-1        LSN-2        LSN-3          LSN-4
///      |            |            |              |
///  ────●────────────●────────────●──────────────●────▶  main
///                   |
///                   └────────────●──────────────●────▶  backup
///                                |       |      |
///                           put foo.txt  |  put baz.md
///                             LSN-3      |    LSN-4
///                                        |
///                                        └──────●────▶  dev (fork_lsn=LSN-3)
///
/// dev's new node reads through backup's node but is capped at LSN-3,
/// so baz.md at LSN-4 is invisible. backup sees the full pre-restore history.
///
/// After restore(dev, LSN-1) — read resolution view:
///
///  put foo.txt  put bar.pdf  put bax.png  delete foo.txt
///    LSN-1        LSN-2        LSN-3          LSN-4
///      |            |            |              |
///  ────●────────────●────────────●──────────────●────▶  main
///      |            |
///      |            └────────────●──────────────●────▶  backup (fork_lsn=LSN-2)
///      |                         |              |
///      |                    put foo.txt     put baz.md
///      |                      LSN-3           LSN-4
///      |
///      └──●────▶  dev (fork_lsn=LSN-1)
///
///
/// Branch handle tree (deletion order) for both restore cases:
///   main ← dev ← backup
///
/// Covers in-place rewind, backup branch creation, cross-node isolation,
/// rewind past the original fork point, and deletion ordering enforcement.
use axum::http::StatusCode;

use crate::common::Ctx;

async fn setup(ctx: &Ctx) -> (String, String, String, u64, u64) {
    let (project, main) = ctx.create_project().await;

    ctx.put(&project, &main, "foo.txt", b"foo-v1").await;
    ctx.put(&project, &main, "bar.pdf", b"bar").await;

    let (dev, _) = ctx.fork_branch(&project, &main).await;

    ctx.put(&project, &main, "bax.png", b"bax").await;
    ctx.delete_object(&project, &main, "foo.txt").await;

    let lsn3_dev = ctx.put(&project, &dev, "foo.txt", b"foo-v2").await;
    let lsn4_dev = ctx.put(&project, &dev, "baz.md", b"baz").await;

    (project, main, dev, lsn3_dev, lsn4_dev)
}

#[tokio::test]
async fn restore_to_mid_branch_lsn_discards_later_writes() {
    let ctx = Ctx::new().await;
    let (project, _main, dev, lsn3_dev, _lsn4_dev) = setup(&ctx).await;

    let (branch, backup) = ctx.restore(&project, &dev, lsn3_dev).await;
    assert_eq!(branch, dev, "branch_id must survive restore unchanged");

    let (status, body) = ctx.get(&project, &dev, "foo.txt").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"foo-v2");

    let (status, _) = ctx.get(&project, &dev, "baz.md").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, body) = ctx.get(&project, &dev, "bar.pdf").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"bar");

    let (status, body) = ctx.get(&project, &backup, "baz.md").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"baz");

    let (status, body) = ctx.get(&project, &backup, "foo.txt").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"foo-v2");
}

#[tokio::test]
async fn restore_to_before_fork_lsn_rewinds_past_own_history() {
    let ctx = Ctx::new().await;

    let (project, main) = ctx.create_project().await;
    let lsn1 = ctx.put(&project, &main, "foo.txt", b"foo-v1").await;
    ctx.put(&project, &main, "bar.pdf", b"bar").await;
    let (dev, _) = ctx.fork_branch(&project, &main).await;
    ctx.put(&project, &dev, "foo.txt", b"foo-v2").await;
    ctx.put(&project, &dev, "baz.md", b"baz").await;

    let (branch_id, backup) = ctx.restore(&project, &dev, lsn1).await;
    assert_eq!(branch_id, dev);

    let (status, body) = ctx.get(&project, &dev, "foo.txt").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"foo-v1");

    let (status, _) = ctx.get(&project, &dev, "bar.pdf").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = ctx.get(&project, &dev, "baz.md").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, body) = ctx.get(&project, &backup, "foo.txt").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"foo-v2");
}

#[tokio::test]
async fn restore_backup_is_child_of_restored_branch_and_blocks_deletion() {
    let ctx = Ctx::new().await;
    let (project, _main, dev, lsn3_dev, _lsn4_dev) = setup(&ctx).await;

    let (_branch_id, backup) = ctx.restore(&project, &dev, lsn3_dev).await;

    let status = ctx.delete_branch(&project, &dev).await;
    assert_eq!(status, StatusCode::CONFLICT);

    let status = ctx.delete_branch(&project, &backup).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let status = ctx.delete_branch(&project, &dev).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn new_writes_after_restore_do_not_bleed_into_backup() {
    let ctx = Ctx::new().await;
    let (project, _main, dev, lsn3_dev, _lsn4_dev) = setup(&ctx).await;

    let (_branch_id, backup) = ctx.restore(&project, &dev, lsn3_dev).await;

    ctx.put(&project, &dev, "new.txt", b"new").await;

    let (status, body) = ctx.get(&project, &dev, "new.txt").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"new");

    let (status, _) = ctx.get(&project, &backup, "new.txt").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
