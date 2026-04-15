/// Tests merge (promote) semantics from the @after_promote figure in the report.
///
/// Initial state (same as branching):
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
/// After merge(dev → main):
///
///  put foo.txt    put bar.pdf    put bax.png  delete foo.txt  put foo.txt  put baz.md
///    LSN-1          LSN-2          LSN-3          LSN-4          LSN-5        LSN-6
///      |              |              |              |              |            |
///  ────●──────────────●──────────────●──────────────●──────────────●────────────●────▶  main
///                     |
///                     └──────────────●──────────────●────▶  dev (unchanged)
///
/// Merge replays only the mutations on the source branch since the fork
/// onto the target, writing each at a new LSN. Because the
/// new LSN is higher than any prior write, it overrides deletes and outdated
/// versions on the target.
///
/// Covers direct-mutation replay, tombstone propagation, inherited-object
/// exclusion, and LSN-ordering override of existing deletes.
use axum::http::StatusCode;

use crate::common::Ctx;

async fn build_graph(ctx: &Ctx) -> (String, String, String) {
    let (project, main) = ctx.create_project().await;

    ctx.put(&project, &main, "foo.txt", b"foo-v1").await;
    ctx.put(&project, &main, "bar.pdf", b"bar").await;

    let (dev, _) = ctx.fork_branch(&project, &main).await;

    ctx.put(&project, &main, "bax.png", b"bax").await;
    ctx.delete_object(&project, &main, "foo.txt").await;

    ctx.put(&project, &dev, "foo.txt", b"foo-v2").await;
    ctx.put(&project, &dev, "baz.md", b"baz").await;

    (project, main, dev)
}

#[tokio::test]
async fn merge_replays_direct_mutations_onto_parent() {
    let ctx = Ctx::new().await;
    let (project, main, dev) = build_graph(&ctx).await;

    let objects_merged = ctx.merge(&project, &dev, &main).await;
    assert_eq!(
        objects_merged, 2,
        "only direct dev writes (foo.txt, baz.md) should be merged"
    );

    let (status, body) = ctx.get(&project, &main, "foo.txt").await;
    assert_eq!(status, StatusCode::OK, "foo.txt must be restored by merge");
    assert_eq!(body, b"foo-v2");

    let (status, body) = ctx.get(&project, &main, "baz.md").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"baz");

    let (status, body) = ctx.get(&project, &main, "bar.pdf").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"bar");

    let (status, body) = ctx.get(&project, &main, "bax.png").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"bax");
}

#[tokio::test]
async fn merge_tombstone_on_source_propagates_to_parent() {
    let ctx = Ctx::new().await;
    let (project, main) = ctx.create_project().await;

    ctx.put(&project, &main, "x.txt", b"x").await;

    let (dev, _) = ctx.fork_branch(&project, &main).await;
    ctx.delete_object(&project, &dev, "x.txt").await;

    ctx.merge(&project, &dev, &main).await;

    let (status, _) = ctx.get(&project, &main, "x.txt").await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "tombstone from source must propagate to target"
    );
}

#[tokio::test]
async fn merge_does_not_copy_inherited_objects() {
    let ctx = Ctx::new().await;
    let (project, main) = ctx.create_project().await;

    ctx.put(&project, &main, "base.txt", b"base").await;

    let (dev, _) = ctx.fork_branch(&project, &main).await;
    ctx.put(&project, &dev, "new.txt", b"new").await;

    let objects_merged = ctx.merge(&project, &dev, &main).await;
    assert_eq!(
        objects_merged, 1,
        "only direct dev write (new.txt) should be counted"
    );

    let (status, _) = ctx.get(&project, &main, "new.txt").await;
    assert_eq!(status, StatusCode::OK);
}
