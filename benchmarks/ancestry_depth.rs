mod common;

use common::Ctx;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use tokio::runtime::Runtime;

const DEPTHS: &[usize] = &[1, 2, 3, 4, 5, 6, 7, 8];

fn ancestry_depth(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("ancestry_depth");

    for &depth in DEPTHS {
        group.bench_with_input(BenchmarkId::from_parameter(depth), &depth, |b, &depth| {
            let ctx = rt.block_on(Ctx::new());
            let (project, root) = rt.block_on(ctx.create_project());
            rt.block_on(ctx.put(&project, &root, "needle.bin", &vec![42u8; 1024]));
            let deepest = rt.block_on(async {
                let mut current = root.clone();
                for _ in 0..depth {
                    let (child, _) = ctx.fork_branch(&project, &current).await;
                    current = child;
                }
                current
            });
            b.iter(|| rt.block_on(ctx.get(&project, &deepest, "needle.bin")));
        });
    }

    group.finish();
}

criterion_group!(benches, ancestry_depth);
criterion_main!(benches);
