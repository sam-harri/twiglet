mod common;

use common::Ctx;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use tokio::runtime::Runtime;

const DATASET_SIZES: &[usize] = &[100, 1_000, 10_000, 100_000];

fn fork_cost(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("fork_cost");

    group.sample_size(10);

    for &n in DATASET_SIZES {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let ctx = rt.block_on(Ctx::new());
            let (project, branch) = rt.block_on(ctx.create_project());
            let body = vec![0u8; 256];

            rt.block_on(async {
                for i in 0..n {
                    ctx.put(&project, &branch, &format!("obj-{i}.bin"), &body)
                        .await;
                }
            });

            b.iter(|| rt.block_on(ctx.fork_branch(&project, &branch)));
        });
    }

    group.finish();
}

criterion_group!(benches, fork_cost);
criterion_main!(benches);
