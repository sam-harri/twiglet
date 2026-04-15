mod common;

use common::Ctx;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Runtime;

const SIZES: &[usize] = &[
    1_024,      // 1 KB
    16_384,     // 16 KB
    65_536,     // 64 KB
    262_144,    // 256 KB
    1_048_576,  // 1 MB
    4_194_304,  // 4 MB
    8_388_608,  // 8 MB
    16_777_216, // 16 MB
    33_554_432, // 32 MB
    67_108_864, // 64 MB
];

fn write_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("write_throughput");

    for &size in SIZES {
        group.throughput(Throughput::Bytes(size as u64));

        if size >= 8_388_608 {
            group.sample_size(10);
        }

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let ctx = rt.block_on(Ctx::new());
            let (project, branch) = rt.block_on(ctx.create_project());
            let body = vec![0u8; size];
            let mut n = 0u64;

            b.iter(|| {
                n += 1;
                rt.block_on(ctx.put(&project, &branch, &format!("obj-{n}.bin"), &body))
            });
        });
    }

    group.finish();
}

criterion_group!(benches, write_throughput);
criterion_main!(benches);
