//! Slow cluster-manipulating benchmarks
//!
//! The benchmarks in this module are potentially quite slow (seconds to
//! minutes per iteration), and so are disabled by default. To run these tests,
//! the `--features` flag can be provided to cargo bench to enable them, for
//! example:
//!
//! ```ignore
//! cargo bench -p clustertest --features slow_bench
//! ```

#[cfg(not(feature = "slow_bench"))]
fn main() {}

#[cfg(feature = "slow_bench")]
criterion::criterion_group! {
    name = benches;
    // As these tests are potentially quite slow to begin with, set the sample
    // size to the lowest allowed by criterion
    config = criterion::Criterion::default().sample_size(10);
    targets = slow_bench::leader_failover, slow_bench::worker_failover, slow_bench::add_worker
}

#[cfg(feature = "slow_bench")]
criterion::criterion_main!(benches);

#[cfg(feature = "slow_bench")]
mod slow_bench {
    use clustertest::{DeploymentBuilder, ServerParams};
    use criterion::Criterion;
    use std::cell::RefCell;
    use std::sync::Arc;

    pub fn leader_failover(c: &mut Criterion) {
        let rt = tokio::runtime::Runtime::new().unwrap();

        let deployment = Arc::new(RefCell::new(rt.block_on(async {
            DeploymentBuilder::new("ct_bench_leader_failover")
                .add_server(ServerParams::default())
                .start()
                .await
                .unwrap()
        })));
        c.bench_function("leader_failover", |b| {
            let leader = &deployment.borrow().server_addrs()[0];

            rt.block_on(async {
                deployment
                    .borrow_mut()
                    .start_server(ServerParams::default(), true)
                    .await
                    .unwrap();
            });

            b.to_async(&rt).iter(|| async {
                deployment
                    .borrow_mut()
                    .kill_server(&leader, true)
                    .await
                    .unwrap();
            });
        });

        rt.block_on(async {
            deployment.borrow_mut().teardown().await.unwrap();
        });
    }

    pub fn worker_failover(c: &mut Criterion) {
        let rt = tokio::runtime::Runtime::new().unwrap();

        let deployment = Arc::new(RefCell::new(rt.block_on(async {
            DeploymentBuilder::new("ct_bench_worker_failover")
                .add_server(ServerParams::default())
                .start()
                .await
                .unwrap()
        })));
        c.bench_function("worker_failover", |b| {
            let worker = rt.block_on(async {
                deployment
                    .borrow_mut()
                    .start_server(ServerParams::default(), true)
                    .await
                    .unwrap()
            });

            b.to_async(&rt).iter(|| async {
                deployment
                    .borrow_mut()
                    .kill_server(&worker, true)
                    .await
                    .unwrap();
            });
        });

        rt.block_on(async {
            deployment.borrow_mut().teardown().await.unwrap();
        });
    }

    pub fn add_worker(c: &mut Criterion) {
        let rt = tokio::runtime::Runtime::new().unwrap();

        let deployment = Arc::new(RefCell::new(rt.block_on(async {
            DeploymentBuilder::new("ct_bench_add_worker")
                .add_server(ServerParams::default())
                .start()
                .await
                .unwrap()
        })));
        c.bench_function("add_worker", |b| {
            b.to_async(&rt).iter(|| async {
                deployment
                    .borrow_mut()
                    .start_server(ServerParams::default(), true)
                    .await
                    .unwrap()
            });
        });

        rt.block_on(async {
            deployment.borrow_mut().teardown().await.unwrap();
        });
    }
}
