//! Integration tests for ReadySet that create an in-process instance
//! of the controller and noria-server component. Tests in this file
//! should all use #[serial] to ensure that they are run serially.
//! These tests typically modify on process-level global objects, such
//! as the metrics recorder, and may exhibit flaky behavior if run
//! in parallel in the same process. Implement tests without this
//! requirement in integration.rs, which supports running tests in
//! parallel.

use std::collections::HashMap;
use std::convert::TryFrom;

use assert_approx_eq::assert_approx_eq;
use common::Index;
use dataflow::node::special::Base;
use dataflow::ops::union::{self, Union};
use dataflow::utils::make_columns;
use noria::get_metric;
use noria::metrics::{recorded, DumpedMetricValue, MetricsDump};
use noria_data::DataType;
use serial_test::serial;

use crate::integration_utils::*;
use crate::{get_col, Builder};

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn it_works_basic() {
    register_metric_recorder();
    let mut g = {
        let mut builder = Builder::for_tests();
        builder.set_sharding(None);
        builder.set_persistence(get_persistence_params("it_works_basic"));
        builder.set_allow_topk(true);
        builder.enable_packet_filters();
        builder.start_local()
    }
    .await
    .unwrap();

    let _ = g
        .migrate(|mig| {
            let a = mig.add_base(
                "a",
                make_columns(&["a", "b"]),
                Base::new().with_primary_key([0]),
            );
            let b = mig.add_base(
                "b",
                make_columns(&["a", "b"]),
                Base::new().with_primary_key([0]),
            );

            let mut emits = HashMap::new();
            emits.insert(a, vec![0, 1]);
            emits.insert(b, vec![0, 1]);
            let u = Union::new(emits, union::DuplicateMode::UnionAll).unwrap();
            let c = mig.add_ingredient("c", make_columns(&["a", "b"]), u);
            mig.maintain_anonymous(c, &Index::hash_map(vec![0]));
            (a, b, c)
        })
        .await;
    let mut metrics_client = initialize_metrics(&mut g).await;

    let mut cq = g.view("c").await.unwrap();
    let mut muta = g.table("a").await.unwrap();
    let mut mutb = g.table("b").await.unwrap();
    let id: DataType = 1.into();

    assert_eq!(muta.table_name(), "a");
    assert_eq!(muta.columns(), &["a", "b"]);

    // send a value on a
    muta.insert(vec![id.clone(), DataType::try_from(2i32).unwrap()])
        .await
        .unwrap();

    // send a value on a that won't be used.
    // We expect the egress node to drop it.
    muta.insert(vec![
        DataType::try_from(2i32).unwrap(),
        DataType::try_from(2i32).unwrap(),
    ])
    .await
    .unwrap();

    // Force the table to flush so we get a non zero table size metric
    muta.set_snapshot_mode(false).await.unwrap();

    // give it some time to propagate
    sleep().await;

    // send a query to c
    assert_eq!(
        cq.lookup(&[id.clone()], true).await.unwrap().into_vec(),
        vec![vec![1.into(), 2.into()]]
    );

    let metrics = metrics_client.get_metrics().await.unwrap();
    let metrics_dump = &metrics[0].metrics;
    assert_approx_eq!(
        get_counter(recorded::BASE_TABLE_LOOKUP_REQUESTS, metrics_dump),
        1.0
    );
    assert_approx_eq!(
        get_counter(recorded::EGRESS_NODE_DROPPED_PACKETS, metrics_dump),
        2.0
    );
    assert_approx_eq!(
        get_counter(recorded::EGRESS_NODE_SENT_PACKETS, metrics_dump),
        1.0
    );
    assert_eq!(
        get_metric!(metrics_dump, recorded::SERVER_VIEW_QUERY_MISS),
        Some(DumpedMetricValue::Counter(1.0))
    );
    assert_eq!(
        get_metric!(metrics_dump, recorded::SERVER_VIEW_QUERY_HIT),
        Some(DumpedMetricValue::Counter(0.0))
    );

    // update value again
    mutb.insert(vec![id.clone(), DataType::try_from(4i32).unwrap()])
        .await
        .unwrap();

    // give it some time to propagate
    sleep().await;

    // check that value was updated again
    let res = cq.lookup(&[id.clone()], true).await.unwrap().into_vec();
    assert!(res.iter().any(|r| *r == vec![id.clone(), 2.into()]));
    assert!(res.iter().any(|r| *r == vec![id.clone(), 4.into()]));

    // check that looking up columns by name works
    assert!(res.iter().all(|r| get_col!(cq, r, "a", i32) == 1));
    assert!(res.iter().any(|r| get_col!(cq, r, "b", i32) == 2));
    assert!(res.iter().any(|r| get_col!(cq, r, "b", i32) == 4));
    // same with index
    assert!(res.iter().all(|r| get_col!(cq, r, "a", DataType) == id));
    assert!(res
        .iter()
        .any(|r| get_col!(cq, r, "b", DataType) == 2.into()));
    assert!(res
        .iter()
        .any(|r| get_col!(cq, r, "b", DataType) == 4.into()));

    // This request does not hit the base table.
    let metrics = metrics_client.get_metrics().await.unwrap();
    let metrics_dump = &metrics[0].metrics;
    assert_approx_eq!(
        get_counter(recorded::BASE_TABLE_LOOKUP_REQUESTS, metrics_dump),
        1.0
    );
    // TODO(vlad): add a metric for embedded view cache hit instead
    /*assert_eq!(
        get_metric!(metrics_dump, recorded::SERVER_VIEW_QUERY_HIT),
        Some(DumpedMetricValue::Counter(1.0))
    );*/

    assert!(matches!(
        get_metric!(
            metrics_dump,
            recorded::DOMAIN_ESTIMATED_BASE_TABLE_SIZE_BYTES,
            "domain" => "0"
        )
        .unwrap(), DumpedMetricValue::Gauge(v) if v > 16.0));

    // Delete first record
    muta.delete(vec![id.clone()]).await.unwrap();

    // give it some time to propagate
    sleep().await;

    // send a query to c
    assert_eq!(
        cq.lookup(&[id.clone()], true).await.unwrap().into_vec(),
        vec![vec![1.into(), 4.into()]]
    );

    // Update second record
    // TODO(malte): disabled until we have update support on bases; the current way of doing this
    // is incompatible with bases' enforcement of the primary key uniqueness constraint.
    //mutb.update(vec![id.clone(), 6.into()]).await.unwrap();

    // give it some time to propagate
    //sleep().await;

    // send a query to c
    //assert_eq!(cq.lookup(&[id.clone()], true).await, Ok(vec![vec![1.into(), 6.into()]]));
}

fn get_external_requests_count(metrics_dump: &MetricsDump) -> f64 {
    get_counter(recorded::SERVER_CONTROLLER_REQUESTS, metrics_dump)
}

// FIXME(eta): this test is now slightly hacky after we started making more
//             external requests as part of the RPC refactor.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_metrics_client() {
    // Start a local instance of noria and connect the metrics client to it.
    // We assign it a different port than the rest of the tests to prevent
    // other tests impacting the metrics collected.
    register_metric_recorder();
    let builder = Builder::for_tests();
    let mut g = builder.start_local().await.unwrap();
    let mut client = initialize_metrics(&mut g).await;

    let metrics = client.get_metrics().await.unwrap();
    let metrics_dump = &metrics[0].metrics;
    let count = get_external_requests_count(metrics_dump);
    assert!(get_external_requests_count(metrics_dump) > 0.0);

    // Verify that this value is incrementing.
    let metrics = client.get_metrics().await.unwrap();
    let metrics_dump = &metrics[0].metrics;
    let second_count = get_external_requests_count(metrics_dump);
    assert!(get_external_requests_count(metrics_dump) > count);

    // Reset the metrics and verify the metrics actually reset.
    assert!(client.reset_metrics().await.is_ok());
    let metrics = client.get_metrics().await.unwrap();
    let metrics_dump = &metrics[0].metrics;
    assert!(get_external_requests_count(metrics_dump) < second_count);
}
