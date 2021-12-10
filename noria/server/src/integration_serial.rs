//! Integration tests for ReadySet that create an in-process instance
//! of the controller and noria-server component. Tests in this file
//! should all use #[serial] to ensure that they are run serially.
//! These tests typically modify on process-level global objects, such
//! as the metrics recorder, and may exhibit flaky behavior if run
//! in parallel in the same process. Implement tests without this
//! requirement in integration.rs, which supports running tests in
//! parallel.

use crate::get_col;
use crate::integration_utils::*;
use crate::Builder;
use crate::DataType;
use assert_approx_eq::assert_approx_eq;
use common::Index;
use dataflow::node::special::Base;
use dataflow::ops::union::{self, Union};
use noria::consensus::{Authority, LocalAuthority, LocalAuthorityStore};
use noria::internal::DomainIndex;
use noria::metrics::{recorded, DumpedMetricValue, MetricsDump};
use noria::{get_all_metrics, get_metric};
use petgraph::graph::NodeIndex;
use serial_test::serial;

use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn it_works_basic() {
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
            let a = mig.add_base("a", &["a", "b"], Base::new().with_primary_key([0]));
            let b = mig.add_base("b", &["a", "b"], Base::new().with_primary_key([0]));

            let mut emits = HashMap::new();
            emits.insert(a, vec![0, 1]);
            emits.insert(b, vec![0, 1]);
            let u = Union::new(emits, union::DuplicateMode::UnionAll).unwrap();
            let c = mig.add_ingredient("c", &["a", "b"], u);
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
        cq.lookup(&[id.clone()], true).await.unwrap(),
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
        get_metric!(
            metrics_dump,
            recorded::SERVER_VIEW_QUERY_RESULT,
            "result" => recorded::ViewQueryResultTag::Replay.value()
        )
        .unwrap(),
        DumpedMetricValue::Counter(1.0)
    );
    assert_eq!(
        get_metric!(
            metrics_dump,
            recorded::SERVER_VIEW_QUERY_RESULT,
            "result" => recorded::ViewQueryResultTag::ServedFromCache.value()
        ),
        None
    );

    // update value again
    mutb.insert(vec![id.clone(), DataType::try_from(4i32).unwrap()])
        .await
        .unwrap();

    // give it some time to propagate
    sleep().await;

    // check that value was updated again
    let res = cq.lookup(&[id.clone()], true).await.unwrap();
    assert!(res.iter().any(|r| r == vec![id.clone(), 2.into()]));
    assert!(res.iter().any(|r| r == vec![id.clone(), 4.into()]));

    // check that looking up columns by name works
    assert!(res.iter().all(|r| get_col!(r, "a", i32) == 1));
    assert!(res.iter().any(|r| get_col!(r, "b", i32) == 2));
    assert!(res.iter().any(|r| get_col!(r, "b", i32) == 4));
    // same with index
    assert!(res.iter().all(|r| get_col!(r, "a", DataType) == id));
    assert!(res.iter().any(|r| get_col!(r, "b", DataType) == 2.into()));
    assert!(res.iter().any(|r| get_col!(r, "b", DataType) == 4.into()));

    // This request does not hit the base table.
    let metrics = metrics_client.get_metrics().await.unwrap();
    let metrics_dump = &metrics[0].metrics;
    assert_approx_eq!(
        get_counter(recorded::BASE_TABLE_LOOKUP_REQUESTS, metrics_dump),
        1.0
    );
    assert_eq!(
        get_metric!(
            metrics_dump,
            recorded::SERVER_VIEW_QUERY_RESULT,
            "result" => recorded::ViewQueryResultTag::ServedFromCache.value()
        )
        .unwrap(),
        DumpedMetricValue::Counter(1.0)
    );

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
        cq.lookup(&[id.clone()], true).await.unwrap(),
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
    assert!(!client.reset_metrics().await.is_err());
    let metrics = client.get_metrics().await.unwrap();
    let metrics_dump = &metrics[0].metrics;
    assert!(get_external_requests_count(metrics_dump) < second_count);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn reader_replication() {
    let authority_store = Arc::new(LocalAuthorityStore::new());
    let w1_authority = Arc::new(Authority::from(LocalAuthority::new_with_store(
        authority_store.clone(),
    )));
    let w2_authority = Arc::new(Authority::from(LocalAuthority::new_with_store(
        authority_store,
    )));
    let cluster_name = "reader_replication";

    let mut w1 = build_custom(
        cluster_name,
        Some(DEFAULT_SHARDING),
        true,
        w1_authority,
        None,
        false,
        None,
    )
    .await;
    let mut metrics_client = initialize_metrics(&mut w1).await;

    let instances_standalone = w1.get_instances().await.unwrap();
    assert_eq!(1usize, instances_standalone.len());

    let w1_addr = instances_standalone[0].0.clone();

    let _w2 = build_custom(
        "reader_replication",
        Some(DEFAULT_SHARDING),
        false,
        w2_authority,
        None,
        false,
        None,
    )
    .await;

    while w1.get_instances().await.unwrap().len() < 2 {
        eprintln!("waiting");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let instances_cluster = w1.get_instances().await.unwrap();

    let w2_addr = instances_cluster
        .iter()
        .map(|(addr, _)| addr)
        .find(|&addr| addr != &w1_addr)
        .unwrap()
        .clone();

    w1.install_recipe(
        "
      CREATE TABLE t1 (id_1 int, id_2 int, val_1 int);
      QUERY q:
        SELECT *
        FROM t1;",
    )
    .await
    .unwrap();

    let metrics = metrics_client.get_metrics().await.unwrap();
    let metrics_dump = &metrics[0].metrics;
    let reader_added_metrics = get_all_metrics!(
        metrics_dump,
        recorded::DOMAIN_NODE_ADDED,
        "ntype" => "Reader"
    );
    assert_eq!(reader_added_metrics.len(), 1);

    w1.table("t1")
        .await
        .unwrap()
        .insert(vec![1i64.into(), 2i64.into(), 3i64.into()])
        .await
        .unwrap();

    let worker_without_reader = if w1
        .view_from_workers("q", vec![w1_addr.clone()])
        .await
        .is_err()
    {
        w1_addr
    } else {
        w2_addr
    };

    let info_pre_replication = w1.get_info().await.unwrap();
    let domains_pre_replication =
        info_pre_replication
            .workers
            .values()
            .fold(HashMap::new(), |mut acc, entry| {
                acc.extend(entry.iter().map(|e| (e.0 .0, e.1)));
                acc
            });

    let repl_result = w1
        .replicate_readers(vec!["q".to_owned()], Some(worker_without_reader.clone()))
        .await
        .unwrap();

    let metrics = metrics_client.get_metrics().await.unwrap();
    let metrics_dump = &metrics[0].metrics;
    let reader_added_metrics = get_all_metrics!(
        metrics_dump,
        recorded::DOMAIN_NODE_ADDED,
        "ntype" => "Reader"
    );
    assert_eq!(reader_added_metrics.len(), 2);

    let readers = repl_result.new_readers;
    assert!(readers.contains_key("q"));

    let info_post_replication = w1.get_info().await.unwrap();
    let domains_from_worker: HashMap<DomainIndex, Vec<NodeIndex>> = info_post_replication
        .workers
        .get(&worker_without_reader)
        .unwrap()
        .iter()
        .fold(HashMap::new(), |mut acc, (dk, nodes)| {
            acc.entry(dk.0)
                .or_insert_with(Vec::new)
                .extend(nodes.iter());
            acc
        });

    let result = w1.view_from_workers("q", vec![worker_without_reader]).await;
    assert!(result.is_ok());
    let mut records: Vec<Vec<DataType>> = result
        .unwrap()
        .lookup(&[0.into()], true)
        .await
        .unwrap()
        .into();
    records.sort();
    assert_eq!(records, vec![vec![1.into(), 2.into(), 3.into()]]);
    for (domain, nodes) in readers.get("q").unwrap().iter() {
        assert!(domains_pre_replication.get(domain).is_none());
        let domain_nodes_opt = domains_from_worker.get(domain);
        assert!(domain_nodes_opt.is_some());
        let domain_nodes = domain_nodes_opt.unwrap();
        for node in nodes {
            assert!(domain_nodes.contains(node))
        }
    }
}
