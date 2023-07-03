//! Integration tests for ReadySet that create an in-process instance
//! of the controller and readyset-server component. These tests may be
//! run in parallel. For tests that modify process-level global objects
//! consider using integration_serial and having the tests run serially
//! to prevent flaky behavior.
#![allow(clippy::many_single_char_names)]

use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;
use std::{iter, thread};

use chrono::NaiveDate;
use common::Index;
use dataflow::node::special::Base;
use dataflow::ops::grouped::aggregate::Aggregation;
use dataflow::ops::identity::Identity;
use dataflow::ops::join::{Join, JoinType};
use dataflow::ops::project::Project;
use dataflow::ops::union::{self, Union};
use dataflow::ops::Side;
use dataflow::utils::{dataflow_column, make_columns};
use dataflow::{
    BinaryOperator, DurabilityMode, Expr as DfExpr, PersistenceParameters, ReaderProcessing,
};
use futures::{join, StreamExt};
use itertools::Itertools;
use nom_sql::{
    parse_create_table, parse_create_view, parse_query, parse_select_statement, OrderType,
    Relation, SqlQuery,
};
use readyset_client::consensus::{Authority, LocalAuthority, LocalAuthorityStore};
use readyset_client::consistency::Timestamp;
use readyset_client::internal::LocalNodeIndex;
use readyset_client::recipe::changelist::{Change, ChangeList, CreateCache};
use readyset_client::{KeyComparison, Modification, SchemaType, ViewPlaceholder, ViewQuery};
use readyset_data::{DfType, DfValue, Dialect};
use readyset_errors::ReadySetError::{
    self, MigrationPlanFailed, RpcFailed, SelectQueryCreationFailed,
};
use readyset_util::eventually;
use readyset_util::shutdown::ShutdownSender;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rusty_fork::rusty_fork_test;
use tempfile::TempDir;
use test_utils::skip_with_flaky_finder;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use vec1::vec1;

use crate::controller::sql::SqlIncorporator;
use crate::integration_utils::*;
use crate::{get_col, Builder};

#[tokio::test(flavor = "multi_thread")]
async fn it_completes() {
    readyset_tracing::init_test_logging();

    let mut builder = Builder::for_tests();
    builder.set_sharding(Some(DEFAULT_SHARDING));
    builder.set_persistence(get_persistence_params("it_completes"));
    let (mut g, shutdown_tx) = builder.start_local().await.unwrap();

    // do some stuff (== it_works_basic)
    let (a, b) = g
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
            (a, b)
        })
        .await;

    let mut cq = g.view("c").await.unwrap().into_reader_handle().unwrap();
    let mut muta = g.table_by_index(a).await.unwrap();
    let mut mutb = g.table_by_index(b).await.unwrap();
    let id: DfValue = 1.into();

    assert_eq!(*muta.table_name(), "a".into());
    assert_eq!(muta.columns(), &["a", "b"]);

    muta.insert(vec![id.clone(), 2.into()]).await.unwrap();
    sleep().await;
    assert_eq!(
        cq.lookup(&[id.clone()], true).await.unwrap().into_vec(),
        vec![vec![1.into(), 2.into()]]
    );
    mutb.insert(vec![id.clone(), 4.into()]).await.unwrap();
    sleep().await;
    let res = cq.lookup(&[id.clone()], true).await.unwrap().into_vec();
    assert!(res.iter().any(|r| *r == vec![id.clone(), 2.into()]));
    assert!(res.iter().any(|r| *r == vec![id.clone(), 4.into()]));
    muta.delete(vec![id.clone()]).await.unwrap();
    sleep().await;
    assert_eq!(
        cq.lookup(&[id.clone()], true).await.unwrap().into_vec(),
        vec![vec![1.into(), 4.into()]]
    );

    // wait for exit
    eprintln!("waiting for completion");
    shutdown_tx.shutdown().await;
}

fn timestamp(pairs: Vec<(u32, u64)>) -> Timestamp {
    let mut t = Timestamp::default();
    for p in pairs {
        t.map.insert(LocalNodeIndex::make(p.0), p.1);
    }

    t
}

// Tests that a write to a single base table accompanied by a timestamp
// update propagates to the reader nodes. Tests that a read with a timestamp
// that can be satisfied by the reader node succeeds and that a timestamp
// that cannot be satisfied does not. If the reader node had not received
// a timestamp, the read would not be satisfied, unless there is a bug with
// timestamp satisfiability.
#[tokio::test(flavor = "multi_thread")]
async fn test_timestamp_propagation_simple() {
    let (mut g, shutdown_tx) = start_simple_unsharded("test_timestamp_propagation_simple").await;

    // Create a base table "a" with columns "a", and "b".
    let a = g
        .migrate(|mig| {
            // Adds a base table with fields "a", "b".
            let a = mig.add_base(
                "a",
                make_columns(&["a", "b"]),
                Base::new().with_primary_key([0]),
            );

            let mut emits = HashMap::new();
            emits.insert(a, vec![0, 1]);
            let u = Union::new(emits, union::DuplicateMode::UnionAll).unwrap();
            let c = mig.add_ingredient("c", make_columns(&["a"]), u);
            mig.maintain_anonymous(c, &Index::hash_map(vec![0]));
            a
        })
        .await;

    let mut cq = g.view("c").await.unwrap().into_reader_handle().unwrap();
    let mut muta = g.table_by_index(a).await.unwrap();

    // Insert <1, 2> into table "a".
    let id: DfValue = 1.into();
    let value: DfValue = 2.into();
    muta.insert(vec![id.clone(), value.clone()]).await.unwrap();

    // Create and pass the timestamp to the base table node.
    let t = timestamp(vec![(0, 1)]);
    muta.update_timestamp(t.clone()).await.unwrap();

    // Successful read with a timestamp that the reader node timestamp
    // satisfies. We begin with a blocking read as the data is not
    // materialized at the reader.
    let res = cq
        .raw_lookup(ViewQuery::from((
            vec![KeyComparison::Equal(vec1![id.clone()])],
            true,
            Some(t.clone()),
        )))
        .await
        .unwrap()
        .into_vec();

    assert_eq!(res, vec![vec![id.clone(), value.clone()]]);

    // Perform a read with a timestamp the reader cannot satisfy.
    assert!(matches!(
        cq.raw_lookup(ViewQuery::from((
            vec![KeyComparison::Equal(vec1![id.clone()])],
            false,
            // The timestamp at the reader node { 0: 4 }, does not
            // satisfy this timestamp.
            Some(timestamp(vec![(1, 4)]))
        )))
        .await,
        Err(ReadySetError::ReaderMissingKey)
    ));

    shutdown_tx.shutdown().await;
}

// Simulate writes from two clients.
#[tokio::test(flavor = "multi_thread")]
async fn test_timestamp_propagation_multitable() {
    let (mut g, shutdown_tx) =
        start_simple_unsharded("test_timestamp_propagation_multitable").await;

    // Create two base tables "a" and "b" with columns "a", and "b".
    let (a, b) = g
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
            (a, b)
        })
        .await;

    let mut cq = g.view("c").await.unwrap().into_reader_handle().unwrap();
    let mut muta = g.table_by_index(a).await.unwrap();
    let mut mutb = g.table_by_index(b).await.unwrap();

    // Insert some data into table a.
    muta.insert(vec![DfValue::Int(1), DfValue::Int(2)])
        .await
        .unwrap();

    // Update timestamps to simulate two clients performing writes
    // to two base tables at the same time.
    //
    // Client 1's update timestamp calls.
    muta.update_timestamp(timestamp(vec![(0, 6)]))
        .await
        .unwrap();
    mutb.update_timestamp(timestamp(vec![(1, 5)]))
        .await
        .unwrap();

    // Client 2's update timestamp calls.
    muta.update_timestamp(timestamp(vec![(0, 5)]))
        .await
        .unwrap();
    mutb.update_timestamp(timestamp(vec![(1, 6)]))
        .await
        .unwrap();

    // Successful read with a timestamp that the reader node timestamp
    // satisfies. We begin with a blocking read as the data is not
    // materialized at the reader. In order for the timestamp to satisfy
    // the timestamp { 0: 6, 1: 6 }, each dataflow node will have to had
    // calculated the max over the timestamp entries they had seen for
    // each table.
    let res = cq
        .raw_lookup(ViewQuery::from((
            vec![KeyComparison::Equal(vec1![DfValue::Int(1)])],
            true,
            Some(timestamp(vec![(0, 6), (1, 6)])),
        )))
        .await
        .unwrap()
        .into_vec();

    assert_eq!(res, vec![vec![DfValue::Int(1), DfValue::Int(2)]]);

    // Perform a non-blocking read with a timestamp that the reader should not
    // be able to satisfy. A non-blocking read of a satisfiable timestamp would
    // suceed here due to the previous read materializing the data.
    assert!(matches!(
        cq.raw_lookup(ViewQuery::from((
            vec![KeyComparison::Equal(vec1![DfValue::Int(1)])],
            false,
            Some(timestamp(vec![(0, 6), (1, 7)])),
        )))
        .await,
        Err(ReadySetError::ReaderMissingKey)
    ));

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "Ignoring sharded tests"]
async fn sharded_shuffle() {
    let (mut g, shutdown_tx) = start_simple("sharded_shuffle").await;

    // in this test, we have a single sharded base node that is keyed on one column, and a sharded
    // reader that is keyed by a different column. this requires a shuffle. we want to make sure
    // that that shuffle happens correctly.

    let a = g
        .migrate(|mig| {
            let a = mig.add_base(
                "base",
                make_columns(&["id", "non_id"]),
                Base::new().with_primary_key([0]),
            );
            mig.maintain_anonymous(a, &Index::hash_map(vec![1]));
            a
        })
        .await;

    eprintln!("{}", g.graphviz().await.unwrap());

    let mut base = g.table_by_index(a).await.unwrap();
    let mut view = g.view("base").await.unwrap().into_reader_handle().unwrap();

    // make sure there is data on >1 shard, and that we'd get multiple rows by querying the reader
    // for a single key.
    base.perform_all((0..100).map(|i| vec![i.into(), DfValue::Int(1)]))
        .await
        .unwrap();

    sleep().await;

    // moment of truth
    let rows = view
        .lookup(&[DfValue::Int(1)], true)
        .await
        .unwrap()
        .into_vec();
    assert_eq!(rows.len(), 100);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn broad_recursing_upquery() {
    let nshards = 16;
    let (mut g, shutdown_tx) = build("bru", Some(nshards), None).await;

    // our goal here is to have a recursive upquery such that both levels of the upquery require
    // contacting _all_ shards. in this setting, any miss at the leaf requires the upquery to go to
    // all shards of the intermediate operator, and each miss there requires an upquery to each
    // shard of the top level. as a result, we would expect every base to receive 2 upqueries for
    // the same key, and a total of n^2+n upqueries. crucially, what we want to test is that the
    // partial logic correctly manages all these requests, and the resulting responses (especially
    // at the shard mergers). to achieve this, we're going to use this layout:
    //
    // base x    base y [sharded by a]
    //   |         |
    //   +----+----+ [lookup by b]
    //        |
    //      join [sharded by b]
    //        |
    //     reader [sharded by c]
    //
    // we basically _need_ a join in order to get this layout, since only joins allow us to
    // introduce a new sharding without also dropping all columns that are not the sharding column
    // (like aggregations would). with an aggregation for example, the downstream view could not be
    // partial, since it would have no way to know the partial key to upquery for given a miss,
    // since the miss would be on an _output_ column of the aggregation. we _could_ use a
    // multi-column aggregation group by, but those have their own problems that we do not want to
    // exercise here.
    //
    // we're also going to make the join a left join so that we know the upquery will go to base_x.

    let (x, _y) = g
        .migrate(|mig| {
            // bases, both sharded by their first column
            let x = mig.add_base(
                "base_x",
                make_columns(&["base_col", "join_col", "reader_col"]),
                Base::new().with_primary_key([0]),
            );
            let y = mig.add_base(
                "base_y",
                make_columns(&["id"]),
                Base::new().with_primary_key([0]),
            );
            // join, sharded by the join column, which is be the second column on x
            let join = mig.add_ingredient(
                "join",
                make_columns(&["base_col", "join_col", "reader_col"]),
                Join::new(
                    x,
                    y,
                    JoinType::Left,
                    vec![(1, 0)],
                    vec![(Side::Left, 0), (Side::Left, 1), (Side::Left, 2)],
                ),
            );
            // reader, sharded by the lookup column, which is the third column on x
            mig.maintain(
                "reader".into(),
                join,
                &Index::hash_map(vec![2]),
                Default::default(),
                Default::default(),
            );
            (x, y)
        })
        .await;

    eprintln!("{}", g.graphviz().await.unwrap());

    let mut base_x = g.table_by_index(x).await.unwrap();
    let mut reader = g
        .view("reader")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    // we want to make sure that all the upqueries recurse all the way to cause maximum headache
    // for the partial logic. we do this by ensuring that every shard at every operator has at
    // least one record. we also ensure that we can get _all_ the rows by querying a single key on
    // the reader.
    let n = 10_000;
    base_x
        .perform_all((0..n).map(|i| {
            vec![
                DfValue::Int(i),
                DfValue::Int(i % nshards as i64),
                DfValue::Int(1),
            ]
        }))
        .await
        .unwrap();

    sleep().await;

    // moment of truth
    let rows = reader
        .lookup(&[DfValue::Int(1)], true)
        .await
        .unwrap()
        .into_vec();
    assert_eq!(rows.len(), n as usize);
    for i in 0..n {
        assert!(rows
            .iter()
            .any(|row| get_col!(reader, row, "base_col", i64) == i));
    }

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn base_mutation() {
    use readyset_client::{Modification, Operation};

    let (mut g, shutdown_tx) = start_simple_unsharded("base_mutation").await;
    let a = g
        .migrate(|mig| {
            let a = mig.add_base(
                "a",
                make_columns(&["a", "b"]),
                Base::new().with_primary_key([0]),
            );
            mig.maintain_anonymous(a, &Index::hash_map(vec![0]));
            a
        })
        .await;

    let mut read = g.view("a").await.unwrap().into_reader_handle().unwrap();
    let mut write = g.table_by_index(a).await.unwrap();

    // insert a new record
    write.insert(vec![1.into(), 2.into()]).await.unwrap();
    sleep().await;
    assert_eq!(
        read.lookup(&[1.into()], true).await.unwrap().into_vec(),
        vec![vec![1.into(), 2.into()]]
    );

    // update that record in place (set)
    write
        .update(vec![1.into()], vec![(1, Modification::Set(3.into()))])
        .await
        .unwrap();
    sleep().await;
    assert_eq!(
        read.lookup(&[1.into()], true).await.unwrap().into_vec(),
        vec![vec![1.into(), 3.into()]]
    );

    // update that record in place (add)
    write
        .update(
            vec![1.into()],
            vec![(1, Modification::Apply(Operation::Add, 1.into()))],
        )
        .await
        .unwrap();
    sleep().await;
    assert_eq!(
        read.lookup(&[1.into()], true).await.unwrap().into_vec(),
        vec![vec![1.into(), 4.into()]]
    );

    // insert or update should update
    write
        .insert_or_update(
            vec![1.into(), 2.into()],
            vec![(1, Modification::Apply(Operation::Add, 1.into()))],
        )
        .await
        .unwrap();
    sleep().await;
    assert_eq!(
        read.lookup(&[1.into()], true).await.unwrap().into_vec(),
        vec![vec![1.into(), 5.into()]]
    );

    // delete should, well, delete
    write.delete(vec![1.into()]).await.unwrap();
    sleep().await;
    assert!(read
        .lookup(&[1.into()], true)
        .await
        .unwrap()
        .into_vec()
        .is_empty());

    // insert or update should insert
    write
        .insert_or_update(
            vec![1.into(), 2.into()],
            vec![(1, Modification::Apply(Operation::Add, 1.into()))],
        )
        .await
        .unwrap();
    sleep().await;
    assert_eq!(
        read.lookup(&[1.into()], true).await.unwrap().into_vec(),
        vec![vec![1.into(), 2.into()]]
    );

    // truncate deletes everything
    write.truncate().await.unwrap();
    sleep().await;
    assert_eq!(
        read.lookup(&[1.into()], true).await.unwrap().into_vec(),
        Vec::<Vec<DfValue>>::new()
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn shared_interdomain_ancestor() {
    // set up graph
    let (mut g, shutdown_tx) = start_simple_unsharded("shared_interdomain_ancestor").await;
    let a = g
        .migrate(|mig| {
            let a = mig.add_base("a", make_columns(&["a", "b"]), Base::default());

            let mut emits = HashMap::new();
            emits.insert(a, vec![0, 1]);

            let u = Union::new(emits.clone(), union::DuplicateMode::UnionAll).unwrap();
            let b = mig.add_ingredient("b", make_columns(&["a", "b"]), u);
            mig.maintain_anonymous(b, &Index::hash_map(vec![0]));

            let u = Union::new(emits, union::DuplicateMode::UnionAll).unwrap();
            let c = mig.add_ingredient("c", make_columns(&["a", "b"]), u);
            mig.maintain_anonymous(c, &Index::hash_map(vec![0]));

            a
        })
        .await;

    let mut bq = g.view("b").await.unwrap().into_reader_handle().unwrap();
    let mut cq = g.view("c").await.unwrap().into_reader_handle().unwrap();
    let mut muta = g.table_by_index(a).await.unwrap();
    let id: DfValue = 1.into();

    // send a value on a
    muta.insert(vec![id.clone(), 2.into()]).await.unwrap();
    sleep().await;
    assert_eq!(
        bq.lookup(&[id.clone()], true).await.unwrap().into_vec(),
        vec![vec![id.clone(), 2.into()]]
    );
    assert_eq!(
        cq.lookup(&[id.clone()], true).await.unwrap().into_vec(),
        vec![vec![id.clone(), 2.into()]]
    );

    // update value again
    let id: DfValue = 2.into();
    muta.insert(vec![id.clone(), 4.into()]).await.unwrap();
    sleep().await;
    assert_eq!(
        bq.lookup(&[id.clone()], true).await.unwrap().into_vec(),
        vec![vec![id.clone(), 4.into()]]
    );
    assert_eq!(
        cq.lookup(&[id.clone()], true).await.unwrap().into_vec(),
        vec![vec![id.clone(), 4.into()]]
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn it_works_w_mat() {
    // set up graph
    let (mut g, shutdown_tx) = start_simple_unsharded("it_works_w_mat").await;
    let (a, b) = g
        .migrate(|mig| {
            let a = mig.add_base("a", make_columns(&["a", "b"]), Base::default());
            let b = mig.add_base("b", make_columns(&["a", "b"]), Base::default());

            let mut emits = HashMap::new();
            emits.insert(a, vec![0, 1]);
            emits.insert(b, vec![0, 1]);
            let u = Union::new(emits, union::DuplicateMode::UnionAll).unwrap();
            let c = mig.add_ingredient("c", make_columns(&["a", "b"]), u);
            mig.maintain_anonymous(c, &Index::hash_map(vec![0]));
            (a, b)
        })
        .await;

    let mut cq = g.view("c").await.unwrap().into_reader_handle().unwrap();
    let mut muta = g.table_by_index(a).await.unwrap();
    let mut mutb = g.table_by_index(b).await.unwrap();
    let id: DfValue = 1.into();

    // send a few values on a
    muta.insert(vec![id.clone(), 1.into()]).await.unwrap();
    muta.insert(vec![id.clone(), 2.into()]).await.unwrap();
    muta.insert(vec![id.clone(), 3.into()]).await.unwrap();

    // give them some time to propagate
    sleep().await;

    // send a query to c
    // we should see all the a values
    let res = cq.lookup(&[id.clone()], true).await.unwrap().into_vec();
    assert_eq!(res.len(), 3);
    assert!(res.iter().any(|r| *r == vec![id.clone(), 1.into()]));
    assert!(res.iter().any(|r| *r == vec![id.clone(), 2.into()]));
    assert!(res.iter().any(|r| *r == vec![id.clone(), 3.into()]));

    // update value again (and again send some secondary updates)
    mutb.insert(vec![id.clone(), 4.into()]).await.unwrap();
    mutb.insert(vec![id.clone(), 5.into()]).await.unwrap();
    mutb.insert(vec![id.clone(), 6.into()]).await.unwrap();

    // give it some time to propagate
    sleep().await;

    // check that value was updated again
    let res = cq.lookup(&[id.clone()], true).await.unwrap().into_vec();
    assert_eq!(res.len(), 6);
    assert!(res.iter().any(|r| *r == vec![id.clone(), 1.into()]));
    assert!(res.iter().any(|r| *r == vec![id.clone(), 2.into()]));
    assert!(res.iter().any(|r| *r == vec![id.clone(), 3.into()]));
    assert!(res.iter().any(|r| *r == vec![id.clone(), 4.into()]));
    assert!(res.iter().any(|r| *r == vec![id.clone(), 5.into()]));
    assert!(res.iter().any(|r| *r == vec![id.clone(), 6.into()]));

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn it_works_w_partial_mat() {
    // set up graph
    let (mut g, shutdown_tx) = start_simple_unsharded("it_works_w_partial_mat").await;
    let (a, b) = g
        .migrate(|mig| {
            let a = mig.add_base("a", make_columns(&["a", "b"]), Base::default());
            let b = mig.add_base("b", make_columns(&["a", "b"]), Base::default());
            (a, b)
        })
        .await;

    let mut muta = g.table_by_index(a).await.unwrap();
    let id: DfValue = 1.into();

    // send a few values on a
    muta.insert(vec![id.clone(), 1.into()]).await.unwrap();
    muta.insert(vec![id.clone(), 2.into()]).await.unwrap();
    muta.insert(vec![id.clone(), 3.into()]).await.unwrap();

    // give it some time to propagate
    sleep().await;

    let _ = g
        .migrate(move |mig| {
            let mut emits = HashMap::new();
            emits.insert(a, vec![0, 1]);
            emits.insert(b, vec![0, 1]);
            let u = Union::new(emits, union::DuplicateMode::UnionAll).unwrap();
            let c = mig.add_ingredient("c", make_columns(&["a", "b"]), u);
            mig.maintain_anonymous(c, &Index::hash_map(vec![0]));
            c
        })
        .await;

    // give it some time to propagate
    sleep().await;

    let mut cq = g.view("c").await.unwrap().into_reader_handle().unwrap();

    // because the reader is partial, we should have no key until we read
    assert_eq!(cq.len().await.unwrap(), 0);

    // now do some reads
    let res = cq.lookup(&[id.clone()], true).await.unwrap().into_vec();
    assert_eq!(res.len(), 3);
    assert!(res.iter().any(|r| *r == vec![id.clone(), 1.into()]));
    assert!(res.iter().any(|r| *r == vec![id.clone(), 2.into()]));
    assert!(res.iter().any(|r| *r == vec![id.clone(), 3.into()]));

    // should have one key in the reader now
    assert_eq!(cq.len().await.unwrap(), 1);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn it_works_w_partial_mat_below_empty() {
    // set up graph with all nodes added in a single migration. The base tables are therefore empty
    // for now.
    let (mut g, shutdown_tx) = start_simple_unsharded("it_works_w_partial_mat_below_empty").await;
    let (a, _b) = g
        .migrate(|mig| {
            let a = mig.add_base("a", make_columns(&["a", "b"]), Base::default());
            let b = mig.add_base("b", make_columns(&["a", "b"]), Base::default());
            let mut emits = HashMap::new();
            emits.insert(a, vec![0, 1]);
            emits.insert(b, vec![0, 1]);
            let u = Union::new(emits, union::DuplicateMode::UnionAll).unwrap();
            let c = mig.add_ingredient("c", make_columns(&["a", "b"]), u);
            mig.maintain_anonymous(c, &Index::hash_map(vec![0]));
            (a, b)
        })
        .await;

    let mut muta = g.table_by_index(a).await.unwrap();
    let id: DfValue = 1.into();

    // send a few values on a
    muta.insert(vec![id.clone(), 1.into()]).await.unwrap();
    muta.insert(vec![id.clone(), 2.into()]).await.unwrap();
    muta.insert(vec![id.clone(), 3.into()]).await.unwrap();

    // give it some time to propagate
    sleep().await;

    let mut cq = g.view("c").await.unwrap().into_reader_handle().unwrap();

    // despite the empty base tables, we'll make the reader partial and therefore we should have no
    // key until we read
    assert_eq!(cq.len().await.unwrap(), 0);

    // now do some reads
    let res = cq.lookup(&[id.clone()], true).await.unwrap().into_vec();
    assert_eq!(res.len(), 3);
    assert!(res.iter().any(|r| *r == vec![id.clone(), 1.into()]));
    assert!(res.iter().any(|r| *r == vec![id.clone(), 2.into()]));
    assert!(res.iter().any(|r| *r == vec![id.clone(), 3.into()]));

    // should have one key in the reader now
    assert_eq!(cq.len().await.unwrap(), 1);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn it_works_deletion() {
    // set up graph
    let (mut g, shutdown_tx) = start_simple_unsharded("it_works_deletion").await;
    let (a, b) = g
        .migrate(|mig| {
            let a = mig.add_base(
                "a",
                make_columns(&["x", "y"]),
                Base::new().with_primary_key([1]),
            );
            let b = mig.add_base(
                "b",
                make_columns(&["_", "x", "y"]),
                Base::new().with_primary_key([2]),
            );

            let mut emits = HashMap::new();
            emits.insert(a, vec![0, 1]);
            emits.insert(b, vec![1, 2]);
            let u = Union::new(emits, union::DuplicateMode::UnionAll).unwrap();
            let c = mig.add_ingredient("c", make_columns(&["x", "y"]), u);
            mig.maintain_anonymous(c, &Index::hash_map(vec![0]));
            (a, b)
        })
        .await;

    let mut cq = g.view("c").await.unwrap().into_reader_handle().unwrap();
    let mut muta = g.table_by_index(a).await.unwrap();
    let mut mutb = g.table_by_index(b).await.unwrap();

    // send a value on a
    muta.insert(vec![1.into(), 2.into()]).await.unwrap();
    sleep().await;
    assert_eq!(
        cq.lookup(&[1.into()], true).await.unwrap().into_vec(),
        vec![vec![1.into(), 2.into()]]
    );

    // send a value on b
    mutb.insert(vec![0.into(), 1.into(), 4.into()])
        .await
        .unwrap();
    sleep().await;

    let res = cq.lookup(&[1.into()], true).await.unwrap().into_vec();
    assert_eq!(res.len(), 2);
    assert!(res.contains(&vec![1.into(), 2.into()]));
    assert!(res.contains(&vec![1.into(), 4.into()]));

    // delete first value
    muta.delete(vec![2.into()]).await.unwrap();
    sleep().await;
    assert_eq!(
        cq.lookup(&[1.into()], true).await.unwrap().into_vec(),
        vec![vec![1.into(), 4.into()]]
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_row() {
    let (mut g, shutdown_tx) = start_simple_unsharded("delete_row").await;
    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE t1 (x int, y int, z int);
             CREATE CACHE all_rows FROM SELECT * FROM t1;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let mut t = g.table("t1").await.unwrap();
    let mut all_rows = g
        .view("all_rows")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    t.insert_many(vec![
        vec![DfValue::from(1), DfValue::from(2), DfValue::from(3)],
        vec![DfValue::from(1), DfValue::from(2), DfValue::from(3)],
        vec![DfValue::from(4), DfValue::from(5), DfValue::from(6)],
    ])
    .await
    .unwrap();

    t.delete_row(vec![DfValue::from(1), DfValue::from(2), DfValue::from(3)])
        .await
        .unwrap();

    // Let the delete propagate
    sleep().await;

    assert_eq!(
        all_rows.lookup(&[0.into()], true).await.unwrap().into_vec(),
        vec![
            vec![DfValue::from(1), DfValue::from(2), DfValue::from(3)],
            vec![DfValue::from(4), DfValue::from(5), DfValue::from(6)],
        ]
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn it_works_with_sql_recipe() {
    let (mut g, shutdown_tx) = start_simple_unsharded("it_works_with_sql_recipe").await;
    let sql = "
        CREATE TABLE Car (id int, brand varchar(255), PRIMARY KEY(id));
        CREATE CACHE CountCars FROM SELECT COUNT(*) FROM Car WHERE brand = ?;
    ";
    g.extend_recipe(ChangeList::from_str(sql, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    let mut mutator = g.table("Car").await.unwrap();
    let mut getter = g
        .view("CountCars")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    assert_eq!(*mutator.table_name(), "Car".into());
    assert_eq!(mutator.columns(), &["id", "brand"]);

    let brands = vec!["Volvo", "Volvo", "Volkswagen"];
    for (i, &brand) in brands.iter().enumerate() {
        mutator
            .insert(vec![i.into(), brand.try_into().unwrap()])
            .await
            .unwrap();
    }

    // Let writes propagate:
    sleep().await;

    // Retrieve the result of the count query:
    let result = getter
        .lookup(&["Volvo".try_into().unwrap()], true)
        .await
        .unwrap()
        .into_vec();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][0], 2.into());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn it_works_with_vote() {
    let (mut g, shutdown_tx) = start_simple_unsharded("it_works_with_vote").await;
    let sql = "
        # base tables
        CREATE TABLE Article (id int, title varchar(255), PRIMARY KEY(id));
        CREATE TABLE Vote (article_id int, user int);

        # read queries
        CREATE CACHE ArticleWithVoteCount FROM SELECT Article.id, title, VoteCount.votes AS votes \
                    FROM Article \
                    LEFT JOIN (SELECT Vote.article_id, COUNT(user) AS votes \
                               FROM Vote GROUP BY Vote.article_id) AS VoteCount \
                    ON (Article.id = VoteCount.article_id) WHERE Article.id = ?;
    ";

    g.extend_recipe(ChangeList::from_str(sql, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();
    let mut article = g.table("Article").await.unwrap();
    let mut vote = g.table("Vote").await.unwrap();
    let mut awvc = g
        .view("ArticleWithVoteCount")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    article
        .insert(vec![0i64.into(), "Article".try_into().unwrap()])
        .await
        .unwrap();
    article
        .insert(vec![1i64.into(), "Article".try_into().unwrap()])
        .await
        .unwrap();
    vote.insert(vec![0i64.into(), 0.into()]).await.unwrap();

    sleep().await;

    let rs = awvc.lookup(&[0i64.into()], true).await.unwrap().into_vec();
    assert_eq!(rs.len(), 1);
    assert_eq!(
        rs[0],
        vec![0i64.into(), "Article".try_into().unwrap(), 1.into()]
    );

    let empty = awvc.lookup(&[1i64.into()], true).await.unwrap().into_vec();
    assert_eq!(empty.len(), 1);
    assert_eq!(
        empty[0],
        vec![1i64.into(), "Article".try_into().unwrap(), DfValue::None]
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn it_works_with_identical_queries() {
    let (mut g, shutdown_tx) = start_simple_unsharded("it_works_with_identical_queries").await;
    let sql = "
        CREATE TABLE Article (aid int, PRIMARY KEY(aid));
        CREATE CACHE aq1 FROM SELECT Article.* FROM Article WHERE Article.aid = ?;
        CREATE CACHE aq2 FROM SELECT Article.* FROM Article WHERE Article.aid = ?;
    ";

    g.extend_recipe(ChangeList::from_str(sql, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();
    let mut article = g.table("Article").await.unwrap();
    let mut aq1 = g.view("aq1").await.unwrap().into_reader_handle().unwrap();
    let mut aq2 = g.view("aq2").await.unwrap().into_reader_handle().unwrap();

    let aid = 1u64;

    assert!(aq1
        .lookup(&[aid.into()], true)
        .await
        .unwrap()
        .into_vec()
        .is_empty());
    assert!(aq2
        .lookup(&[aid.into()], true)
        .await
        .unwrap()
        .into_vec()
        .is_empty());
    article.insert(vec![aid.into()]).await.unwrap();
    sleep().await;

    let result = aq2.lookup(&[aid.into()], true).await.unwrap().into_vec();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], vec![aid.into()]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn it_works_with_double_query_through() {
    let (mut g, shutdown_tx) = start_simple_unsharded("it_works_with_double_query_through").await;
    let sql = "
        # base tables
        CREATE TABLE A (aid int, other int, PRIMARY KEY(aid));
        CREATE TABLE B (bid int, PRIMARY KEY(bid));

        # read queries
        CREATE CACHE ReadJoin FROM SELECT J.aid, J.other \
            FROM B \
            LEFT JOIN (SELECT A.aid, A.other FROM A \
                WHERE A.other = 5) AS J \
            ON (J.aid = B.bid) \
            WHERE J.aid = ?;
    ";

    g.extend_recipe(ChangeList::from_str(sql, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();
    let mut a = g.table("A").await.unwrap();
    let mut b = g.table("B").await.unwrap();
    let mut getter = g
        .view("ReadJoin")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    a.insert(vec![1i64.into(), 5.into()]).await.unwrap();
    a.insert(vec![2i64.into(), 10.into()]).await.unwrap();
    b.insert(vec![1i64.into()]).await.unwrap();

    sleep().await;

    let rs = getter
        .lookup(&[1i64.into()], true)
        .await
        .unwrap()
        .into_vec();
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0], vec![1i64.into(), 5.into()]);

    let empty = getter
        .lookup(&[2i64.into()], true)
        .await
        .unwrap()
        .into_vec();
    assert_eq!(empty.len(), 0);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn it_works_with_duplicate_subquery() {
    let (mut g, shutdown_tx) = start_simple_unsharded("it_works_with_double_query_through").await;
    let sql = "
        # base tables
        CREATE TABLE A (aid int, other int, PRIMARY KEY(aid));
        CREATE TABLE B (bid int, PRIMARY KEY(bid));

        # read queries
        CREATE CACHE ReadJoin FROM SELECT J.aid, J.other \
            FROM B \
            LEFT JOIN (SELECT A.aid, A.other FROM A \
                WHERE A.other = 5) AS J \
            ON (J.aid = B.bid) \
            WHERE J.aid = ?;

        # Another query, with a subquery identical to the one above but named differently.
        CREATE CACHE ReadJoin2 FROM SELECT J2.aid, J2.other \
            FROM B \
            LEFT JOIN (SELECT A.aid, A.other FROM A \
                WHERE A.other = 5) AS J2 \
            ON (J2.aid = B.bid) \
            WHERE J2.aid = ?;
    ";

    g.extend_recipe(ChangeList::from_str(sql, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();
    let mut a = g.table("A").await.unwrap();
    let mut b = g.table("B").await.unwrap();
    let mut getter = g
        .view("ReadJoin2")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    a.insert(vec![1i64.into(), 5.into()]).await.unwrap();
    a.insert(vec![2i64.into(), 10.into()]).await.unwrap();
    b.insert(vec![1i64.into()]).await.unwrap();

    sleep().await;

    let rs = getter
        .lookup(&[1i64.into()], true)
        .await
        .unwrap()
        .into_vec();
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0], vec![1i64.into(), 5.into()]);

    let empty = getter
        .lookup(&[2i64.into()], true)
        .await
        .unwrap()
        .into_vec();
    assert_eq!(empty.len(), 0);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn it_works_with_reads_before_writes() {
    let (mut g, shutdown_tx) = start_simple_unsharded("it_works_with_reads_before_writes").await;
    let sql = "
        CREATE TABLE Article (aid int, PRIMARY KEY(aid));
        CREATE TABLE Vote (aid int, uid int, PRIMARY KEY(aid, uid));
        CREATE CACHE ArticleVote FROM SELECT Article.aid, Vote.uid \
            FROM Article, Vote \
            WHERE Article.aid = Vote.aid AND Article.aid = ?;
    ";

    g.extend_recipe(ChangeList::from_str(sql, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();
    let mut article = g.table("Article").await.unwrap();
    let mut vote = g.table("Vote").await.unwrap();
    let mut awvc = g
        .view("ArticleVote")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    let aid = 1;
    let uid = 10;

    assert!(awvc
        .lookup(&[aid.into()], true)
        .await
        .unwrap()
        .into_vec()
        .is_empty());
    article.insert(vec![aid.into()]).await.unwrap();
    sleep().await;

    vote.insert(vec![aid.into(), uid.into()]).await.unwrap();
    sleep().await;

    let result = awvc.lookup(&[aid.into()], true).await.unwrap().into_vec();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], vec![aid.into(), uid.into()]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn forced_shuffle_despite_same_shard() {
    // XXX: this test doesn't currently *fail* despite
    // multiple trailing replay responses that are simply ignored...

    let (mut g, shutdown_tx) = start_simple_unsharded("forced_shuffle_despite_same_shard").await;
    let sql = "
        CREATE TABLE Car (cid int, pid int, PRIMARY KEY(pid));
        CREATE TABLE Price (pid int, price int, PRIMARY KEY(pid));
        CREATE CACHE CarPrice FROM SELECT cid, price FROM Car \
            JOIN Price ON Car.pid = Price.pid WHERE cid = ?;
    ";
    g.extend_recipe(ChangeList::from_str(sql, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    let mut car_mutator = g.table("Car").await.unwrap();
    let mut price_mutator = g.table("Price").await.unwrap();
    let mut getter = g
        .view("CarPrice")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    let cid = 1;
    let pid = 1;
    let price = 100;

    price_mutator
        .insert(vec![pid.into(), price.into()])
        .await
        .unwrap();
    car_mutator
        .insert(vec![cid.into(), pid.into()])
        .await
        .unwrap();

    // Let writes propagate:
    sleep().await;

    // Retrieve the result of the count query:
    let result = getter.lookup(&[cid.into()], true).await.unwrap().into_vec();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][1], price.into());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn double_shuffle() {
    let (mut g, shutdown_tx) = start_simple_unsharded("double_shuffle").await;
    let sql = "
        CREATE TABLE Car (cid int, pid int, PRIMARY KEY(cid));
        CREATE TABLE Price (pid int, price int, PRIMARY KEY(pid));
        CREATE CACHE CarPrice FROM SELECT cid, price FROM Car \
            JOIN Price ON Car.pid = Price.pid WHERE cid = ?;
    ";
    g.extend_recipe(ChangeList::from_str(sql, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    let mut car_mutator = g.table("Car").await.unwrap();
    let mut price_mutator = g.table("Price").await.unwrap();
    let mut getter = g
        .view("CarPrice")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    let cid = 1;
    let pid = 1;
    let price = 100;

    price_mutator
        .insert(vec![pid.into(), price.into()])
        .await
        .unwrap();
    car_mutator
        .insert(vec![cid.into(), pid.into()])
        .await
        .unwrap();

    // Let writes propagate:
    sleep().await;

    // Retrieve the result of the count query:
    let result = getter.lookup(&[cid.into()], true).await.unwrap().into_vec();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][1], price.into());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn it_works_with_arithmetic_aliases() {
    let (mut g, shutdown_tx) = start_simple_unsharded("it_works_with_arithmetic_aliases").await;
    let sql = "
        CREATE TABLE Price (pid int, cent_price int, PRIMARY KEY(pid));
        CREATE VIEW ModPrice AS SELECT pid, cent_price / 100 AS price FROM Price;
        CREATE CACHE AltPrice FROM SELECT pid, price FROM ModPrice WHERE pid = ?;
    ";
    g.extend_recipe(ChangeList::from_str(sql, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    let mut price_mutator = g.table("Price").await.unwrap();
    let mut getter = g
        .view("AltPrice")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    let pid = 1;
    let price = 10000;
    price_mutator
        .insert(vec![pid.into(), price.into()])
        .await
        .unwrap();

    // Let writes propagate:
    sleep().await;

    // Retrieve the result of the count query:
    let result = getter.lookup(&[pid.into()], true).await.unwrap().into_vec();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][1], (price / 100).into());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn it_recovers_persisted_bases() {
    let authority_store = Arc::new(LocalAuthorityStore::new());
    let authority = Arc::new(Authority::from(LocalAuthority::new_with_store(
        authority_store.clone(),
    )));
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("it_recovers_persisted_bases");
    let persistence_params = PersistenceParameters::new(
        DurabilityMode::Permanent,
        Some(path.to_string_lossy().into()),
        1,
        None,
    );

    {
        let mut g = Builder::for_tests();
        g.set_persistence(persistence_params.clone());
        let (mut g, shutdown_tx) = g.start(authority.clone()).await.unwrap();
        g.backend_ready().await;

        {
            let sql = "
            CREATE TABLE Car (id int, price int, PRIMARY KEY(id));
            CREATE CACHE CarPrice FROM SELECT price FROM Car WHERE id = ?;
        ";
            g.extend_recipe(ChangeList::from_str(sql, Dialect::DEFAULT_MYSQL).unwrap())
                .await
                .unwrap();

            let mut mutator = g.table("Car").await.unwrap();

            for i in 1..10 {
                let price = i * 10;
                mutator.insert(vec![i.into(), price.into()]).await.unwrap();
            }
        }

        // Let writes propagate:
        sleep().await;
        shutdown_tx.shutdown().await;
        if let Authority::LocalAuthority(l) = authority.as_ref() {
            l.delete_ephemeral();
        }
    }

    sleep().await;

    let authority = Arc::new(Authority::from(LocalAuthority::new_with_store(
        authority_store.clone(),
    )));

    let mut g = Builder::for_tests();
    g.set_persistence(persistence_params);
    let (mut g, shutdown_tx) = g.start(authority.clone()).await.unwrap();
    g.backend_ready().await;
    {
        let mut getter = g
            .view("CarPrice")
            .await
            .unwrap()
            .into_reader_handle()
            .unwrap();

        // Make sure that the new graph contains the old writes
        for i in 1..10 {
            let price = i * 10;
            let result = getter.lookup(&[i.into()], true).await.unwrap().into_vec();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0][0], price.into());
        }
    }

    shutdown_tx.shutdown().await;
}

// TODO(ENG-860): Flaky test.
#[tokio::test(flavor = "multi_thread")]
async fn it_recovers_persisted_bases_with_volume_id() {
    let authority_store = Arc::new(LocalAuthorityStore::new());
    let authority = Arc::new(Authority::from(LocalAuthority::new_with_store(
        authority_store.clone(),
    )));

    let dir = tempfile::tempdir().unwrap();
    let path = dir
        .path()
        .join("it_recovers_persisted_bases_with_volume_id");
    let persistence_params = PersistenceParameters::new(
        DurabilityMode::Permanent,
        Some(path.to_string_lossy().into()),
        1,
        None,
    );

    {
        let mut g = Builder::for_tests();
        g.set_persistence(persistence_params.clone());
        g.set_volume_id("ef731j2".into());
        let (mut g, shutdown_tx) = g.start(authority.clone()).await.unwrap();
        g.backend_ready().await;
        {
            let sql = "
            CREATE TABLE Car (id int, price int, PRIMARY KEY(id));
            CREATE CACHE CarPrice FROM SELECT price FROM Car WHERE id = ?;
        ";
            g.extend_recipe(ChangeList::from_str(sql, Dialect::DEFAULT_MYSQL).unwrap())
                .await
                .unwrap();

            let mut mutator = g.table("Car").await.unwrap();

            for i in 1..10 {
                let price = i * 10;
                mutator.insert(vec![i.into(), price.into()]).await.unwrap();
            }
        }

        // Let writes propagate:
        sleep().await;
        shutdown_tx.shutdown().await;
        if let Authority::LocalAuthority(l) = authority.as_ref() {
            l.delete_ephemeral();
        }
    }

    sleep().await;

    let authority = Arc::new(Authority::from(LocalAuthority::new_with_store(
        authority_store.clone(),
    )));
    let mut g = Builder::for_tests();
    g.set_persistence(persistence_params);
    g.set_volume_id("ef731j2".into());
    let (mut g, shutdown_tx) = g.start(authority.clone()).await.unwrap();
    g.backend_ready().await;
    {
        let mut getter = g
            .view("CarPrice")
            .await
            .unwrap()
            .into_reader_handle()
            .unwrap();

        // Make sure that the new graph contains the old writes
        for i in 1..10 {
            let price = i * 10;
            let result = getter.lookup(&[i.into()], true).await.unwrap().into_vec();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0][0], price.into());
        }
    }
    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn it_doesnt_recover_persisted_bases_with_wrong_volume_id() {
    let authority = Arc::new(Authority::from(LocalAuthority::new()));
    let dir = tempfile::tempdir().unwrap();
    let path = dir
        .path()
        .join("it_doesnt_recover_persisted_bases_with_wrong_volume_id");
    let persistence_params = PersistenceParameters::new(
        DurabilityMode::Permanent,
        Some(path.to_string_lossy().into()),
        1,
        None,
    );

    {
        let mut g = Builder::for_tests();
        g.set_persistence(persistence_params.clone());
        g.set_volume_id("ef731j2".into());
        let (mut g, shutdown_tx) = g.start(authority.clone()).await.unwrap();
        sleep().await;

        {
            let sql = "
            CREATE TABLE Car (id int, price int, PRIMARY KEY(id));
            CREATE CACHE CarPrice FROM SELECT price FROM Car WHERE id = ?;
        ";
            g.extend_recipe(ChangeList::from_str(sql, Dialect::DEFAULT_MYSQL).unwrap())
                .await
                .unwrap();

            let mut mutator = g.table("Car").await.unwrap();

            for i in 1..10 {
                let price = i * 10;
                mutator.insert(vec![i.into(), price.into()]).await.unwrap();
            }
        }

        // Let writes propagate:
        sleep().await;
        shutdown_tx.shutdown().await;
        if let Authority::LocalAuthority(l) = authority.as_ref() {
            l.delete_ephemeral();
        }
    }

    sleep().await;

    let mut g = Builder::for_tests();
    let authority = Arc::new(Authority::from(LocalAuthority::new()));
    g.set_persistence(persistence_params);
    g.set_volume_id("j3131t8".into());
    let (mut g, shutdown_tx) = g.start(authority.clone()).await.unwrap();
    let getter = g.view("CarPrice").await;
    // This throws an error because there is no worker to place the domain on.
    getter.unwrap_err();
    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn mutator_churn() {
    let (mut g, shutdown_tx) = start_simple_unsharded("mutator_churn").await;
    let vote = g
        .migrate(|mig| {
            // migrate

            // add vote base table
            let vote = mig.add_base("vote", make_columns(&["user", "id"]), Base::default());

            // add vote count
            let vc = mig.add_ingredient(
                "votecount",
                make_columns(&["id", "votes"]),
                Aggregation::Count
                    .over(vote, 0, &[1], &DfType::Unknown)
                    .unwrap(),
            );

            mig.maintain_anonymous_with_reader_processing(
                vc,
                &Index::hash_map(vec![0]),
                ReaderProcessing::new(None, None, Some(vec![0, 1]), None, None).unwrap(),
            );
            vote
        })
        .await;

    let mut vc_state = g
        .view("votecount")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    let ids = 10;
    let votes = 7;

    // continuously write to vote with new mutators
    let user: DfValue = 0.into();
    for _ in 0..votes {
        for i in 0..ids {
            g.table_by_index(vote)
                .await
                .unwrap()
                .insert(vec![user.clone(), i.into()])
                .await
                .unwrap();
        }
    }

    // allow the system to catch up with the last writes
    sleep().await;

    // check that all writes happened the right number of times
    for i in 0..ids {
        assert_eq!(
            vc_state.lookup(&[i.into()], true).await.unwrap().into_vec(),
            vec![vec![i.into(), votes.into()]]
        );
    }

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn view_connection_churn() {
    let authority_store = Arc::new(LocalAuthorityStore::new());
    let authority = Arc::new(Authority::from(LocalAuthority::new_with_store(
        authority_store,
    )));

    let mut builder = Builder::for_tests();
    builder.set_sharding(Some(DEFAULT_SHARDING));
    builder.set_persistence(get_persistence_params("connection_churn"));
    let (mut g, shutdown_tx) = builder.start(authority.clone()).await.unwrap();

    g.extend_recipe(
        ChangeList::from_str(
            "
        CREATE TABLE A (id int, PRIMARY KEY(id));
        CREATE CACHE AID FROM SELECT id FROM A WHERE id = ?;
    ",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);

    // continuously write to vote with entirely new connections
    let jhs: Vec<_> = (0..20)
        .map(|i| {
            let authority = authority.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                let mut builder = Builder::for_tests();
                builder.set_sharding(Some(DEFAULT_SHARDING));
                builder.set_persistence(get_persistence_params("connection_churn"));
                let (mut g, shutdown_tx) = builder.start(authority.clone()).await.unwrap();

                g.view("AID")
                    .await
                    .unwrap()
                    .into_reader_handle()
                    .unwrap()
                    .lookup(&[DfValue::from(i)], true)
                    .await
                    .unwrap();

                drop(tx);
                shutdown_tx.shutdown().await;
            })
        })
        .collect();
    drop(tx);
    assert_eq!(rx.recv().await, None);
    shutdown_tx.shutdown().await;
    for jh in jhs {
        jh.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "Flaky test (ENG-1931)"]
async fn table_connection_churn() {
    let authority_store = Arc::new(LocalAuthorityStore::new());
    let authority = Arc::new(Authority::from(LocalAuthority::new_with_store(
        authority_store,
    )));

    let mut builder = Builder::for_tests();
    builder.set_sharding(Some(DEFAULT_SHARDING));
    builder.set_persistence(get_persistence_params("connection_churn"));
    let (mut g, shutdown_tx) = builder.start(authority.clone()).await.unwrap();

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE A (id int, PRIMARY KEY(id));",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);

    // continuously write to vote with entirely new connections
    let jhs: Vec<_> = (0..20)
        .map(|i| {
            let authority = authority.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                let mut builder = Builder::for_tests();
                builder.set_sharding(Some(DEFAULT_SHARDING));
                builder.set_persistence(get_persistence_params("connection_churn"));
                let (mut g, shutdown_tx) = builder.start(authority.clone()).await.unwrap();

                g.table("A")
                    .await
                    .unwrap()
                    .insert(vec![DfValue::from(i)])
                    .await
                    .unwrap();

                drop(tx);
                shutdown_tx.shutdown().await;
            })
        })
        .collect();
    drop(tx);
    assert_eq!(rx.recv().await, None);
    shutdown_tx.shutdown().await;
    for jh in jhs {
        jh.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn it_recovers_persisted_bases_w_multiple_nodes() {
    let authority_store = Arc::new(LocalAuthorityStore::new());
    let authority = Arc::new(Authority::from(LocalAuthority::new_with_store(
        authority_store.clone(),
    )));

    let dir = tempfile::tempdir().unwrap();
    let path = dir
        .path()
        .join("it_recovers_persisted_bases_w_multiple_nodes");
    let tables = vec!["A", "B", "C"];
    let persistence_parameters = PersistenceParameters::new(
        DurabilityMode::Permanent,
        Some(path.to_string_lossy().into()),
        1,
        None,
    );

    {
        let mut g = Builder::for_tests();
        g.set_persistence(persistence_parameters.clone());
        let (mut g, shutdown_tx) = g.start(authority.clone()).await.unwrap();
        g.backend_ready().await;

        {
            let sql = "
            CREATE TABLE A (id int, PRIMARY KEY(id));
            CREATE TABLE B (id int, PRIMARY KEY(id));
            CREATE TABLE C (id int, PRIMARY KEY(id));

            CREATE CACHE AID FROM SELECT id FROM A WHERE id = ?;
            CREATE CACHE BID FROM SELECT id FROM B WHERE id = ?;
            CREATE CACHE CID FROM SELECT id FROM C WHERE id = ?;
        ";
            g.extend_recipe(ChangeList::from_str(sql, Dialect::DEFAULT_MYSQL).unwrap())
                .await
                .unwrap();
            for (i, table) in tables.iter().enumerate() {
                let mut mutator = g.table(*table).await.unwrap();
                mutator.insert(vec![i.into()]).await.unwrap();
            }
        }
        sleep().await;
        shutdown_tx.shutdown().await;
        if let Authority::LocalAuthority(l) = authority.as_ref() {
            l.delete_ephemeral();
        }
    }

    sleep().await;

    // Create a new controller with the same authority store, and make sure that it recovers to the
    // same state that the other one had.
    let authority = Arc::new(Authority::from(LocalAuthority::new_with_store(
        authority_store,
    )));
    let mut g = Builder::for_tests();
    g.set_persistence(persistence_parameters);
    let (mut g, shutdown_tx) = g.start(authority.clone()).await.unwrap();
    g.backend_ready().await;
    for (i, table) in tables.iter().enumerate() {
        let mut getter = g
            .view(&format!("{}ID", table))
            .await
            .unwrap()
            .into_reader_handle()
            .unwrap();
        let result = getter.lookup(&[i.into()], true).await.unwrap().into_vec();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], i.into());
    }
    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn it_recovers_persisted_bases_w_multiple_nodes_and_volume_id() {
    let authority_store = Arc::new(LocalAuthorityStore::new());
    let authority = Arc::new(Authority::from(LocalAuthority::new_with_store(
        authority_store.clone(),
    )));
    let dir = tempfile::tempdir().unwrap();
    let path = dir
        .path()
        .join("it_recovers_persisted_bases_w_multiple_nodes_and_volume_id");
    let tables = vec!["A", "B", "C"];
    let persistence_parameters = PersistenceParameters::new(
        DurabilityMode::Permanent,
        Some(path.to_string_lossy().into()),
        1,
        None,
    );

    {
        let mut g = Builder::for_tests();
        g.set_persistence(persistence_parameters.clone());
        g.set_volume_id("ef731j2".into());
        let (mut g, shutdown_tx) = g.start(authority.clone()).await.unwrap();
        g.backend_ready().await;

        {
            let sql = "
            CREATE TABLE A (id int, PRIMARY KEY(id));
            CREATE TABLE B (id int, PRIMARY KEY(id));
            CREATE TABLE C (id int, PRIMARY KEY(id));

            CREATE CACHE AID FROM SELECT id FROM A WHERE id = ?;
            CREATE CACHE BID FROM SELECT id FROM B WHERE id = ?;
            CREATE CACHE CID FROM SELECT id FROM C WHERE id = ?;
        ";
            g.extend_recipe(ChangeList::from_str(sql, Dialect::DEFAULT_MYSQL).unwrap())
                .await
                .unwrap();
            for (i, table) in tables.iter().enumerate() {
                let mut mutator = g.table(*table).await.unwrap();
                mutator.insert(vec![i.into()]).await.unwrap();
            }
        }
        sleep().await;
        shutdown_tx.shutdown().await;
        if let Authority::LocalAuthority(l) = authority.as_ref() {
            l.delete_ephemeral();
        }
    }
    sleep().await;

    // Create a new controller with the same authority store, and make sure that it recovers to the
    // same state that the other one had.
    let authority = Arc::new(Authority::from(LocalAuthority::new_with_store(
        authority_store,
    )));
    let mut g = Builder::for_tests();
    g.set_persistence(persistence_parameters);
    g.set_volume_id("ef731j2".into());
    let (mut g, shutdown_tx) = g.start(authority.clone()).await.unwrap();
    g.backend_ready().await;
    std::thread::sleep(Duration::from_secs(10));
    for (i, table) in tables.iter().enumerate() {
        let mut getter = g
            .view(&format!("{}ID", table))
            .await
            .unwrap()
            .into_reader_handle()
            .unwrap();
        let result = getter.lookup(&[i.into()], true).await.unwrap().into_vec();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], i.into());
    }
    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn it_works_with_simple_arithmetic() {
    let (mut g, shutdown_tx) = start_simple_unsharded("it_works_with_simple_arithmetic").await;

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE Car (id int, price int, PRIMARY KEY(id));
         CREATE CACHE CarPrice FROM SELECT 2 * price FROM Car WHERE id = ?;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut mutator = g.table("Car").await.unwrap();
    let mut getter = g
        .view("CarPrice")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    let id: DfValue = 1.into();
    let price: DfValue = 123.into();
    mutator.insert(vec![id.clone(), price]).await.unwrap();

    // Let writes propagate:
    sleep().await;

    // Retrieve the result of the count query:
    let result = getter.lookup(&[id.clone()], true).await.unwrap().into_vec();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][0], 246.into());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn it_works_with_multiple_arithmetic_expressions() {
    let (mut g, shutdown_tx) =
        start_simple_unsharded("it_works_with_multiple_arithmetic_expressions").await;
    let sql = "CREATE TABLE Car (id int, price int, PRIMARY KEY(id));
               CREATE CACHE CarPrice FROM SELECT 10 * 10, 2 * price, 10 * price, FROM Car WHERE id = ?;
               ";
    g.extend_recipe(ChangeList::from_str(sql, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    let mut mutator = g.table("Car").await.unwrap();
    let mut getter = g
        .view("CarPrice")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    let id: DfValue = 1.into();
    let price: DfValue = 123.into();
    mutator.insert(vec![id.clone(), price]).await.unwrap();

    // Let writes propagate:
    sleep().await;

    // Retrieve the result of the count query:
    let result = getter.lookup(&[id.clone()], true).await.unwrap().into_vec();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][0], 100.into());
    assert_eq!(result[0][1], 246.into());
    assert_eq!(result[0][2], 1230.into());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn it_works_with_join_arithmetic() {
    let (mut g, shutdown_tx) = start_simple_unsharded("it_works_with_join_arithmetic").await;
    let sql = "
        CREATE TABLE Car (car_id int, price_id int, PRIMARY KEY(car_id));
        CREATE TABLE Price (price_id int, price int, PRIMARY KEY(price_id));
        CREATE TABLE Sales (sales_id int, price_id int, fraction float, PRIMARY KEY(sales_id));
        CREATE CACHE CarPrice FROM SELECT price * fraction FROM Car \
                  JOIN Price ON Car.price_id = Price.price_id \
                  JOIN Sales ON Price.price_id = Sales.price_id \
                  WHERE car_id = ?;
    ";
    g.extend_recipe(ChangeList::from_str(sql, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    let mut car_mutator = g.table("Car").await.unwrap();
    let mut price_mutator = g.table("Price").await.unwrap();
    let mut sales_mutator = g.table("Sales").await.unwrap();
    let mut getter = g
        .view("CarPrice")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    let id: i32 = 1;
    let price: i32 = 123;
    let fraction = 0.7;
    car_mutator
        .insert(vec![id.into(), id.into()])
        .await
        .unwrap();
    price_mutator
        .insert(vec![id.into(), price.into()])
        .await
        .unwrap();
    sales_mutator
        .insert(vec![
            id.into(),
            id.into(),
            DfValue::try_from(fraction).unwrap(),
        ])
        .await
        .unwrap();

    // Let writes propagate:
    sleep().await;

    // Retrieve the result of the count query:
    let result = getter.lookup(&[id.into()], true).await.unwrap().into_vec();
    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0][0],
        DfValue::try_from(f64::from(price) * fraction).unwrap()
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn it_works_with_function_arithmetic() {
    readyset_tracing::init_test_logging();
    let (mut g, shutdown_tx) = start_simple_unsharded("it_works_with_function_arithmetic").await;
    let sql = "
        CREATE TABLE Bread (id int, price int, PRIMARY KEY(id));
        CREATE CACHE Price FROM SELECT 2 * MAX(price) FROM Bread;
    ";
    g.extend_recipe(ChangeList::from_str(sql, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    let mut mutator = g.table("Bread").await.unwrap();
    let mut getter = g.view("Price").await.unwrap().into_reader_handle().unwrap();
    let max_price = 20;
    for (i, price) in (10..=max_price).enumerate() {
        let id = i + 1;
        mutator.insert(vec![id.into(), price.into()]).await.unwrap();
    }

    // Let writes propagate:
    sleep().await;

    let result = getter.lookup(&[0.into()], true).await.unwrap().into_vec();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][0], DfValue::from(max_price * 2));

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn votes() {
    // set up graph
    readyset_tracing::init_test_logging();
    let (mut g, shutdown_tx) = start_simple_unsharded("votes").await;
    let (article1, article2, vote) = g
        .migrate(|mig| {
            // add article base nodes (we use two so we can exercise unions too)
            let article1 =
                mig.add_base("article1", make_columns(&["id", "title"]), Base::default());
            let article2 =
                mig.add_base("article2", make_columns(&["id", "title"]), Base::default());

            // add a (stupid) union of article1 + article2
            let mut emits = HashMap::new();
            emits.insert(article1, vec![0, 1]);
            emits.insert(article2, vec![0, 1]);
            let u = Union::new(emits, union::DuplicateMode::UnionAll).unwrap();
            let article = mig.add_ingredient("article", make_columns(&["id", "title"]), u);
            mig.maintain_anonymous(article, &Index::hash_map(vec![0]));

            // add vote base table
            let vote = mig.add_base("vote", make_columns(&["user", "id"]), Base::default());

            // add vote count
            let vc = mig.add_ingredient(
                "vc",
                make_columns(&["id", "votes"]),
                Aggregation::Count
                    .over(vote, 0, &[1], &DfType::Unknown)
                    .unwrap(),
            );
            mig.maintain_anonymous(vc, &Index::hash_map(vec![0]));

            // add final join using first field from article and first from vc
            let j = Join::new(
                article,
                vc,
                JoinType::Inner,
                vec![(0, 0)],
                vec![(Side::Left, 0), (Side::Left, 1), (Side::Right, 1)],
            );
            let end = mig.add_ingredient("end", make_columns(&["id", "title", "votes"]), j);
            mig.maintain_anonymous(end, &Index::hash_map(vec![0]));

            (article1, article2, vote)
        })
        .await;

    let mut articleq = g
        .view("article")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    let mut vcq = g.view("vc").await.unwrap().into_reader_handle().unwrap();
    let mut endq = g.view("end").await.unwrap().into_reader_handle().unwrap();

    let mut mut1 = g.table_by_index(article1).await.unwrap();
    let mut mut2 = g.table_by_index(article2).await.unwrap();
    let mut mutv = g.table_by_index(vote).await.unwrap();

    let a1: DfValue = 1.into();
    let a2: DfValue = 2.into();

    // make one article
    mut1.insert(vec![a1.clone(), 2.into()]).await.unwrap();

    // give it some time to propagate
    sleep().await;

    // query articles to see that it was updated
    assert_eq!(
        articleq
            .lookup(&[a1.clone()], true)
            .await
            .unwrap()
            .into_vec(),
        vec![vec![a1.clone(), 2.into()]]
    );

    // make another article
    mut2.insert(vec![a2.clone(), 4.into()]).await.unwrap();

    // give it some time to propagate
    sleep().await;

    // query articles again to see that the new article was absorbed
    // and that the old one is still present
    assert_eq!(
        articleq
            .lookup(&[a1.clone()], true)
            .await
            .unwrap()
            .into_vec(),
        vec![vec![a1.clone(), 2.into()]]
    );
    assert_eq!(
        articleq
            .lookup(&[a2.clone()], true)
            .await
            .unwrap()
            .into_vec(),
        vec![vec![a2.clone(), 4.into()]]
    );

    // create a vote (user 1 votes for article 1)
    mutv.insert(vec![1.into(), a1.clone()]).await.unwrap();

    // give it some time to propagate
    sleep().await;

    // query vote count to see that the count was updated
    let res = vcq.lookup(&[a1.clone()], true).await.unwrap().into_vec();
    assert!(res.iter().all(|r| r[0] == a1.clone() && r[1] == 1.into()));
    assert_eq!(res.len(), 1);

    // check that article 1 appears in the join view with a vote count of one
    let res = endq.lookup(&[a1.clone()], true).await.unwrap().into_vec();
    assert!(
        res.iter()
            .any(|r| r[0] == a1.clone() && r[1] == 2.into() && r[2] == 1.into()),
        "no entry for [1,2,1|2] in {:?}",
        res
    );
    assert_eq!(res.len(), 1);

    // check that article 2 doesn't have any votes
    let res = endq.lookup(&[a2.clone()], true).await.unwrap().into_vec();
    assert!(res.len() <= 1); // could be 1 if we had zero-rows

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_migration() {
    // set up graph
    let (mut g, shutdown_tx) = start_simple_unsharded("empty_migration").await;
    g.migrate(|_| {}).await;

    let (a, b) = g
        .migrate(|mig| {
            let a = mig.add_base("a", make_columns(&["a", "b"]), Base::default());
            let b = mig.add_base("b", make_columns(&["a", "b"]), Base::default());

            let mut emits = HashMap::new();
            emits.insert(a, vec![0, 1]);
            emits.insert(b, vec![0, 1]);
            let u = Union::new(emits, union::DuplicateMode::UnionAll).unwrap();
            let c = mig.add_ingredient("c", make_columns(&["a", "b"]), u);
            mig.maintain_anonymous(c, &Index::hash_map(vec![0]));
            (a, b)
        })
        .await;

    let mut cq = g.view("c").await.unwrap().into_reader_handle().unwrap();
    let mut muta = g.table_by_index(a).await.unwrap();
    let mut mutb = g.table_by_index(b).await.unwrap();
    let id: DfValue = 1.into();

    // send a value on a
    muta.insert(vec![id.clone(), 2.into()]).await.unwrap();

    // give it some time to propagate
    sleep().await;

    // send a query to c
    assert_eq!(
        cq.lookup(&[id.clone()], true).await.unwrap().into_vec(),
        vec![vec![1.into(), 2.into()]]
    );

    // update value again
    mutb.insert(vec![id.clone(), 4.into()]).await.unwrap();

    // give it some time to propagate
    sleep().await;

    // check that value was updated again
    let res = cq.lookup(&[id.clone()], true).await.unwrap().into_vec();
    assert!(res.iter().any(|r| *r == vec![id.clone(), 2.into()]));
    assert!(res.iter().any(|r| *r == vec![id.clone(), 4.into()]));

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn simple_migration() {
    let id: DfValue = 1.into();

    // set up graph
    let (mut g, shutdown_tx) = start_simple_unsharded("simple_migration").await;
    let a = g
        .migrate(|mig| {
            let a = mig.add_base("a", make_columns(&["a", "b"]), Base::default());
            mig.maintain_anonymous(a, &Index::hash_map(vec![0]));
            a
        })
        .await;

    let mut aq = g.view("a").await.unwrap().into_reader_handle().unwrap();
    let mut muta = g.table_by_index(a).await.unwrap();

    // send a value on a
    muta.insert(vec![id.clone(), 2.into()]).await.unwrap();

    // give it some time to propagate
    sleep().await;

    // check that a got it
    assert_eq!(
        aq.lookup(&[id.clone()], true).await.unwrap().into_vec(),
        vec![vec![1.into(), 2.into()]]
    );

    // add unrelated node b in a migration
    let b = g
        .migrate(|mig| {
            let b = mig.add_base("b", make_columns(&["a", "b"]), Base::default());
            mig.maintain_anonymous(b, &Index::hash_map(vec![0]));
            b
        })
        .await;

    let mut bq = g.view("b").await.unwrap().into_reader_handle().unwrap();
    let mut mutb = g.table_by_index(b).await.unwrap();

    // send a value on b
    mutb.insert(vec![id.clone(), 4.into()]).await.unwrap();

    // give it some time to propagate
    sleep().await;

    // check that b got it
    assert_eq!(
        bq.lookup(&[id.clone()], true).await.unwrap().into_vec(),
        vec![vec![1.into(), 4.into()]]
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn add_columns() {
    let id: DfValue = "x".try_into().unwrap();

    // set up graph
    let (mut g, shutdown_tx) = start_simple_unsharded("add_columns").await;
    let a = g
        .migrate(|mig| {
            let a = mig.add_base(
                "a",
                make_columns(&["a", "b"]),
                Base::new().with_default_values(vec![1.into(), 2.into()]),
            );
            mig.maintain_anonymous(a, &Index::hash_map(vec![0]));
            a
        })
        .await;
    let mut aq = g.view("a").await.unwrap().into_reader_handle().unwrap();
    let mut muta = g.table_by_index(a).await.unwrap();

    // send a value on a
    muta.insert(vec![id.clone(), "y".try_into().unwrap()])
        .await
        .unwrap();
    sleep().await;

    // check that a got it
    assert_eq!(
        aq.lookup(&[id.clone()], true).await.unwrap().into_vec(),
        vec![vec![id.clone(), "y".try_into().unwrap()]]
    );

    // add a third column to a
    g.migrate(move |mig| {
        mig.add_column(a, dataflow_column("c"), 3.into()).unwrap();
    })
    .await;
    sleep().await;

    // send another (old) value on a
    muta.insert(vec![id.clone(), "z".try_into().unwrap()])
        .await
        .unwrap();
    sleep().await;

    // check that a got it, and added the new, third column's default
    let res = aq.lookup(&[id.clone()], true).await.unwrap().into_vec();
    assert_eq!(res.len(), 2);
    assert!(res.contains(&vec![id.clone(), "y".try_into().unwrap()]));
    assert!(res.contains(&vec![id.clone(), "z".try_into().unwrap(), 3.into()]));

    // get a new muta and send a new value on it
    let mut muta = g.table_by_index(a).await.unwrap();
    muta.insert(vec![id.clone(), "a".try_into().unwrap(), 10.into()])
        .await
        .unwrap();
    sleep().await;

    // check that a got it, and included the third column
    let res = aq.lookup(&[id.clone()], true).await.unwrap().into_vec();
    assert_eq!(res.len(), 3);
    assert!(res.contains(&vec![id.clone(), "y".try_into().unwrap()]));
    assert!(res.contains(&vec![id.clone(), "z".try_into().unwrap(), 3.into()]));
    assert!(res.contains(&vec![id.clone(), "a".try_into().unwrap(), 10.into()]));

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn migrate_added_columns() {
    let id: DfValue = "x".try_into().unwrap();

    // set up graph
    let (mut g, shutdown_tx) = start_simple_unsharded("migrate_added_columns").await;
    let a = g
        .migrate(|mig| {
            mig.add_base(
                "a",
                make_columns(&["a", "b"]),
                Base::new().with_default_values(vec![1.into(), 2.into()]),
            )
        })
        .await;
    let mut muta = g.table_by_index(a).await.unwrap();

    // send a value on a
    muta.insert(vec![id.clone(), "y".try_into().unwrap()])
        .await
        .unwrap();
    sleep().await;

    // add a third column to a, and a view that uses it
    let _ = g
        .migrate(move |mig| {
            mig.add_column(a, dataflow_column("c"), 3.into()).unwrap();
            let b = mig.add_ingredient(
                "x",
                make_columns(&["c", "b"]),
                Project::new(
                    a,
                    vec![
                        DfExpr::Column {
                            index: 2,
                            ty: DfType::Unknown,
                        },
                        DfExpr::Column {
                            index: 0,
                            ty: DfType::Unknown,
                        },
                    ],
                ),
            );
            mig.maintain_anonymous(b, &Index::hash_map(vec![1]));
            b
        })
        .await;

    let mut bq = g.view("x").await.unwrap().into_reader_handle().unwrap();

    // send another (old) value on a
    muta.insert(vec![id.clone(), "z".try_into().unwrap()])
        .await
        .unwrap();
    // and an entirely new value
    let mut muta = g.table_by_index(a).await.unwrap();
    muta.insert(vec![id.clone(), "a".try_into().unwrap(), 10.into()])
        .await
        .unwrap();

    // give it some time to propagate
    sleep().await;

    // we should now see the pre-migration write and the old post-migration write with the default
    // value, and the new post-migration write with the value it contained.
    let res = bq.lookup(&[id.clone()], true).await.unwrap().into_vec();
    assert_eq!(res.len(), 3);
    assert_eq!(
        res.iter()
            .filter(|r| *r == &vec![3.into(), id.clone()])
            .count(),
        2
    );
    assert!(res.iter().any(|r| *r == vec![10.into(), id.clone()]));

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn migrate_drop_columns() {
    let id: DfValue = "x".try_into().unwrap();

    // set up graph
    let (mut g, shutdown_tx) = start_simple_unsharded("migrate_drop_columns").await;
    let a = g
        .migrate(|mig| {
            let a = mig.add_base(
                "a",
                make_columns(&["a", "b"]),
                Base::new().with_default_values(vec!["a".into(), "b".into()]),
            );
            mig.maintain_anonymous(a, &Index::hash_map(vec![0]));
            a
        })
        .await;
    let mut aq = g.view("a").await.unwrap().into_reader_handle().unwrap();
    let mut muta1 = g.table("a").await.unwrap();

    // send a value on a
    muta1
        .insert(vec![id.clone(), "bx".try_into().unwrap()])
        .await
        .unwrap();

    // check that it's there
    sleep().await;
    let res = aq.lookup(&[id.clone()], true).await.unwrap().into_vec();
    assert_eq!(res.len(), 1);
    assert!(res.contains(&vec![id.clone(), "bx".try_into().unwrap()]));

    // drop a column
    g.migrate(move |mig| {
        mig.drop_column(a, 1).unwrap();
        mig.maintain_anonymous(a, &Index::hash_map(vec![0]));
    })
    .await;

    // new mutator should only require one column
    // and should inject default for a.b
    let mut muta2 = g.table("a").await.unwrap();
    muta2.insert(vec![id.clone()]).await.unwrap();

    // so two rows now!
    sleep().await;
    let res = aq.lookup(&[id.clone()], true).await.unwrap().into_vec();
    assert_eq!(res.len(), 2);
    assert!(res.contains(&vec![id.clone(), "bx".try_into().unwrap()]));
    assert!(res.contains(&vec![id.clone(), "b".try_into().unwrap()]));

    // add a new column
    g.migrate(move |mig| {
        mig.add_column(a, dataflow_column("c"), "c".try_into().unwrap())
            .unwrap();
    })
    .await;

    // new mutator allows putting two values, and injects default for a.b
    let mut muta3 = g.table("a").await.unwrap();
    muta3
        .insert(vec![id.clone(), "cy".try_into().unwrap()])
        .await
        .unwrap();

    // using an old putter now should add default for c
    muta1
        .insert(vec![id.clone(), "bz".try_into().unwrap()])
        .await
        .unwrap();

    // using putter that knows of neither b nor c should result in defaults for both
    muta2.insert(vec![id.clone()]).await.unwrap();
    sleep().await;

    let res = aq.lookup(&[id.clone()], true).await.unwrap().into_vec();
    assert_eq!(res.len(), 5);
    // NOTE: if we *hadn't* read bx and b above, they would have also have c because it would have
    // been added when the lookups caused partial backfills.
    assert!(res.contains(&vec![id.clone(), "bx".try_into().unwrap()]));
    assert!(res.contains(&vec![id.clone(), "b".try_into().unwrap()]));
    assert!(res.contains(&vec![
        id.clone(),
        "b".try_into().unwrap(),
        "cy".try_into().unwrap()
    ]));
    assert!(res.contains(&vec![
        id.clone(),
        "bz".try_into().unwrap(),
        "c".try_into().unwrap()
    ]));
    assert!(res.contains(&vec![
        id.clone(),
        "b".try_into().unwrap(),
        "c".try_into().unwrap()
    ]));

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn key_on_added() {
    // set up graph
    let (mut g, shutdown_tx) = start_simple_unsharded("key_on_added").await;
    let a = g
        .migrate(|mig| {
            mig.add_base(
                "a",
                make_columns(&["a", "b"]),
                Base::new().with_default_values(vec![1.into(), 2.into()]),
            )
        })
        .await;

    // add a maintained view keyed on newly added column
    let _ = g
        .migrate(move |mig| {
            mig.add_column(a, dataflow_column("c"), 3.into()).unwrap();
            let b = mig.add_ingredient(
                "x",
                make_columns(&["c", "b"]),
                Project::new(
                    a,
                    vec![
                        DfExpr::Column {
                            index: 2,
                            ty: DfType::Unknown,
                        },
                        DfExpr::Column {
                            index: 1,
                            ty: DfType::Unknown,
                        },
                    ],
                ),
            );
            mig.maintain_anonymous(b, &Index::hash_map(vec![0]));
            b
        })
        .await;

    // make sure we can read (may trigger a replay)
    let mut bq = g.view("x").await.unwrap().into_reader_handle().unwrap();
    assert!(bq
        .lookup(&[3.into()], true)
        .await
        .unwrap()
        .into_vec()
        .is_empty());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn replay_during_replay() {
    // what we're trying to set up here is a case where a join receives a record with a value for
    // the join key that does not exist in the view the record was sent from. since joins only do
    // lookups into the origin view during forward processing when it receives things from the
    // right in a left join, that's what we have to construct.
    let mut g = Builder::for_tests();
    g.disable_partial();
    g.set_persistence(get_persistence_params("replay_during_replay"));
    let (mut g, shutdown_tx) = g.start_local().await.unwrap();
    let (a, u1, u2) = g
        .migrate(|mig| {
            // we need three bases:
            //
            //  - a will be the left side of the left join
            //  - u1 and u2 will be joined together with a regular one-to-one join to produce a
            //    partial view (remember, we need to miss in the source of the replay, so it must be
            //    partial).
            let a = mig.add_base(
                "a",
                make_columns(&["a"]),
                Base::new().with_default_values(vec![1.into()]),
            );
            let u1 = mig.add_base(
                "u1",
                make_columns(&["u"]),
                Base::new().with_default_values(vec![1.into()]),
            );
            let u2 = mig.add_base(
                "u2",
                make_columns(&["u", "a"]),
                Base::new().with_default_values(vec![1.into(), 2.into()]),
            );
            (a, u1, u2)
        })
        .await;

    // add our joins
    let (u, _) = g
        .migrate(move |mig| {
            // u = u1 * u2
            let j = Join::new(
                u1,
                u2,
                JoinType::Inner,
                vec![(0, 0)],
                vec![(Side::Left, 0), (Side::Right, 1)],
            );
            let u = mig.add_ingredient("u", make_columns(&["u", "a"]), j);
            let j = Join::new(
                a,
                u,
                JoinType::Left,
                vec![(0, 1)],
                vec![(Side::Left, 0), (Side::Right, 0)],
            );
            let end = mig.add_ingredient("end", make_columns(&["a", "u"]), j);
            mig.maintain_anonymous(end, &Index::hash_map(vec![0]));
            (u, end)
        })
        .await;

    // at this point, there's no secondary index on `u`, so any records that are forwarded from `u`
    // must already be present in the one index that `u` has. let's do some writes and check that
    // nothing crashes.

    let mut muta = g.table_by_index(a).await.unwrap();
    let mut mutu1 = g.table_by_index(u1).await.unwrap();
    let mut mutu2 = g.table_by_index(u2).await.unwrap();

    // as are numbers
    muta.insert(vec![1.into()]).await.unwrap();
    muta.insert(vec![2.into()]).await.unwrap();
    muta.insert(vec![3.into()]).await.unwrap();

    // us are strings
    mutu1.insert(vec!["a".try_into().unwrap()]).await.unwrap();
    mutu1.insert(vec!["b".try_into().unwrap()]).await.unwrap();
    mutu1.insert(vec!["c".try_into().unwrap()]).await.unwrap();

    // we want there to be data for all keys
    mutu2
        .insert(vec!["a".try_into().unwrap(), 1.into()])
        .await
        .unwrap();
    mutu2
        .insert(vec!["b".try_into().unwrap(), 2.into()])
        .await
        .unwrap();
    mutu2
        .insert(vec!["c".try_into().unwrap(), 3.into()])
        .await
        .unwrap();

    sleep().await;

    // since u and target are both partial, the writes should not actually have propagated through
    // yet. do a read to see that one makes it through correctly:
    let mut r = g.view("end").await.unwrap().into_reader_handle().unwrap();

    assert_eq!(
        r.lookup(&[1.into()], true).await.unwrap().into_vec(),
        vec![vec![1.into(), "a".try_into().unwrap()]]
    );

    // we now know that u has key a=1 in its index
    // now we add a secondary index on u.u
    g.migrate(move |mig| {
        mig.maintain_anonymous(u, &Index::hash_map(vec![0]));
    })
    .await;

    let mut second = g.view("u").await.unwrap().into_reader_handle().unwrap();

    // second is partial and empty, so any read should trigger a replay.
    // though that shouldn't interact with target in any way.
    assert_eq!(
        second
            .lookup(&["a".try_into().unwrap()], true)
            .await
            .unwrap()
            .into_vec(),
        vec![vec!["a".try_into().unwrap(), 1.into()]]
    );

    // now we get to the funky part.
    // we're going to request a second key from the secondary index on `u`, which causes that hole
    // to disappear. then we're going to do a write to `u2` that has that second key, but has an
    // "a" value for which u has a hole. that record is then going to be forwarded to *both*
    // children, and it'll be interesting to see what the join then does.
    assert_eq!(
        second
            .lookup(&["b".try_into().unwrap()], true)
            .await
            .unwrap()
            .into_vec(),
        vec![vec!["b".try_into().unwrap(), 2.into()]]
    );

    // u has a hole for a=2, but not for u=b, and so should forward this to both children
    mutu2
        .insert(vec!["b".try_into().unwrap(), 2.into()])
        .await
        .unwrap();

    sleep().await;

    // what happens if we now query for 2?
    assert_eq!(
        r.lookup(&[2.into()], true).await.unwrap().into_vec(),
        vec![
            vec![2.into(), "b".try_into().unwrap()],
            vec![2.into(), "b".try_into().unwrap()]
        ]
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "Ignoring sharded tests"]
async fn cascading_replays_with_sharding() {
    let (mut g, shutdown_tx) = start_simple("cascading_replays_with_sharding").await;

    // add each two bases. these are initially unsharded, but f will end up being sharded by u1,
    // while v will be sharded by u

    // force v to be in a different domain by adding it in a separate migration
    let v = g
        .migrate(|mig| {
            mig.add_base(
                "v",
                make_columns(&["u", "s"]),
                Base::new().with_default_values(vec!["".into(), 1.into()]),
            )
        })
        .await;
    // now add the rest
    let _ = g
        .migrate(move |mig| {
            let f = mig.add_base(
                "f",
                make_columns(&["f1", "f2"]),
                Base::new().with_default_values(vec!["".into(), "".into()]),
            );
            // add a join
            let jb = Join::new(
                f,
                v,
                JoinType::Inner,
                vec![(0, 0)],
                vec![(Side::Left, 0), (Side::Right, 1), (Side::Left, 1)],
            );
            let j = mig.add_ingredient("j", make_columns(&["u", "s", "f2"]), jb);
            // aggregate over the join. this will force a shard merger to be inserted because the
            // group-by column ("f2") isn't the same as the join's output sharding column ("f1"/"u")
            let a = Aggregation::Count
                .over(j, 0, &[2], &DfType::Unknown)
                .unwrap();
            let end = mig.add_ingredient("end", make_columns(&["u", "c"]), a);
            mig.maintain_anonymous_with_reader_processing(
                end,
                &Index::hash_map(vec![0]),
                ReaderProcessing::new(None, None, Some(vec![0, 1]), None, None).unwrap(),
            );
            (j, end)
        })
        .await;

    let mut mutf = g.table("f").await.unwrap();
    let mut mutv = g.table("v").await.unwrap();

    //                f1           f2
    mutf.insert(vec!["u1".try_into().unwrap(), "u3".try_into().unwrap()])
        .await
        .unwrap();
    mutf.insert(vec!["u2".try_into().unwrap(), "u3".try_into().unwrap()])
        .await
        .unwrap();
    mutf.insert(vec!["u3".try_into().unwrap(), "u1".try_into().unwrap()])
        .await
        .unwrap();

    //                u
    mutv.insert(vec!["u1".try_into().unwrap(), 1.into()])
        .await
        .unwrap();
    mutv.insert(vec!["u2".try_into().unwrap(), 1.into()])
        .await
        .unwrap();
    mutv.insert(vec!["u3".try_into().unwrap(), 1.into()])
        .await
        .unwrap();

    sleep().await;

    let mut e = g.view("end").await.unwrap().into_reader_handle().unwrap();

    assert_eq!(
        e.lookup(&["u1".try_into().unwrap()], true)
            .await
            .unwrap()
            .into_vec(),
        vec![vec!["u1".try_into().unwrap(), 1.into()]]
    );
    assert_eq!(
        e.lookup(&["u2".try_into().unwrap()], true)
            .await
            .unwrap()
            .into_vec(),
        Vec::<Vec<DfValue>>::new()
    );
    assert_eq!(
        e.lookup(&["u3".try_into().unwrap()], true)
            .await
            .unwrap()
            .into_vec(),
        vec![vec!["u3".try_into().unwrap(), 2.into()]]
    );

    sleep().await;

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn replay_multiple_keys_then_write() {
    let (mut g, shutdown_tx) = start_simple_unsharded("replay_multiple_keys_then_write").await;
    g.extend_recipe(
        ChangeList::from_str(
            "
        CREATE TABLE t (id INTEGER PRIMARY KEY, value INTEGER);
        CREATE CACHE q FROM SELECT id, value FROM t WHERE id = ?;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let mut t = g.table("t").await.unwrap();
    let mut q = g.view("q").await.unwrap().into_reader_handle().unwrap();

    t.insert_many(vec![
        vec![DfValue::from(1), DfValue::from(1)],
        vec![DfValue::from(2), DfValue::from(2)],
    ])
    .await
    .unwrap();

    q.lookup(&[1.into()], true).await.unwrap();
    q.lookup(&[2.into()], true).await.unwrap();

    t.update(
        vec![DfValue::from(1)],
        vec![(1, Modification::Set(2.into()))],
    )
    .await
    .unwrap();

    sleep().await;

    let res = &q.lookup(&[1.into()], true).await.unwrap().into_vec()[0];

    assert_eq!(*res, vec![DfValue::from(1), DfValue::from(2)]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn full_aggregation_with_bogokey() {
    // set up graph
    let (mut g, shutdown_tx) = start_simple_unsharded("full_aggregation_with_bogokey").await;
    let base = g
        .migrate(|mig| {
            mig.add_base(
                "base",
                make_columns(&["x"]),
                Base::new().with_default_values(vec![1.into()]),
            )
        })
        .await;

    // add an aggregation over the base with a bogo key.
    // in other words, the aggregation is across all rows.
    let _ = g
        .migrate(move |mig| {
            let bogo = mig.add_ingredient(
                "bogo",
                make_columns(&["x", "bogo"]),
                Project::new(
                    base,
                    vec![
                        DfExpr::Column {
                            index: 0,
                            ty: DfType::Unknown,
                        },
                        DfExpr::Literal {
                            val: 0.into(),
                            ty: DfType::Int,
                        },
                    ],
                ),
            );
            let agg = mig.add_ingredient(
                "agg",
                make_columns(&["bogo", "count"]),
                Aggregation::Count
                    .over(bogo, 0, &[1], &DfType::Unknown)
                    .unwrap(),
            );
            mig.maintain_anonymous_with_reader_processing(
                agg,
                &Index::hash_map(vec![0]),
                ReaderProcessing::new(None, None, Some(vec![0, 1]), None, None).unwrap(),
            );
            agg
        })
        .await;

    let mut aggq = g.view("agg").await.unwrap().into_reader_handle().unwrap();
    let mut base = g.table_by_index(base).await.unwrap();

    // insert some values
    base.insert(vec![1.into()]).await.unwrap();
    base.insert(vec![2.into()]).await.unwrap();
    base.insert(vec![3.into()]).await.unwrap();

    // give it some time to propagate
    sleep().await;

    // send a query to aggregation materialization
    assert_eq!(
        aggq.lookup(&[0.into()], true).await.unwrap().into_vec(),
        vec![vec![0.into(), 3.into()]]
    );

    // update value again
    base.insert(vec![4.into()]).await.unwrap();

    // give it some time to propagate
    sleep().await;

    // check that value was updated again
    assert_eq!(
        aggq.lookup(&[0.into()], true).await.unwrap().into_vec(),
        vec![vec![0.into(), 4.into()]]
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn pkey_then_full_table_with_bogokey() {
    let (mut g, shutdown_tx) = start_simple_unsharded("pkey_then_full_table_with_bogokey").await;
    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE posts (id int, title text)",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    g.extend_recipe(
        ChangeList::from_str(
            "CREATE CACHE by_id FROM SELECT id, title FROM posts WHERE id = ?",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    g.extend_recipe(
        ChangeList::from_str(
            "CREATE CACHE all_posts FROM SELECT id, title FROM posts",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut posts = g.table("posts").await.unwrap();
    let mut by_id = g.view("by_id").await.unwrap().into_reader_handle().unwrap();
    let mut all_posts = g
        .view("all_posts")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    let rows: Vec<Vec<DfValue>> = (0..10)
        .map(|n| vec![n.into(), format!("post {}", n).try_into().unwrap()])
        .collect();
    posts.insert_many(rows.clone()).await.unwrap();

    sleep().await;

    // Looking up post with id 1 should return the correct post.
    assert_eq!(
        by_id.lookup(&[1.into()], true).await.unwrap().into_vec(),
        vec![vec![DfValue::from(1), DfValue::from("post 1")]]
    );

    // Looking up all posts using a 0 bogokey should return all posts.
    let rows_with_bogokey: Vec<Vec<DfValue>> = (0..10)
        .map(|n| vec![n.into(), format!("post {}", n).into()])
        .collect();
    assert_eq!(
        all_posts
            .lookup(&[0.into()], true)
            .await
            .unwrap()
            .into_vec(),
        rows_with_bogokey
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn materialization_frontier() {
    // set up graph
    let (mut g, shutdown_tx) = start_simple_unsharded("materialization_frontier").await;
    let (article, vote) = g
        .migrate(|mig| {
            // migrate

            // add article base node
            let article = mig.add_base("article", make_columns(&["id", "title"]), Base::default());

            // add vote base table
            let vote = mig.add_base(
                "vote",
                make_columns(&["user", "id"]),
                Base::new().with_primary_key([0, 1]),
            );

            // add vote count
            let vc = mig.add_ingredient(
                "votecount",
                make_columns(&["id", "votes"]),
                Aggregation::Count
                    .over(vote, 0, &[1], &DfType::Unknown)
                    .unwrap(),
            );
            mig.mark_shallow(vc);

            // add final join using first field from article and first from vc
            let j = Join::new(
                article,
                vc,
                JoinType::Left,
                vec![(0, 0)],
                vec![(Side::Left, 0), (Side::Left, 1), (Side::Right, 1)],
            );
            let end = mig.add_ingredient("awvc", make_columns(&["id", "title", "votes"]), j);

            let ri = mig.maintain_anonymous(end, &Index::hash_map(vec![0]));
            mig.mark_shallow(ri);
            (article, vote)
        })
        .await;

    let mut a = g.table_by_index(article).await.unwrap();
    let mut v = g.table_by_index(vote).await.unwrap();
    let mut r = g.view("awvc").await.unwrap().into_reader_handle().unwrap();

    // seed votes
    v.insert(vec!["a".try_into().unwrap(), 1.into()])
        .await
        .unwrap();
    v.insert(vec!["a".try_into().unwrap(), 2.into()])
        .await
        .unwrap();
    v.insert(vec!["b".try_into().unwrap(), 1.into()])
        .await
        .unwrap();
    v.insert(vec!["c".try_into().unwrap(), 2.into()])
        .await
        .unwrap();
    v.insert(vec!["d".try_into().unwrap(), 2.into()])
        .await
        .unwrap();

    // seed articles
    a.insert(vec![1.into(), "Hello world #1".try_into().unwrap()])
        .await
        .unwrap();
    a.insert(vec![2.into(), "Hello world #2".try_into().unwrap()])
        .await
        .unwrap();
    sleep().await;

    // we want to alternately read article 1 and 2, knowing that reading one will purge the other.
    // we first "warm up" by reading both to ensure all other necessary state is present.
    let one = 1.into();
    let two = 2.into();
    assert_eq!(
        r.lookup(&[one], true).await.unwrap().into_vec(),
        vec![vec![
            1.into(),
            "Hello world #1".try_into().unwrap(),
            2.into()
        ]]
    );
    assert_eq!(
        r.lookup(&[two], true).await.unwrap().into_vec(),
        vec![vec![
            2.into(),
            "Hello world #2".try_into().unwrap(),
            3.into()
        ]]
    );

    for _ in 0..1_000 {
        for &id in &[1, 2] {
            let r = r.lookup(&[id.into()], true).await.unwrap().into_vec();
            match id {
                1 => {
                    assert_eq!(
                        r,
                        vec![vec![
                            1.into(),
                            "Hello world #1".try_into().unwrap(),
                            2.into()
                        ]]
                    );
                }
                2 => {
                    assert_eq!(
                        r,
                        vec![vec![
                            2.into(),
                            "Hello world #2".try_into().unwrap(),
                            3.into()
                        ]]
                    );
                }
                _ => unreachable!(),
            }
        }
    }

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn crossing_migration() {
    // set up graph
    let (mut g, shutdown_tx) = start_simple_unsharded("crossing_migration").await;
    let (a, b) = g
        .migrate(|mig| {
            let a = mig.add_base("a", make_columns(&["a", "b"]), Base::default());
            let b = mig.add_base("b", make_columns(&["a", "b"]), Base::default());
            (a, b)
        })
        .await;
    let mut muta = g.table_by_index(a).await.unwrap();
    let mut mutb = g.table_by_index(b).await.unwrap();

    let _ = g
        .migrate(move |mig| {
            let mut emits = HashMap::new();
            emits.insert(a, vec![0, 1]);
            emits.insert(b, vec![0, 1]);
            let u = Union::new(emits, union::DuplicateMode::UnionAll).unwrap();
            let c = mig.add_ingredient("c", make_columns(&["a", "b"]), u);
            mig.maintain_anonymous(c, &Index::hash_map(vec![0]));
            c
        })
        .await;

    let mut cq = g.view("c").await.unwrap().into_reader_handle().unwrap();

    let id: DfValue = 1.into();

    // send a value on a
    muta.insert(vec![id.clone(), 2.into()]).await.unwrap();
    sleep().await;

    assert_eq!(
        cq.lookup(&[id.clone()], true).await.unwrap().into_vec(),
        vec![vec![id.clone(), 2.into()]]
    );

    // update value again
    mutb.insert(vec![id.clone(), 4.into()]).await.unwrap();
    sleep().await;

    let res = cq.lookup(&[id.clone()], true).await.unwrap().into_vec();
    assert_eq!(res.len(), 2);
    assert!(res.contains(&vec![id.clone(), 2.into()]));
    assert!(res.contains(&vec![id.clone(), 4.into()]));

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn independent_domain_migration() {
    let id: DfValue = 1.into();

    // set up graph
    let (mut g, shutdown_tx) = start_simple_unsharded("independent_domain_migration").await;
    let a = g
        .migrate(|mig| {
            let a = mig.add_base("a", make_columns(&["a", "b"]), Base::default());
            mig.maintain_anonymous(a, &Index::hash_map(vec![0]));
            a
        })
        .await;

    let mut aq = g.view("a").await.unwrap().into_reader_handle().unwrap();
    let mut muta = g.table_by_index(a).await.unwrap();

    // send a value on a
    muta.insert(vec![id.clone(), 2.into()]).await.unwrap();

    // give it some time to propagate
    sleep().await;

    // check that a got it
    assert_eq!(
        aq.lookup(&[id.clone()], true).await.unwrap().into_vec(),
        vec![vec![1.into(), 2.into()]]
    );

    // add unrelated node b in a migration
    let b = g
        .migrate(|mig| {
            let b = mig.add_base("b", make_columns(&["a", "b"]), Base::default());
            mig.maintain_anonymous(b, &Index::hash_map(vec![0]));
            b
        })
        .await;

    let mut bq = g.view("b").await.unwrap().into_reader_handle().unwrap();
    let mut mutb = g.table_by_index(b).await.unwrap();

    // send a value on b
    mutb.insert(vec![id.clone(), 4.into()]).await.unwrap();

    // give it some time to propagate
    sleep().await;

    // check that a got it
    assert_eq!(
        bq.lookup(&[id.clone()], true).await.unwrap().into_vec(),
        vec![vec![1.into(), 4.into()]]
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn domain_amend_migration() {
    // set up graph
    let (mut g, shutdown_tx) = start_simple_unsharded("domain_amend_migration").await;
    let (a, b) = g
        .migrate(|mig| {
            let a = mig.add_base("a", make_columns(&["a", "b"]), Base::default());
            let b = mig.add_base("b", make_columns(&["a", "b"]), Base::default());
            (a, b)
        })
        .await;
    let mut muta = g.table_by_index(a).await.unwrap();
    let mut mutb = g.table_by_index(b).await.unwrap();

    let _ = g
        .migrate(move |mig| {
            let mut emits = HashMap::new();
            emits.insert(a, vec![0, 1]);
            emits.insert(b, vec![0, 1]);
            let u = Union::new(emits, union::DuplicateMode::UnionAll).unwrap();
            let c = mig.add_ingredient("c", make_columns(&["a", "b"]), u);
            mig.maintain_anonymous(c, &Index::hash_map(vec![0]));
            c
        })
        .await;
    let mut cq = g.view("c").await.unwrap().into_reader_handle().unwrap();

    let id: DfValue = 1.into();

    // send a value on a
    muta.insert(vec![id.clone(), 2.into()]).await.unwrap();
    sleep().await;

    assert_eq!(
        cq.lookup(&[id.clone()], true).await.unwrap().into_vec(),
        vec![vec![id.clone(), 2.into()]]
    );

    // update value again
    mutb.insert(vec![id.clone(), 4.into()]).await.unwrap();
    sleep().await;

    let res = cq.lookup(&[id.clone()], true).await.unwrap().into_vec();
    assert_eq!(res.len(), 2);
    assert!(res.contains(&vec![id.clone(), 2.into()]));
    assert!(res.contains(&vec![id.clone(), 4.into()]));

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn migration_depends_on_unchanged_domain() {
    // here's the case we want to test: before the migration, we have some domain that contains
    // some materialized node n, as well as an egress node. after the migration, we add a domain
    // that depends on n being materialized. the tricky part here is that n's domain hasn't changed
    // as far as the system is aware (in particular, because it didn't need to add an egress node).
    // this is tricky, because the system must realize that n is materialized, even though it
    // normally wouldn't even look at that part of the data flow graph!

    let (mut g, shutdown_tx) =
        start_simple_unsharded("migration_depends_on_unchanged_domain").await;
    let left = g
        .migrate(|mig| {
            // base node, so will be materialized
            let left = mig.add_base("foo", make_columns(&["a", "b"]), Base::default());

            // node in different domain that depends on foo causes egress to be added
            mig.add_ingredient("bar", make_columns(&["a", "b"]), Identity::new(left));
            left
        })
        .await;

    g.migrate(move |mig| {
        // joins require their inputs to be materialized
        // we need a new base as well so we can actually make a join
        let tmp = mig.add_base("tmp", make_columns(&["a", "b"]), Base::default());
        let j = Join::new(
            left,
            tmp,
            JoinType::Inner,
            vec![(0, 0)],
            vec![(Side::Left, 0), (Side::Right, 1)],
        );
        mig.add_ingredient("join", make_columns(&["a", "b"]), j);
    })
    .await;

    shutdown_tx.shutdown().await;
}

async fn do_full_vote_migration(sharded: bool, old_puts_after: bool) {
    let name = format!("do_full_vote_migration_{}", old_puts_after);
    let (mut g, shutdown_tx) = if sharded {
        start_simple(&name).await
    } else {
        start_simple_unsharded(&name).await
    };
    let (article, vote, vc, _end) = g
        .migrate(|mig| {
            // migrate

            // add article base node
            let article = mig.add_base("article", make_columns(&["id", "title"]), Base::default());

            // add vote base table
            // NOTE: the double-column key here means that we can't shard vote
            let vote = mig.add_base(
                "vote",
                make_columns(&["user", "id"]),
                Base::new().with_primary_key([0, 1]),
            );

            // add vote count
            let vc = mig.add_ingredient(
                "votecount",
                make_columns(&["id", "votes"]),
                Aggregation::Count
                    .over(vote, 0, &[1], &DfType::Unknown)
                    .unwrap(),
            );

            // add final join using first field from article and first from vc
            let j = Join::new(
                article,
                vc,
                JoinType::Left,
                vec![(0, 0)],
                vec![(Side::Left, 0), (Side::Left, 1), (Side::Right, 1)],
            );
            let end = mig.add_ingredient("awvc", make_columns(&["id", "title", "votes"]), j);

            mig.maintain_anonymous(end, &Index::hash_map(vec![0]));
            (article, vote, vc, end)
        })
        .await;
    let mut muta = g.table_by_index(article).await.unwrap();
    let mut mutv = g.table_by_index(vote).await.unwrap();

    let n = 250i64;
    let title: DfValue = "foo".try_into().unwrap();
    let raten: DfValue = 5.into();

    for i in 0..n {
        muta.insert(vec![i.into(), title.clone()]).await.unwrap();
    }
    for i in 0..n {
        mutv.insert(vec![1.into(), i.into()]).await.unwrap();
    }

    let mut last = g.view("awvc").await.unwrap().into_reader_handle().unwrap();
    thread::sleep(get_settle_time().checked_mul(3).unwrap());
    for i in 0..n {
        let rows = last.lookup(&[i.into()], true).await.unwrap().into_vec();
        assert!(!rows.is_empty(), "every article should be voted for");
        assert_eq!(rows.len(), 1, "every article should have only one entry");
        let row = rows.into_iter().next().unwrap();
        assert_eq!(
            row[0],
            i.into(),
            "each article result should have the right id"
        );
        assert_eq!(row[1], title, "all articles should have title 'foo'");
        assert_eq!(row[2], 1.into(), "all articles should have one vote");
    }

    // migrate
    let rating = g
        .migrate(move |mig| {
            // add new "ratings" base table
            let rating = mig.add_base(
                "rating",
                make_columns(&["user", "id", "stars"]),
                Base::default(),
            );

            // add sum of ratings
            let rs = mig.add_ingredient(
                "rsum",
                make_columns(&["id", "total"]),
                Aggregation::Sum
                    .over(rating, 2, &[1], &DfType::Unknown)
                    .unwrap(),
            );

            // join vote count and rsum (and in theory, sum them)
            let j = Join::new(
                rs,
                vc,
                JoinType::Left,
                vec![(0, 0)],
                vec![(Side::Left, 0), (Side::Left, 1), (Side::Right, 1)],
            );
            let total = mig.add_ingredient("total", make_columns(&["id", "ratings", "votes"]), j);

            // finally, produce end result
            let j = Join::new(
                article,
                total,
                JoinType::Inner,
                vec![(0, 0)],
                vec![
                    (Side::Left, 0),
                    (Side::Left, 1),
                    (Side::Right, 1),
                    (Side::Right, 2),
                ],
            );
            let newend =
                mig.add_ingredient("awr", make_columns(&["id", "title", "ratings", "votes"]), j);
            mig.maintain_anonymous(newend, &Index::hash_map(vec![0]));

            rating
        })
        .await;

    let mut last = g.view("awr").await.unwrap().into_reader_handle().unwrap();
    let mut mutr = g.table_by_index(rating).await.unwrap();
    for i in 0..n {
        if old_puts_after {
            mutv.insert(vec![2.into(), i.into()]).await.unwrap();
        }
        mutr.insert(vec![2.into(), i.into(), raten.clone()])
            .await
            .unwrap();
    }

    thread::sleep(get_settle_time().checked_mul(3).unwrap());
    for i in 0..n {
        let rows = last.lookup(&[i.into()], true).await.unwrap().into_vec();
        assert!(!rows.is_empty(), "every article should be voted for");
        assert_eq!(rows.len(), 1, "every article should have only one entry");
        let row = rows.into_iter().next().unwrap();
        assert_eq!(
            row[0],
            i.into(),
            "each article result should have the right id"
        );
        assert_eq!(row[1], title, "all articles should have title 'foo'");
        assert_eq!(
            row[2],
            Decimal::from(5).into(),
            "all articles should have one 5-star rating"
        );
        if old_puts_after {
            assert_eq!(row[3], 2.into(), "all articles should have two votes");
        } else {
            assert_eq!(row[3], 1.into(), "all articles should have one vote");
        }
    }

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "Ignoring sharded tests"]
async fn full_vote_migration_only_new() {
    do_full_vote_migration(true, false).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "Ignoring sharded tests"]
async fn full_vote_migration_new_and_old() {
    do_full_vote_migration(true, true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn full_vote_migration_new_and_old_unsharded() {
    do_full_vote_migration(false, true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn live_writes() {
    readyset_tracing::init_test_logging();
    let (mut g, shutdown_tx) = start_simple_unsharded("live_writes").await;
    let (vote, vc) = g
        .migrate(|mig| {
            // migrate

            // add vote base table
            let vote = mig.add_base("vote", make_columns(&["user", "id"]), Base::default());

            // add vote count
            let vc = mig.add_ingredient(
                "votecount",
                make_columns(&["id", "votes"]),
                Aggregation::Count
                    .over(vote, 0, &[1], &DfType::Unknown)
                    .unwrap(),
            );

            mig.maintain_anonymous_with_reader_processing(
                vc,
                &Index::hash_map(vec![0]),
                ReaderProcessing::new(None, None, Some(vec![0, 1]), None, None).unwrap(),
            );
            (vote, vc)
        })
        .await;

    let mut vc_state = g
        .view("votecount")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    let mut add = g.table_by_index(vote).await.unwrap();

    let ids = 1000;
    let votes = 7;

    // continuously write to vote
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        // we need to use a batch putter because otherwise we'd wait for 7000 batch intervals
        let fut =
            add.perform_all((0..votes).flat_map(|_| (0..ids).map(|i| vec![0.into(), i.into()])));
        fut.await.unwrap();
        tx.send(()).unwrap();
    });

    // let a few writes through to make migration take a while
    sleep().await;

    // now do a migration that's going to have to copy state
    let _ = g
        .migrate(move |mig| {
            let vc2 = mig.add_ingredient(
                "votecount2",
                make_columns(&["id", "votes"]),
                Aggregation::Sum
                    .over(vc, 1, &[0], &DfType::Unknown)
                    .unwrap(),
            );
            mig.maintain_anonymous_with_reader_processing(
                vc2,
                &Index::hash_map(vec![0]),
                ReaderProcessing::new(None, None, Some(vec![0, 1]), None, None).unwrap(),
            );
            vc2
        })
        .await;

    let mut vc2_state = g
        .view("votecount2")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    // TODO: check that the writer did indeed complete writes during the migration

    // wait for writer to finish
    rx.await.unwrap();

    // allow the system to catch up with the last writes
    sleep().await;

    // check that all writes happened the right number of times
    for i in 0..ids {
        assert_eq!(
            vc_state.lookup(&[i.into()], true).await.unwrap().into_vec(),
            vec![vec![i.into(), votes.into()]]
        );
        assert_eq!(
            vc2_state
                .lookup(&[i.into()], true)
                .await
                .unwrap()
                .into_vec(),
            vec![vec![i.into(), Decimal::from(votes).into()]]
        );
    }

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn state_replay_migration_query() {
    // similar to test above, except we will have a materialized Reader node that we're going to
    // read from rather than relying on forwarding. to further stress the graph, *both* base nodes
    // are created and populated before the migration, meaning we have to replay through a join.

    let (mut g, shutdown_tx) = start_simple_unsharded("state_replay_migration_query").await;
    let (a, b) = g
        .migrate(|mig| {
            let a = mig.add_base("a", make_columns(&["x", "y"]), Base::default());
            let b = mig.add_base("b", make_columns(&["x", "z"]), Base::default());

            (a, b)
        })
        .await;
    let mut muta = g.table_by_index(a).await.unwrap();
    let mut mutb = g.table_by_index(b).await.unwrap();

    // make a couple of records
    muta.insert(vec![1.into(), "a".try_into().unwrap()])
        .await
        .unwrap();
    muta.insert(vec![1.into(), "b".try_into().unwrap()])
        .await
        .unwrap();
    muta.insert(vec![2.into(), "c".try_into().unwrap()])
        .await
        .unwrap();
    mutb.insert(vec![1.into(), "n".try_into().unwrap()])
        .await
        .unwrap();
    mutb.insert(vec![2.into(), "o".try_into().unwrap()])
        .await
        .unwrap();

    let _ = g
        .migrate(move |mig| {
            // add join and a reader node
            let j = Join::new(
                a,
                b,
                JoinType::Inner,
                vec![(0, 0)],
                vec![(Side::Left, 0), (Side::Left, 1), (Side::Right, 1)],
            );
            let j = mig.add_ingredient("j", make_columns(&["x", "y", "z"]), j);

            // we want to observe what comes out of the join
            mig.maintain_anonymous(j, &Index::hash_map(vec![0]));
            j
        })
        .await;
    let mut out = g.view("j").await.unwrap().into_reader_handle().unwrap();
    sleep().await;

    // if all went according to plan, the join should now be fully populated!
    // there are (/should be) two records in a with x == 1
    // they may appear in any order
    let res = out.lookup(&[1.into()], true).await.unwrap().into_vec();
    assert!(res.contains(&vec![
        1.into(),
        "a".try_into().unwrap(),
        "n".try_into().unwrap()
    ]));
    assert!(res.contains(&vec![
        1.into(),
        "b".try_into().unwrap(),
        "n".try_into().unwrap()
    ]));

    // there are (/should be) one record in a with x == 2
    assert_eq!(
        out.lookup(&[2.into()], true).await.unwrap().into_vec(),
        vec![vec![
            2.into(),
            "c".try_into().unwrap(),
            "o".try_into().unwrap()
        ]]
    );

    // there are (/should be) no records with x == 3
    assert!(out
        .lookup(&[3.into()], true)
        .await
        .unwrap()
        .into_vec()
        .is_empty());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn recipe_activates_and_migrates() {
    let r_txt = "CREATE TABLE b (a text, c text, x text);\n";
    let r1_txt = "CREATE CACHE qa FROM SELECT a FROM b;\n
                  CREATE CACHE qb FROM SELECT a, c FROM b WHERE a = 42;";

    let (mut g, shutdown_tx) = start_simple_unsharded("recipe_activates_and_migrates").await;
    g.extend_recipe(ChangeList::from_str(r_txt, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();
    // one base node
    assert_eq!(g.tables().await.unwrap().len(), 1);

    g.extend_recipe(ChangeList::from_str(r1_txt, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();
    // still one base node
    assert_eq!(g.tables().await.unwrap().len(), 1);
    // two leaf nodes
    assert_eq!(g.views().await.unwrap().len(), 2);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn recipe_activates_and_migrates_with_join() {
    let r_txt = "CREATE TABLE a (x int, y int, z int);\n
                 CREATE TABLE b (r int, s int);\n";
    let r1_txt = "CREATE CACHE q FROM SELECT y, s FROM a, b WHERE a.x = b.r;";

    let (mut g, shutdown_tx) =
        start_simple_unsharded("recipe_activates_and_migrates_with_join").await;
    g.extend_recipe(ChangeList::from_str(r_txt, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    // two base nodes
    assert_eq!(g.tables().await.unwrap().len(), 2);

    g.extend_recipe(ChangeList::from_str(r1_txt, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    // still two base nodes
    assert_eq!(g.tables().await.unwrap().len(), 2);
    // one leaf node
    assert_eq!(g.views().await.unwrap().len(), 1);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn finkelstein1982_queries() {
    use std::fs::File;
    use std::io::Read;

    // set up graph
    let (mut g, shutdown_tx) = start_simple_unsharded("finkelstein1982_queries").await;
    g.migrate(|mig| {
        let mut inc = SqlIncorporator::default();
        let mut f = File::open("tests/finkelstein82.txt").unwrap();
        let mut s = String::new();

        // Load queries
        f.read_to_string(&mut s).unwrap();
        let lines: Vec<String> = s
            .lines()
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .map(|l| {
                if !(l.ends_with('\n') || l.ends_with(';')) {
                    String::from(l) + "\n"
                } else {
                    String::from(l)
                }
            })
            .collect();

        // Add them one by one
        for q in lines.iter() {
            let q = parse_query(nom_sql::Dialect::MySQL, q).unwrap();
            match q {
                SqlQuery::CreateTable(stmt) => {
                    let stmt = inc
                        .rewrite(stmt, &[], Dialect::DEFAULT_MYSQL, None)
                        .unwrap();
                    inc.add_table(stmt.table, stmt.body.unwrap(), mig).unwrap();
                }
                SqlQuery::Select(stmt) => {
                    inc.add_query(None, stmt, false, &[], mig).unwrap();
                }
                _ => panic!("unexpected query type"),
            }
        }
    })
    .await;

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn node_removal() {
    let (mut g, shutdown_tx) = start_simple_unsharded("domain_removal").await;
    let (a, b, cid) = g
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
            let cid = mig.maintain_anonymous(c, &Index::hash_map(vec![0]));
            (a, b, cid)
        })
        .await;

    let mut cq = g.view("c").await.unwrap().into_reader_handle().unwrap();
    let mut muta = g.table_by_index(a).await.unwrap();
    let mut mutb = g.table_by_index(b).await.unwrap();
    let id: DfValue = 1.into();

    assert_eq!(*muta.table_name(), "a".into());
    assert_eq!(muta.columns(), &["a", "b"]);

    // send a value on a
    muta.insert(vec![id.clone(), 2.into()]).await.unwrap();

    // give it some time to propagate
    sleep().await;

    // send a query to c
    assert_eq!(
        cq.lookup(&[id.clone()], true).await.unwrap().into_vec(),
        vec![vec![1.into(), 2.into()]]
    );

    g.remove_node(cid).await.unwrap();

    // update value again
    mutb.insert(vec![id.clone(), 4.into()]).await.unwrap();

    // give it some time to propagate
    sleep().await;

    // // check that value was updated again
    // let res = cq.lookup(&[id.clone()], true).await.unwrap();
    // assert!(res.iter().any(|r| r == &vec![id.clone(), 2.into()]));
    // assert!(res.iter().any(|r| r == &vec![id.clone(), 4.into()]));

    // Delete first record
    muta.delete(vec![id.clone()]).await.unwrap();

    // give it some time to propagate
    sleep().await;

    // // send a query to c
    // assert_eq!(
    //     cq.lookup(&[id.clone()], true).await,
    //     Ok(vec![vec![1.into(), 4.into()]])
    // );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn remove_query() {
    readyset_tracing::init_test_logging();
    let r_txt = "CREATE TABLE b (a int, c text, x text);\n
                 CREATE CACHE qa FROM SELECT a FROM b;\n
                 CREATE CACHE qb FROM SELECT a, c FROM b WHERE a = 42;";

    let r2_txt = "
        DROP CACHE qb;
        CREATE TABLE b (a int, c text, x text);
        CREATE CACHE qa FROM SELECT a FROM b;";

    let (mut g, shutdown_tx) = start_simple_unsharded("remove_query").await;
    g.extend_recipe(ChangeList::from_str(r_txt, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();
    assert_eq!(g.tables().await.unwrap().len(), 1);
    assert_eq!(g.views().await.unwrap().len(), 2);

    let mut mutb = g.table("b").await.unwrap();
    let mut qa = g.view("qa").await.unwrap().into_reader_handle().unwrap();
    let mut qb = g.view("qb").await.unwrap().into_reader_handle().unwrap();

    mutb.insert(vec![
        42.into(),
        "2".try_into().unwrap(),
        "3".try_into().unwrap(),
    ])
    .await
    .unwrap();
    mutb.insert(vec![
        1.into(),
        "4".try_into().unwrap(),
        "5".try_into().unwrap(),
    ])
    .await
    .unwrap();
    sleep().await;

    assert_eq!(
        qa.lookup(&[0.into()], true).await.unwrap().into_vec().len(),
        2
    );
    assert_eq!(
        qb.lookup(&[0.into()], true).await.unwrap().into_vec().len(),
        1
    );

    // Remove qb and check that the graph still functions as expected.
    g.extend_recipe(ChangeList::from_str(r2_txt, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();
    assert_eq!(g.tables().await.unwrap().len(), 1);
    assert_eq!(g.views().await.unwrap().len(), 1);
    g.view("qb").await.unwrap_err();

    mutb.insert(vec![
        42.into(),
        "6".try_into().unwrap(),
        "7".try_into().unwrap(),
    ])
    .await
    .unwrap();
    sleep().await;

    match qb.lookup(&[0.into()], true).await.unwrap_err() {
        // FIXME(eta): this sucks and should be looking for ViewNotYetAvailable.
        readyset_errors::ReadySetError::ViewError { .. } => {}
        e => unreachable!("{:?}", e),
    }

    shutdown_tx.shutdown().await;
}

macro_rules! get {
    ($private:ident, $public:ident, $uid:expr, $aid:expr) => {{
        // combine private and public results
        // also, there's currently a bug where MIR doesn't guarantee the order of parameters, so we
        // try both O:)
        let mut v = $private
            .lookup(&[$uid.into(), $aid.try_into().unwrap()], true)
            .await
            .unwrap()
            .into_vec();
        v.append(
            &mut $private
                .lookup(&[$aid.try_into().unwrap(), $uid.into()], true)
                .await
                .unwrap()
                .into_vec(),
        );
        v.append(
            &mut $public
                .lookup(&[$aid.try_into().unwrap()], true)
                .await
                .unwrap()
                .into_vec(),
        );
        eprintln!("check {} as {}: {:?}", $aid, $uid, v);
        v
    }};
}

#[tokio::test(flavor = "multi_thread")]
async fn albums() {
    let (mut g, shutdown_tx) = start_simple_unsharded("albums").await;
    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE friend (usera int, userb int);
                 CREATE TABLE album (a_id text, u_id int, public tinyint(1));
                 CREATE TABLE photo (p_id text, album text);",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    g.extend_recipe(ChangeList::from_str("CREATE VIEW album_friends AS \
                   (SELECT album.a_id AS aid, friend.userb AS uid FROM album JOIN friend ON (album.u_id = friend.usera) WHERE album.public = 0) \
                   UNION \
                   (SELECT album.a_id AS aid, friend.usera AS uid FROM album JOIN friend ON (album.u_id = friend.userb) WHERE album.public = 0) \
                   UNION \
                   (SELECT album.a_id AS aid, album.u_id AS uid FROM album WHERE album.public = 0);
CREATE CACHE private_photos FROM \
SELECT photo.p_id FROM photo JOIN album_friends ON (photo.album = album_friends.aid) WHERE album_friends.uid = ? AND photo.album = ?;
CREATE CACHE public_photos FROM \
SELECT photo.p_id FROM photo JOIN album ON (photo.album = album.a_id) WHERE album.public = 1 AND album.a_id = ?;", Dialect::DEFAULT_MYSQL).unwrap()).await.unwrap();

    let mut friends = g.table("friend").await.unwrap();
    let mut albums = g.table("album").await.unwrap();
    let mut photos = g.table("photo").await.unwrap();

    // four users: 1, 2, 3, and 4
    // 1 and 2 are friends, 3 is a friend of 1 but not 2
    // 4 isn't friends with anyone
    //
    // four albums: x, y, z, and q; one authored by each user
    // z is public.
    //
    // there's one photo in each album
    //
    // what should each user be able to see?
    //
    //  - 1 should be able to see albums x, y, and z
    //  - 2 should be able to see albums x, y, and z
    //  - 3 should be able to see albums x and z
    //  - 4 should be able to see albums z and q
    friends
        .perform_all(vec![vec![1.into(), 2.into()], vec![3.into(), 1.into()]])
        .await
        .unwrap();
    albums
        .perform_all(vec![
            vec!["x".try_into().unwrap(), 1.into(), 0.into()],
            vec!["y".try_into().unwrap(), 2.into(), 0.into()],
            vec!["z".try_into().unwrap(), 3.into(), 1.into()],
            vec!["q".try_into().unwrap(), 4.into(), 0.into()],
        ])
        .await
        .unwrap();
    photos
        .perform_all(vec![
            vec!["a".try_into().unwrap(), "x".try_into().unwrap()],
            vec!["b".try_into().unwrap(), "y".try_into().unwrap()],
            vec!["c".try_into().unwrap(), "z".try_into().unwrap()],
            vec!["d".try_into().unwrap(), "q".try_into().unwrap()],
        ])
        .await
        .unwrap();

    let mut private = g
        .view("private_photos")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    let mut public = g
        .view("public_photos")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    sleep().await;

    assert_eq!(get!(private, public, 1, "x").len(), 1);
    assert_eq!(get!(private, public, 1, "y").len(), 1);
    assert_eq!(get!(private, public, 1, "z").len(), 1);
    assert_eq!(get!(private, public, 1, "q").len(), 0);

    assert_eq!(get!(private, public, 2, "x").len(), 1);
    assert_eq!(get!(private, public, 2, "y").len(), 1);
    assert_eq!(get!(private, public, 2, "z").len(), 1);
    assert_eq!(get!(private, public, 2, "q").len(), 0);

    assert_eq!(get!(private, public, 3, "x").len(), 1);
    assert_eq!(get!(private, public, 3, "y").len(), 0);
    assert_eq!(get!(private, public, 3, "z").len(), 1);
    assert_eq!(get!(private, public, 3, "q").len(), 0);

    assert_eq!(get!(private, public, 4, "x").len(), 0);
    assert_eq!(get!(private, public, 4, "y").len(), 0);
    assert_eq!(get!(private, public, 4, "z").len(), 1);
    assert_eq!(get!(private, public, 4, "q").len(), 1);

    shutdown_tx.shutdown().await;
}

// FIXME: The test is disabled because UNION views do not deduplicate results as they should.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn union_basic() {
    use itertools::sorted;

    // Add multiples of 2 to 'twos' and multiples of 3 to 'threes'.

    let (mut g, shutdown_tx) = start_simple_unsharded("union_basic").await;
    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE twos (id INTEGER PRIMARY KEY);
         CREATE TABLE threes (id INTEGER PRIMARY KEY);
         CREATE VIEW twos_union_threes AS (SELECT id FROM twos) UNION (SELECT id FROM threes);
         CREATE CACHE `query` FROM SELECT id FROM twos_union_threes;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut twos = g.table("twos").await.unwrap();
    twos.insert_many((0..10).filter(|i: &i32| i % 2 == 0).map(|i| vec![i.into()]))
        .await
        .unwrap();

    let mut threes = g.table("threes").await.unwrap();
    threes
        .insert_many((0..10).filter(|i: &i32| i % 3 == 0).map(|i| vec![i.into()]))
        .await
        .unwrap();

    sleep().await;

    // Check that a UNION query returns deduplicated results. (Each number appearing in either
    // 'twos' or 'threes' appears just once.)
    let mut query = g.view("query").await.unwrap().into_reader_handle().unwrap();
    let result_ids: Vec<i32> = sorted(
        query
            .lookup(&[0.into()], true)
            .await
            .unwrap()
            .into_vec()
            .iter()
            .map(|r| get_col!(query, r, "id", i32)),
    )
    .collect();
    let expected_ids: Vec<i32> = (0..10).filter(|i: &i32| i % 2 == 0 || i % 3 == 0).collect();
    assert_eq!(result_ids, expected_ids);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn union_all_basic() {
    use itertools::sorted;

    // Add multiples of 2 to 'twos' and multiples of 3 to 'threes'.

    let (mut g, shutdown_tx) = start_simple_unsharded("union_all_basic").await;
    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE twos (id INTEGER PRIMARY KEY);
         CREATE TABLE threes (id INTEGER PRIMARY KEY);
         CREATE VIEW twos_union_threes AS (SELECT id FROM twos) UNION ALL (SELECT id FROM threes);
         CREATE CACHE `query` FROM SELECT id FROM twos_union_threes;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut twos = g.table("twos").await.unwrap();
    twos.insert_many((0..10).filter(|i: &i32| i % 2 == 0).map(|i| vec![i.into()]))
        .await
        .unwrap();

    let mut threes = g.table("threes").await.unwrap();
    threes
        .insert_many((0..10).filter(|i: &i32| i % 3 == 0).map(|i| vec![i.into()]))
        .await
        .unwrap();

    sleep().await;

    // Check that a UNION ALL query includes duplicate results. (Every entry from 'twos' and
    // 'threes' appear once.)
    let mut query = g.view("query").await.unwrap().into_reader_handle().unwrap();
    let result_ids: Vec<i32> = sorted(
        query
            .lookup(&[0.into()], true)
            .await
            .unwrap()
            .into_vec()
            .iter()
            .map(|r| get_col!(query, r, "id", i32)),
    )
    .collect();
    let expected_ids: Vec<i32> = sorted(
        (0..10)
            .filter(|i| i % 2 == 0)
            .chain((0..10).filter(|i| i % 3 == 0)),
    )
    .collect();
    assert_eq!(result_ids, expected_ids);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn between() {
    let (mut g, shutdown_tx) = start_simple_unsharded("between_query").await;
    g.extend_recipe(
        ChangeList::from_str("CREATE TABLE things (bigness INT);", Dialect::DEFAULT_MYSQL).unwrap(),
    )
    .await
    .unwrap();

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE CACHE `between` FROM SELECT bigness FROM things WHERE bigness BETWEEN 3 and 5;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut things = g.table("things").await.unwrap();

    for i in 1..10 {
        things.insert(vec![i.into()]).await.unwrap();
    }

    let mut between_query = g
        .view("between")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    sleep().await;

    let expected: Vec<Vec<DfValue>> = (3..6).map(|i| vec![DfValue::from(i)]).collect();
    let res = between_query.lookup(&[0.into()], true).await.unwrap();
    let rows: Vec<Vec<DfValue>> = res.into();
    assert_eq!(rows, expected);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn between_parametrized() {
    let (mut g, shutdown_tx) = start_simple_unsharded("between_parametrized").await;

    g.extend_recipe(
        ChangeList::from_str("CREATE TABLE things (bigness INT);", Dialect::DEFAULT_MYSQL).unwrap(),
    )
    .await
    .unwrap();

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE CACHE q FROM SELECT bigness FROM things WHERE bigness BETWEEN $1 and $2;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut things = g.table("things").await.unwrap();

    for i in 1..10 {
        things.insert(vec![i.into()]).await.unwrap();
    }

    eprintln!("{}", g.graphviz().await.unwrap());

    let mut q = g.view("q").await.unwrap().into_reader_handle().unwrap();
    assert_eq!(q.key_map(), &[(ViewPlaceholder::Between(1, 2), 0)]);

    let expected: Vec<Vec<DfValue>> = (3..6).map(|i| vec![DfValue::from(i)]).collect();
    let rows = q
        .multi_lookup(
            vec![KeyComparison::from_range(
                &(vec1![DfValue::from(3)]..=vec1![DfValue::from(5)]),
            )],
            true,
        )
        .await
        .unwrap()
        .into_vec();
    assert_eq!(rows, expected);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn topk_updates() {
    let (mut g, shutdown_tx) = start_simple_unsharded("things").await;
    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE posts (id INTEGER PRIMARY KEY, number INTEGER);

         CREATE CACHE top_posts FROM
         SELECT * FROM posts ORDER BY number LIMIT 3;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut posts = g.table("posts").await.unwrap();
    let mut top_posts = g
        .view("top_posts")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    posts
        .insert_many((1..10).map(|i| vec![i.into(), i.into()]))
        .await
        .unwrap();

    sleep().await;

    let res = top_posts.lookup(&[0.into()], true).await.unwrap();
    let mut rows: Vec<Vec<DfValue>> = res.into();
    rows.sort();
    assert_eq!(
        rows,
        (1..=3)
            .map(|i| vec![i.into(), i.into()])
            .collect::<Vec<_>>()
    );

    posts.delete(vec![1.into()]).await.unwrap();

    sleep().await;

    let res = top_posts.lookup(&[0.into()], true).await.unwrap();
    let mut rows: Vec<Vec<DfValue>> = res.into();
    rows.sort();
    assert_eq!(
        rows,
        (2..=4)
            .map(|i| vec![i.into(), i.into()])
            .collect::<Vec<_>>()
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn simple_pagination() {
    let (mut g, shutdown_tx) = start_simple_unsharded("simple_pagination").await;
    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE t (x, y);
         CREATE CACHE q FROM SELECT x, y FROM t WHERE y = $1 ORDER BY x ASC LIMIT 3 OFFSET $2;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    eprintln!("{}", g.graphviz().await.unwrap());

    let mut t = g.table("t").await.unwrap();

    t.insert_many(vec![
        vec![DfValue::from(1), DfValue::from("a")],
        vec![DfValue::from(2), DfValue::from("a")],
        vec![DfValue::from(3), DfValue::from("a")],
        vec![DfValue::from(4), DfValue::from("a")],
        vec![DfValue::from(5), DfValue::from("a")],
        vec![DfValue::from(6), DfValue::from("a")],
        vec![DfValue::from(1), DfValue::from("b")],
        vec![DfValue::from(2), DfValue::from("b")],
        vec![DfValue::from(3), DfValue::from("b")],
    ])
    .await
    .unwrap();

    let mut q = g.view("q").await.unwrap().into_reader_handle().unwrap();
    assert_eq!(
        q.key_map(),
        &[
            (
                ViewPlaceholder::OneToOne(1, nom_sql::BinaryOperator::Equal),
                1
            ),
            (
                ViewPlaceholder::PageNumber {
                    offset_placeholder: 2,
                    limit: 3
                },
                2
            )
        ]
    );

    let mut a_page1: Vec<Vec<DfValue>> = q
        .lookup(&["a".into(), 0.into()], true)
        .await
        .unwrap()
        .into();
    a_page1.sort();
    assert_eq!(
        a_page1,
        vec![
            vec![DfValue::from(1), DfValue::from("a")],
            vec![DfValue::from(2), DfValue::from("a")],
            vec![DfValue::from(3), DfValue::from("a")],
        ]
    );

    let mut a_page2: Vec<Vec<DfValue>> = q
        .lookup(&["a".into(), 1.into()], true)
        .await
        .unwrap()
        .into();
    a_page2.sort();
    assert_eq!(
        a_page2,
        vec![
            vec![DfValue::from(4), DfValue::from("a")],
            vec![DfValue::from(5), DfValue::from("a")],
            vec![DfValue::from(6), DfValue::from("a")],
        ]
    );

    let mut b_page1: Vec<Vec<DfValue>> = q
        .lookup(&["b".into(), 0.into()], true)
        .await
        .unwrap()
        .into();
    b_page1.sort();
    assert_eq!(
        b_page1,
        vec![
            vec![DfValue::from(1), DfValue::from("b")],
            vec![DfValue::from(2), DfValue::from("b")],
            vec![DfValue::from(3), DfValue::from("b")],
        ]
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn correct_nested_view_schema() {
    let r_txt = "CREATE TABLE votes (story int, user int);
                 CREATE TABLE stories (id int, content text);
                 CREATE CACHE swvc FROM
                 SELECT stories.id, stories.content, COUNT(votes.user) AS vc \
                     FROM stories \
                     JOIN votes ON (stories.id = votes.story) \
                     WHERE stories.id = ? GROUP BY votes.story;";

    let mut b = Builder::for_tests();
    // need to disable partial due to lack of support for key subsumption (#99)
    b.disable_partial();
    b.set_sharding(None);
    let (mut g, shutdown_tx) = b.start_local().await.unwrap();
    g.extend_recipe(ChangeList::from_str(r_txt, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    let q = g.view("swvc").await.unwrap().into_reader_handle().unwrap();

    let expected_schema = vec![
        ("swvc.id".try_into().unwrap(), DfType::Int),
        ("swvc.content".try_into().unwrap(), DfType::DEFAULT_TEXT),
        ("swvc.vc".try_into().unwrap(), DfType::BigInt),
    ];
    assert_eq!(
        q.schema()
            .unwrap()
            .schema(SchemaType::ProjectedSchema)
            .iter()
            .map(|cs| (cs.column.clone(), cs.column_type.clone()))
            .collect::<Vec<_>>(),
        expected_schema
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn join_column_projection() {
    let (mut g, shutdown_tx) = start_simple_unsharded("join_column_projection").await;

    // NOTE u_id causes panic in stories_authors_explicit; stories_authors_tables_star also paics
    g.extend_recipe(
            ChangeList::from_str("CREATE TABLE stories (s_id int, author_id int, s_name text, content text);
         CREATE TABLE users (u_id int, u_name text, email text);
         CREATE CACHE stories_authors_explicit FROM SELECT s_id, author_id, s_name, content, u_id, u_name, email
             FROM stories
             JOIN users ON (stories.author_id = users.u_id);
         CREATE CACHE stories_authors_tables_star FROM SELECT stories.*, users.*
             FROM stories
             JOIN users ON (stories.author_id = users.u_id);
         CREATE CACHE stories_authors_star FROM SELECT *
             FROM stories
             JOIN users ON (stories.author_id = users.u_id);", Dialect::DEFAULT_MYSQL)
        .unwrap(),
    )
    .await
    .unwrap();

    let query = g
        .view("stories_authors_explicit")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    assert_eq!(
        query.columns(),
        vec![
            "s_id",
            "author_id",
            "s_name",
            "content",
            "u_id",
            "u_name",
            "email",
        ]
    );

    let query = g
        .view("stories_authors_tables_star")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    assert_eq!(
        query.columns(),
        vec![
            "s_id",
            "author_id",
            "s_name",
            "content",
            "u_id",
            "u_name",
            "email",
        ]
    );

    let query = g
        .view("stories_authors_star")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    assert_eq!(
        query.columns(),
        vec![
            "s_id",
            "author_id",
            "s_name",
            "content",
            "u_id",
            "u_name",
            "email",
        ]
    );

    shutdown_tx.shutdown().await;
}

// Tests the case where the source is sharded by a different column than the key column
// with no parameter.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Ignoring sharded tests"]
async fn test_join_across_shards() {
    let (mut g, shutdown_tx) = start_simple("test_join_across_shards").await;
    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE votes (story int, user int);
         CREATE TABLE recs (story int, other int);
         CREATE CACHE all_user_recs FROM SELECT votes.user as u, recs.other as s
             FROM votes \
             JOIN recs ON (votes.story = recs.story);",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut votes = g.table("votes").await.unwrap();
    votes.insert(vec![1i32.into(), 1i32.into()]).await.unwrap();
    votes.insert(vec![2i32.into(), 1i32.into()]).await.unwrap();
    votes.insert(vec![3i32.into(), 1i32.into()]).await.unwrap();
    votes.insert(vec![2i32.into(), 2i32.into()]).await.unwrap();
    votes.insert(vec![3i32.into(), 3i32.into()]).await.unwrap();
    let mut recs = g.table("recs").await.unwrap();
    recs.insert(vec![1i32.into(), 1i32.into()]).await.unwrap();
    recs.insert(vec![2i32.into(), 1i32.into()]).await.unwrap();
    recs.insert(vec![3i32.into(), 1i32.into()]).await.unwrap();
    recs.insert(vec![2i32.into(), 2i32.into()]).await.unwrap();
    recs.insert(vec![3i32.into(), 3i32.into()]).await.unwrap();

    sleep().await;

    // Check 'all_user_recs' results.
    let mut query = g
        .view("all_user_recs")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    let results: Vec<(i32, i32)> = query
        .lookup(&[0i32.into()], true)
        .await
        .unwrap()
        .into_vec()
        .iter()
        .map(|r| (get_col!(query, r, "u", i32), get_col!(query, r, "s", i32)))
        .sorted()
        .collect();
    let expected = vec![
        (1, 1),
        (1, 1),
        (1, 1),
        (1, 2),
        (1, 3),
        (2, 1),
        (2, 2),
        (3, 1),
        (3, 3),
    ];
    assert_eq!(results, expected);

    shutdown_tx.shutdown().await;
}

// Tests the case where the source is sharded by a different column than the key column
// with a parameter.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Ignoring sharded tests"]
async fn test_join_across_shards_with_param() {
    let (mut g, shutdown_tx) = start_simple("test_join_across_shards_with_param").await;
    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE votes (story int, user int);
         CREATE TABLE recs (story int, other int);
         CREATE CACHE user_recs FROM SELECT votes.user as u, recs.other as s
             FROM votes \
             JOIN recs ON (votes.story = recs.story) WHERE votes.user = ?;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut votes = g.table("votes").await.unwrap();
    votes.insert(vec![1i32.into(), 1i32.into()]).await.unwrap();
    votes.insert(vec![2i32.into(), 1i32.into()]).await.unwrap();
    votes.insert(vec![3i32.into(), 1i32.into()]).await.unwrap();
    votes.insert(vec![2i32.into(), 2i32.into()]).await.unwrap();
    votes.insert(vec![3i32.into(), 3i32.into()]).await.unwrap();
    let mut votes = g.table("recs").await.unwrap();
    votes.insert(vec![1i32.into(), 1i32.into()]).await.unwrap();
    votes.insert(vec![2i32.into(), 1i32.into()]).await.unwrap();
    votes.insert(vec![3i32.into(), 1i32.into()]).await.unwrap();
    votes.insert(vec![2i32.into(), 2i32.into()]).await.unwrap();
    votes.insert(vec![3i32.into(), 3i32.into()]).await.unwrap();

    sleep().await;

    // Check 'user_recs' results.
    let mut query = g
        .view("user_recs")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    let results: Vec<(i32, i32)> = query
        .lookup(&[1i32.into()], true)
        .await
        .unwrap()
        .into_vec()
        .iter()
        .map(|r| (get_col!(query, r, "u", i32), get_col!(query, r, "s", i32)))
        .sorted()
        .collect();
    let expected = vec![(1, 1), (1, 1), (1, 1), (1, 2), (1, 3)];
    assert_eq!(results, expected);

    shutdown_tx.shutdown().await;
}

// FIXME: The test is disabled because aliasing the result columns with names reused from other
// columns causes incorrect results to be returned. (See above 'join_param_results' test for
// correct behavior in the no-param case, when column names are not reused.)
#[tokio::test(flavor = "multi_thread")]
async fn test_join_with_reused_column_name() {
    let (mut g, shutdown_tx) = start_simple_unsharded("test_join_with_reused_column_name").await;
    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE votes (story int, user int);
         CREATE TABLE recs (story int, other int);
         CREATE CACHE all_user_recs FROM SELECT votes.user as user, recs.other as story
             FROM votes \
             JOIN recs ON (votes.story = recs.story);",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut votes = g.table("votes").await.unwrap();
    votes.insert(vec![1i32.into(), 1i32.into()]).await.unwrap();
    votes.insert(vec![2i32.into(), 1i32.into()]).await.unwrap();
    votes.insert(vec![3i32.into(), 1i32.into()]).await.unwrap();
    votes.insert(vec![2i32.into(), 2i32.into()]).await.unwrap();
    votes.insert(vec![3i32.into(), 3i32.into()]).await.unwrap();
    let mut recs = g.table("recs").await.unwrap();
    recs.insert(vec![1i32.into(), 1i32.into()]).await.unwrap();
    recs.insert(vec![2i32.into(), 1i32.into()]).await.unwrap();
    recs.insert(vec![3i32.into(), 1i32.into()]).await.unwrap();
    recs.insert(vec![2i32.into(), 2i32.into()]).await.unwrap();
    recs.insert(vec![3i32.into(), 3i32.into()]).await.unwrap();

    // Check 'all_user_recs' results.
    let mut query = g
        .view("all_user_recs")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    let results: Vec<(i32, i32)> = query
        .lookup(&[0i32.into()], true)
        .await
        .unwrap()
        .into_vec()
        .iter()
        .map(|r| {
            (
                get_col!(query, r, "user", i32),
                get_col!(query, r, "story", i32),
            )
        })
        .sorted()
        .collect();
    let expected = vec![
        (1, 1),
        (1, 1),
        (1, 1),
        (1, 2),
        (1, 3),
        (2, 1),
        (2, 2),
        (3, 1),
        (3, 3),
    ];
    assert_eq!(results, expected);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_join_with_reused_column_name_with_param() {
    let (mut g, shutdown_tx) = start_simple_unsharded("test_join_with_reused_column_name").await;
    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE votes (story int, user int);
         CREATE TABLE recs (story int, other int);
         CREATE CACHE user_recs FROM SELECT votes.user as user, recs.other as story
             FROM votes \
             JOIN recs ON (votes.story = recs.story) WHERE votes.user = ?;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut votes = g.table("votes").await.unwrap();
    votes.insert(vec![1i32.into(), 1i32.into()]).await.unwrap();
    votes.insert(vec![2i32.into(), 1i32.into()]).await.unwrap();
    votes.insert(vec![3i32.into(), 1i32.into()]).await.unwrap();
    votes.insert(vec![2i32.into(), 2i32.into()]).await.unwrap();
    votes.insert(vec![3i32.into(), 3i32.into()]).await.unwrap();
    let mut recs = g.table("recs").await.unwrap();
    recs.insert(vec![1i32.into(), 1i32.into()]).await.unwrap();
    recs.insert(vec![2i32.into(), 1i32.into()]).await.unwrap();
    recs.insert(vec![3i32.into(), 1i32.into()]).await.unwrap();
    recs.insert(vec![2i32.into(), 2i32.into()]).await.unwrap();
    recs.insert(vec![3i32.into(), 3i32.into()]).await.unwrap();

    // Check 'user_recs' results.
    let mut query = g
        .view("user_recs")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    let results: Vec<(i32, i32)> = query
        .lookup(&[1i32.into()], true)
        .await
        .unwrap()
        .into_vec()
        .iter()
        .map(|r| {
            (
                get_col!(query, r, "user", i32),
                get_col!(query, r, "story", i32),
            )
        })
        .sorted()
        .collect();
    let expected = vec![(1, 1), (1, 1), (1, 1), (1, 2), (1, 3)];
    assert_eq!(results, expected);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore] // ENG-411
async fn self_join_basic() {
    let (mut g, shutdown_tx) = start_simple_unsharded("self_join_basic").await;
    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE votes (story int, user int);
         CREATE VIEW like_minded AS SELECT v1.user, v2.user AS agreer \
             FROM votes v1 \
             JOIN votes v2 ON (v1.story = v2.story);
         CREATE CACHE follow_on FROM SELECT user, agreer FROM like_minded;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut votes = g.table("votes").await.unwrap();
    votes.insert(vec![1i32.into(), 1i32.into()]).await.unwrap();
    votes.insert(vec![2i32.into(), 1i32.into()]).await.unwrap();
    votes.insert(vec![3i32.into(), 1i32.into()]).await.unwrap();
    votes.insert(vec![2i32.into(), 2i32.into()]).await.unwrap();
    votes.insert(vec![3i32.into(), 3i32.into()]).await.unwrap();

    let expected = vec![
        (1, 1),
        (1, 1),
        (1, 1),
        (1, 2),
        (1, 3),
        (2, 1),
        (2, 2),
        (3, 1),
        (3, 3),
    ];

    // Check follow_on

    let mut query = g
        .view("follow_on")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    assert_eq!(query.columns(), vec!["user", "agreer"]);
    let results: Vec<(i32, i32)> = query
        .lookup(&[0i32.into()], true)
        .await
        .unwrap()
        .into_vec()
        .iter()
        .map(|r| {
            (
                get_col!(query, r, "user", i32),
                get_col!(query, r, "agreer", i32),
            )
        })
        .sorted()
        .collect();
    assert_eq!(results, expected);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn self_join_param() {
    let (mut g, shutdown_tx) = start_simple_unsharded("self_join_param").await;
    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE users (id int, friend int);
         CREATE CACHE fof FROM SELECT u1.id AS user, u2.friend AS fof \
             FROM users u1 \
             JOIN users u2 ON (u1.friend = u2.id) WHERE u1.id = ?;
         CREATE VIEW fof2 AS SELECT u1.id AS user, u2.friend AS fof \
             FROM users u1 \
             JOIN users u2 ON (u1.friend = u2.id);
         CREATE CACHE follow_on FROM SELECT * FROM fof2 WHERE user = ?;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut votes = g.table("users").await.unwrap();
    votes.insert(vec![1i32.into(), 2i32.into()]).await.unwrap();
    votes.insert(vec![1i32.into(), 3i32.into()]).await.unwrap();
    votes.insert(vec![2i32.into(), 5i32.into()]).await.unwrap();
    votes.insert(vec![3i32.into(), 1i32.into()]).await.unwrap();
    votes.insert(vec![5i32.into(), 6i32.into()]).await.unwrap();
    votes.insert(vec![6i32.into(), 2i32.into()]).await.unwrap();

    // Check fof

    let mut query = g.view("fof").await.unwrap().into_reader_handle().unwrap();
    assert_eq!(query.columns(), vec!["user", "fof"]);
    let results: Vec<(i32, i32)> = query
        .lookup(&[1i32.into()], true)
        .await
        .unwrap()
        .into_vec()
        .iter()
        .map(|r| {
            (
                get_col!(query, r, "user", i32),
                get_col!(query, r, "fof", i32),
            )
        })
        .sorted()
        .collect();
    let expected = vec![(1, 1), (1, 5)];
    assert_eq!(results, expected);

    // Check follow_on

    let mut query = g
        .view("follow_on")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    // Disabled because a "bogokey" column is present as well.
    // assert_eq!(query.columns(), vec!["user", "fof"]);

    let results: Vec<(i32, i32)> = query
        .lookup(&[1i32.into()], true)
        .await
        .unwrap()
        .into_vec()
        .iter()
        .map(|r| {
            (
                get_col!(query, r, "user", i32),
                get_col!(query, r, "fof", i32),
            )
        })
        .sorted()
        .collect();
    assert_eq!(results, expected);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn non_sql_materialized_range_query() {
    let (mut g, shutdown_tx) = {
        let mut builder = Builder::for_tests();
        builder.disable_partial();
        builder.set_sharding(None);
        builder.set_persistence(get_persistence_params("non_sql_materialized_range_query"));
        builder.start_local()
    }
    .await
    .unwrap();

    let a = g
        .migrate(|mig| {
            let a = mig.add_base(
                "a",
                make_columns(&["a", "b"]),
                Base::new().with_primary_key([0]),
            );
            mig.maintain_anonymous(a, &Index::btree_map(vec![0]));
            a
        })
        .await;

    let mut a = g.table_by_index(a).await.unwrap();
    let mut reader = g.view("a").await.unwrap().into_reader_handle().unwrap();
    a.insert_many((0i32..10).map(|n| vec![DfValue::from(n), DfValue::from(n)]))
        .await
        .unwrap();

    sleep().await;

    let res = reader
        .multi_lookup(
            vec![(vec1![DfValue::from(2)]..vec1![DfValue::from(5)]).into()],
            false,
        )
        .await
        .unwrap()
        .into_vec();

    assert_eq!(
        res,
        (2..5).map(|n| vec![n.into(), n.into()]).collect::<Vec<_>>()
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn non_sql_range_upquery() {
    let (mut g, shutdown_tx) = {
        let mut builder = Builder::for_tests();
        builder.set_sharding(None);
        builder.set_persistence(get_persistence_params("non_sql_range_upquery"));
        builder.start_local()
    }
    .await
    .unwrap();

    let a = g
        .migrate(|mig| {
            let a = mig.add_base(
                "a",
                make_columns(&["a", "b"]),
                Base::new().with_primary_key([0]),
            );
            mig.maintain_anonymous(a, &Index::btree_map(vec![0]));
            a
        })
        .await;

    let mut a = g.table_by_index(a).await.unwrap();
    let mut reader = g.view("a").await.unwrap().into_reader_handle().unwrap();
    a.insert_many((0i32..10).map(|n| vec![DfValue::from(n), DfValue::from(n)]))
        .await
        .unwrap();

    sleep().await;

    let res = reader
        .multi_lookup(
            vec![(vec1![DfValue::from(2)]..vec1![DfValue::from(5)]).into()],
            true,
        )
        .await
        .unwrap()
        .into_vec();

    assert_eq!(
        res,
        (2..5).map(|n| vec![n.into(), n.into()]).collect::<Vec<_>>()
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn range_upquery_after_point_queries() {
    let (mut g, shutdown_tx) = {
        let mut builder = Builder::for_tests();
        builder.set_sharding(None);
        builder.set_persistence(get_persistence_params("non_sql_range_upquery"));
        builder.start_local()
    }
    .await
    .unwrap();

    let (a, b) = g
        .migrate(|mig| {
            let a = mig.add_base(
                "a",
                make_columns(&["a", "b"]),
                Base::new().with_primary_key([0]),
            );
            let b = mig.add_base(
                "b",
                make_columns(&["a", "c"]),
                Base::new().with_primary_key([0]),
            );
            let join = mig.add_ingredient(
                "join",
                make_columns(&["a", "a_b", "b_c"]),
                Join::new(
                    a,
                    b,
                    JoinType::Inner,
                    vec![(0, 0)],
                    vec![(Side::Left, 0), (Side::Left, 1), (Side::Right, 1)],
                ),
            );

            mig.maintain(
                "btree_reader".into(),
                join,
                &Index::btree_map(vec![0]),
                Default::default(),
                Default::default(),
            );

            // each node can only have one reader, so add an identity node above the join for the
            // hash reader

            let hash_id = mig.add_ingredient(
                "hash_id",
                make_columns(&["a", "a_b", "a_c"]),
                Identity::new(join),
            );

            mig.maintain(
                "hash_reader".into(),
                hash_id,
                &Index::hash_map(vec![0]),
                Default::default(),
                Default::default(),
            );

            (a, b)
        })
        .await;

    let mut a = g.table_by_index(a).await.unwrap();
    let mut b = g.table_by_index(b).await.unwrap();
    let mut btree_reader = g
        .view("btree_reader")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    let mut hash_reader = g
        .view("hash_reader")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    a.insert_many((0i32..10).map(|n| vec![DfValue::from(n), DfValue::from(n)]))
        .await
        .unwrap();
    b.insert_many((0i32..10).map(|n| vec![DfValue::from(n), DfValue::from(n * 10)]))
        .await
        .unwrap();

    sleep().await;

    // Do some point queries so we get keys covered by our range
    assert_eq!(
        &*hash_reader
            .lookup(&[3.into()], true)
            .await
            .unwrap()
            .into_vec(),
        &[vec![DfValue::from(3), DfValue::from(3), DfValue::from(30)]]
    );
    assert_eq!(
        &*hash_reader
            .lookup(&[3.into()], true)
            .await
            .unwrap()
            .into_vec(),
        &[vec![DfValue::from(3), DfValue::from(3), DfValue::from(30)]]
    );

    let res = btree_reader
        .multi_lookup(
            vec![(vec1![DfValue::from(2)]..vec1![DfValue::from(5)]).into()],
            true,
        )
        .await
        .unwrap()
        .into_vec();

    assert_eq!(
        res,
        (2..5)
            .map(|n| vec![n.into(), n.into(), (n * 10).into()])
            .collect::<Vec<_>>()
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn query_reuse_aliases() {
    let (mut g, shutdown_tx) = start_simple_unsharded("query_reuse_aliases").await;
    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE t1 (a INT, b INT);
         CREATE CACHE q1 FROM SELECT * FROM t1 WHERE a != 1;
         CREATE CACHE q2 FROM SELECT * FROM t1 WHERE a != 1;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    g.view("q1").await.unwrap();
    g.view("q2").await.unwrap();

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE CACHE q3 FROM SELECT * FROM t1 WHERE a != 1",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    g.view("q1").await.unwrap();
    g.view("q2").await.unwrap();
    g.view("q3").await.unwrap();

    // query rewriting means this ends up being identical to the above query, even though the source
    // is different - let's make sure that still aliases successfully.
    g.extend_recipe(
        ChangeList::from_str(
            "CREATE CACHE q4 FROM SELECT * FROM t1 WHERE NOT (a = 1)",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    g.view("q1").await.unwrap();
    g.view("q2").await.unwrap();
    g.view("q3").await.unwrap();
    g.view("q4").await.unwrap();

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn same_table_columns_inequal() {
    let (mut g, shutdown_tx) = start_simple_unsharded("same_table_columns_inequal").await;
    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE t1 (a INT, b INT);
         CREATE CACHE q FROM SELECT * FROM t1 WHERE t1.a != t1.b;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut t1 = g.table("t1").await.unwrap();
    t1.insert_many(vec![
        vec![DfValue::from(1i32), DfValue::from(1i32)],
        vec![DfValue::from(2i32), DfValue::from(2i32)],
        vec![DfValue::from(1i32), DfValue::from(2i32)],
        vec![DfValue::from(2i32), DfValue::from(3i32)],
    ])
    .await
    .unwrap();

    sleep().await;

    let mut q = g.view("q").await.unwrap().into_reader_handle().unwrap();
    let res = q.lookup(&[0i32.into()], true).await.unwrap().into_vec();
    assert_eq!(
        res,
        vec![
            vec![DfValue::from(1i32), DfValue::from(2i32)],
            vec![DfValue::from(2i32), DfValue::from(3i32)],
        ]
    );

    shutdown_tx.shutdown().await;
}

// FIXME: The test is disabled due to panic when querying an aliased view.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn view_reuse_aliases() {
    let (mut g, shutdown_tx) = start_simple_unsharded("view_reuse_aliases").await;

    // NOTE q1 causes panic
    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE t1 (a INT, b INT);
         CREATE VIEW v1 AS SELECT * FROM t1 WHERE a != 1;
         CREATE VIEW v2 AS SELECT * FROM t1 WHERE a != 1;
         CREATE CACHE q1 FROM SELECT * FROM v1;
         CREATE CACHE q2 FROM SELECT * FROM v2;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    g.view("v1").await.unwrap();
    g.view("v2").await.unwrap();
    g.view("q1").await.unwrap();
    g.view("q2").await.unwrap();

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn post_read_ilike() {
    readyset_tracing::init_test_logging();

    let (mut g, shutdown_tx) = {
        let mut builder = Builder::for_tests();
        builder.disable_partial();
        builder.set_sharding(None);
        builder.set_persistence(get_persistence_params("post_read_ilike"));
        builder.start_local()
    }
    .await
    .unwrap();

    let a = g
        .migrate(|mig| {
            let a = mig.add_base(
                "a",
                make_columns(&["a", "b"]),
                Base::new().with_primary_key([0]),
            );
            mig.maintain_anonymous_with_reader_processing(
                a,
                &Index::btree_map(vec![0]),
                ReaderProcessing::new(
                    Some(vec![(1, OrderType::OrderAscending)]),
                    None,
                    None,
                    None,
                    None,
                )
                .unwrap(),
            );
            a
        })
        .await;

    let mut a = g.table_by_index(a).await.unwrap();
    let mut reader = g.view("a").await.unwrap().into_reader_handle().unwrap();
    a.insert_many(vec![
        vec![DfValue::from("foo"), DfValue::from(1i32)],
        vec![DfValue::from("bar"), DfValue::from(2i32)],
        vec![DfValue::from("baz"), DfValue::from(3i32)],
        vec![DfValue::from("BAZ"), DfValue::from(4i32)],
        vec![
            DfValue::from("something else entirely"),
            DfValue::from(5i32),
        ],
    ])
    .await
    .unwrap();

    sleep().await;

    let res = reader
        .raw_lookup(ViewQuery {
            key_comparisons: vec![KeyComparison::from_range(&(..))],
            block: true,
            filter: Some(DfExpr::Op {
                left: Box::new(DfExpr::Column {
                    index: 0,
                    ty: DfType::DEFAULT_TEXT,
                }),
                op: BinaryOperator::ILike,
                right: Box::new(DfExpr::Literal {
                    val: "%a%".into(),
                    ty: DfType::DEFAULT_TEXT,
                }),
                ty: DfType::Bool,
            }),
            timestamp: None,
            limit: None,
            offset: None,
        })
        .await
        .unwrap()
        .into_vec();

    assert_eq!(
        res,
        vec![
            vec![DfValue::from("bar"), DfValue::from(2)],
            vec![DfValue::from("baz"), DfValue::from(3)],
            vec![DfValue::from("BAZ"), DfValue::from(4)],
        ]
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cast_projection() {
    let (mut g, shutdown_tx) = start_simple_unsharded("cast").await;

    g.extend_recipe(
            ChangeList::from_str("CREATE TABLE users (id int, created_at timestamp);
         CREATE CACHE user FROM SELECT id, CAST(created_at AS date) AS created_day FROM users WHERE id = ?;", Dialect::DEFAULT_MYSQL)
        .unwrap(),
    )
    .await
    .unwrap();

    let mut table = g.table("users").await.unwrap();
    table
        .insert(vec![
            1i32.into(),
            NaiveDate::from_ymd(2020, 3, 16).and_hms(16, 40, 30).into(),
        ])
        .await
        .unwrap();

    sleep().await;

    let mut view = g.view("user").await.unwrap().into_reader_handle().unwrap();

    let result = view
        .lookup(&[1i32.into()], true)
        .await
        .unwrap()
        .into_vec()
        .remove(0);

    assert_eq!(
        result,
        vec![DfValue::from(1), NaiveDate::from_ymd(2020, 3, 16).into()]
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn aggregate_expression() {
    let (mut g, shutdown_tx) = start_simple_unsharded("aggregate_expression").await;

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE t (string_num text);
         CREATE CACHE q FROM SELECT max(cast(t.string_num as int)) as max_num from t;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut t = g.table("t").await.unwrap();
    let mut q = g.view("q").await.unwrap().into_reader_handle().unwrap();

    t.insert_many(vec![vec![DfValue::from("100")], vec![DfValue::from("5")]])
        .await
        .unwrap();

    sleep().await;

    let res = &q
        .lookup(&[0i32.into()], true)
        .await
        .unwrap()
        .into_vec()
        .remove(0);

    assert_eq!(get_col!(q, res, "max_num"), &DfValue::from(100));

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn aggregate_missing_columns() {
    let (mut g, shutdown_tx) = start_simple_unsharded("aggregate_missing_columns").await;

    g.extend_recipe(
        ChangeList::from_str("CREATE TABLE t (id INT);", Dialect::DEFAULT_MYSQL).unwrap(),
    )
    .await
    .unwrap();

    let res = g
        .extend_recipe(
            ChangeList::from_str(
                "CREATE CACHE q FROM SELECT max(idd) FROM t",
                Dialect::DEFAULT_MYSQL,
            )
            .unwrap(),
        )
        .await;
    assert!(res.is_err());
    assert!(res.err().unwrap().to_string().contains("idd"));

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn post_join_filter() {
    let (mut g, shutdown_tx) = start_simple_unsharded("post_join_filter").await;

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE t1 (id int, val_1 int);
         CREATE TABLE t2 (id int, val_2 int);
         CREATE CACHE q FROM
            SELECT t1.id AS id_1, t1.val_1 AS val_1, t2.val_2 AS val_2
            FROM t1
            JOIN t2 ON t1.id = t2.id
            WHERE t1.val_1 >= t2.val_2",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut t1 = g.table("t1").await.unwrap();
    let mut t2 = g.table("t2").await.unwrap();
    let mut q = g.view("q").await.unwrap().into_reader_handle().unwrap();

    t1.insert_many(vec![
        vec![DfValue::from(1), DfValue::from(1)],
        vec![DfValue::from(2), DfValue::from(2)],
        vec![DfValue::from(3), DfValue::from(3)],
        vec![DfValue::from(4), DfValue::from(4)],
    ])
    .await
    .unwrap();

    t2.insert_many(vec![
        vec![DfValue::from(1), DfValue::from(1)],
        vec![DfValue::from(2), DfValue::from(1)],
        vec![DfValue::from(2), DfValue::from(5)],
        vec![DfValue::from(3), DfValue::from(5)],
    ])
    .await
    .unwrap();

    sleep().await;

    let mut res: Vec<_> = q.lookup(&[0.into()], true).await.unwrap().into();
    res.sort();

    assert_eq!(
        res,
        vec![
            vec![1.into(), 1.into(), 1.into()],
            vec![2.into(), 2.into(), 1.into()],
        ]
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
/// Tests the case where two tables have the same column name and those columns are
/// used in a post-join filter.
async fn duplicate_column_names() {
    let (mut g, shutdown_tx) = start_simple_unsharded("duplicate_column_names").await;

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE t1 (id int, val int);
         CREATE TABLE t2 (id int, val int);
         CREATE CACHE q FROM
            SELECT t1.id AS id_1, t1.val AS val_1, t2.val AS val_2
            FROM t1
            JOIN t2 ON t1.id = t2.id
            WHERE t1.val >= t2.val",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut t1 = g.table("t1").await.unwrap();
    let mut t2 = g.table("t2").await.unwrap();
    let mut q = g.view("q").await.unwrap().into_reader_handle().unwrap();

    t1.insert_many(vec![
        vec![DfValue::from(1), DfValue::from(1)],
        vec![DfValue::from(2), DfValue::from(2)],
        vec![DfValue::from(3), DfValue::from(3)],
        vec![DfValue::from(4), DfValue::from(4)],
    ])
    .await
    .unwrap();

    t2.insert_many(vec![
        vec![DfValue::from(1), DfValue::from(1)],
        vec![DfValue::from(2), DfValue::from(1)],
        vec![DfValue::from(2), DfValue::from(5)],
        vec![DfValue::from(3), DfValue::from(5)],
    ])
    .await
    .unwrap();

    sleep().await;

    let mut res: Vec<_> = q.lookup(&[0.into()], true).await.unwrap().into();
    res.sort();

    assert_eq!(
        res,
        vec![
            vec![1.into(), 1.into(), 1.into()],
            vec![2.into(), 2.into(), 1.into()]
        ]
    );

    shutdown_tx.shutdown().await;
}
#[tokio::test(flavor = "multi_thread")]
async fn filter_on_expression() {
    let (mut g, shutdown_tx) = start_simple_unsharded("filter_on_expression").await;

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE users (id int, birthday date);
         CREATE CACHE friday_babies FROM SELECT id FROM users WHERE dayofweek(birthday) = 6;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut t = g.table("users").await.unwrap();
    let mut q = g
        .view("friday_babies")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    t.insert_many(vec![
        vec![
            DfValue::from(1),
            DfValue::from(NaiveDate::from_ymd(1995, 6, 2)),
        ],
        vec![
            DfValue::from(2),
            DfValue::from(NaiveDate::from_ymd(2015, 5, 15)),
        ],
    ])
    .await
    .unwrap();

    sleep().await;

    let res = &q.lookup(&[0i32.into()], true).await.unwrap().into_vec();

    assert_eq!(get_col!(q, res[0], "id"), &DfValue::from(1));

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn compound_join_key() {
    let (mut g, shutdown_tx) = start_simple_unsharded("compound_join_key").await;
    g.extend_recipe(
        ChangeList::from_str(
            "
      CREATE TABLE t1 (id_1 int, id_2 int, val_1 int);
      CREATE TABLE t2 (id_1 int, id_2 int, val_2 int);
      CREATE CACHE q FROM
        SELECT t1.val_1, t2.val_2
        FROM t1
        JOIN t2
          ON t1.id_1 = t2.id_1 AND t1.id_2 = t2.id_2;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut t1 = g.table("t1").await.unwrap();
    let mut t2 = g.table("t2").await.unwrap();
    let mut q = g.view("q").await.unwrap().into_reader_handle().unwrap();

    t1.insert_many(vec![
        vec![
            DfValue::from(1i32),
            DfValue::from(2i32),
            DfValue::from(3i32),
        ],
        vec![
            DfValue::from(1i32),
            DfValue::from(3i32),
            DfValue::from(4i32),
        ],
        vec![
            DfValue::from(2i32),
            DfValue::from(3i32),
            DfValue::from(4i32),
        ],
        vec![
            DfValue::from(2i32),
            DfValue::from(4i32),
            DfValue::from(5i32),
        ],
    ])
    .await
    .unwrap();

    t2.insert_many(vec![
        vec![
            DfValue::from(1i32),
            DfValue::from(2i32),
            DfValue::from(33i32),
        ],
        vec![
            DfValue::from(1i32),
            DfValue::from(3i32),
            DfValue::from(44i32),
        ],
        vec![
            DfValue::from(1i32),
            DfValue::from(4i32),
            DfValue::from(123i32),
        ],
        vec![
            DfValue::from(2i32),
            DfValue::from(3i32),
            DfValue::from(44i32),
        ],
        vec![
            DfValue::from(2i32),
            DfValue::from(5i32),
            DfValue::from(123i32),
        ],
    ])
    .await
    .unwrap();

    sleep().await;

    let res = q
        .lookup(&[0i32.into()], true)
        .await
        .unwrap()
        .into_iter()
        .map(|r| (get_col!(q, r, "val_1", i32), get_col!(q, r, "val_2", i32)))
        .sorted()
        .collect::<Vec<(i32, i32)>>();

    assert_eq!(res, vec![(3, 33), (4, 44), (4, 44)]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn left_join_null() {
    let (mut g, shutdown_tx) = start_simple_unsharded("left_join_null").await;

    g.extend_recipe(
            ChangeList::from_str("CREATE TABLE jim (id int, a int);
         CREATE TABLE bob (id int);
         CREATE CACHE funky FROM SELECT * FROM jim LEFT JOIN bob ON jim.id = bob.id WHERE bob.id IS NULL;", Dialect::DEFAULT_MYSQL)
        .unwrap(),
    )
    .await
    .unwrap();

    let mut t = g.table("jim").await.unwrap();
    let mut t2 = g.table("bob").await.unwrap();
    let mut q = g.view("funky").await.unwrap().into_reader_handle().unwrap();

    t.insert_many(vec![
        vec![DfValue::from(1), DfValue::from(2)],
        vec![DfValue::from(3), DfValue::from(6)],
        vec![DfValue::from(4), DfValue::from(6)],
    ])
    .await
    .unwrap();
    t2.insert_many(vec![vec![DfValue::from(3)]]).await.unwrap();

    sleep().await;

    let num_res = q
        .lookup(&[0.into()], true)
        .await
        .unwrap()
        .into_iter()
        .count();
    assert_eq!(num_res, 2);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn overlapping_indices() {
    let (mut g, shutdown_tx) = start_simple_unsharded("overlapping_indices").await;

    // this creates an aggregation operator indexing on [0, 1], and then a TopK child on [1]
    g.extend_recipe(ChangeList::from_str("CREATE TABLE test (id int, a int, b int);
          CREATE CACHE overlapping FROM SELECT SUM(a) as s, id FROM test WHERE b = ? GROUP BY id ORDER BY id LIMIT 2;", Dialect::DEFAULT_MYSQL).unwrap())
    .await
    .unwrap();

    let mut t = g.table("test").await.unwrap();
    let mut q = g
        .view("overlapping")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    t.insert_many(vec![
        vec![DfValue::from(1), DfValue::from(1), DfValue::from(1)],
        vec![DfValue::from(2), DfValue::from(2), DfValue::from(1)],
        vec![DfValue::from(3), DfValue::from(3), DfValue::from(1)],
        vec![DfValue::from(4), DfValue::from(4), DfValue::from(2)],
        vec![DfValue::from(5), DfValue::from(5), DfValue::from(2)],
        vec![DfValue::from(6), DfValue::from(6), DfValue::from(3)],
        vec![DfValue::from(6), DfValue::from(14), DfValue::from(3)],
        vec![DfValue::from(7), DfValue::from(7), DfValue::from(3)],
        vec![DfValue::from(8), DfValue::from(8), DfValue::from(3)],
    ])
    .await
    .unwrap();

    sleep().await;

    let rows = q.lookup(&[3i32.into()], true).await.unwrap();

    let res = rows
        .into_iter()
        .map(|r| {
            (
                get_col!(q, r, "s", Decimal).to_i32().unwrap(),
                get_col!(q, r, "id", i32),
            )
        })
        .sorted()
        .collect::<Vec<(i32, i32)>>();

    assert_eq!(res, vec![(7, 7), (20, 6)]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn aggregate_after_filter_non_equality() {
    let (mut g, shutdown_tx) = start_simple_unsharded("aggregate_after_filter_non_equality").await;

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE test (number int, value int);
         CREATE CACHE filteragg FROM SELECT sum(value) AS s FROM test WHERE number > 2;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut t = g.table("test").await.unwrap();
    let mut q = g
        .view("filteragg")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    t.insert_many(vec![
        vec![DfValue::from(1i32), DfValue::from(1i32)],
        vec![DfValue::from(2i32), DfValue::from(4i32)],
        vec![DfValue::from(3i32), DfValue::from(5i32)],
        vec![DfValue::from(4i32), DfValue::from(7i32)],
        vec![DfValue::from(5i32), DfValue::from(1i32)],
    ])
    .await
    .unwrap();

    sleep().await;

    let res = q
        .lookup(&[0i32.into()], true)
        .await
        .unwrap()
        .into_vec()
        .iter()
        .map(|r| get_col!(q, r, "s", Decimal).to_i32().unwrap())
        .collect::<Vec<i32>>();

    assert_eq!(res, vec![13]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn join_simple_cte() {
    let (mut g, shutdown_tx) = start_simple_unsharded("join_simple_cte").await;

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE t1 (id int, value int);
         CREATE TABLE t2 (value int, name text);
         CREATE CACHE join_simple_cte FROM
         WITH max_val AS (SELECT max(value) as value FROM t1)
         SELECT name FROM t2 JOIN max_val ON max_val.value = t2.value;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut t1 = g.table("t1").await.unwrap();
    let mut t2 = g.table("t2").await.unwrap();
    let mut view = g
        .view("join_simple_cte")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    t1.insert_many(vec![
        vec![DfValue::from(1i32), DfValue::from(2i32)],
        vec![DfValue::from(1i32), DfValue::from(4i32)],
    ])
    .await
    .unwrap();

    t2.insert_many(vec![
        vec![DfValue::from(2i32), DfValue::from("two")],
        vec![DfValue::from(4i32), DfValue::from("four")],
    ])
    .await
    .unwrap();

    sleep().await;

    let res = view.lookup(&[0i32.into()], true).await.unwrap().into_vec();
    assert_eq!(get_col!(view, res[0], "name"), &DfValue::from("four"));

    shutdown_tx.shutdown().await;
}

// multiple_aggregate_sum tests multiple aggregators of the same type, in this case sum(),
// operating over different columns from the same table.
#[tokio::test(flavor = "multi_thread")]
async fn multiple_aggregate_sum() {
    let (mut g, shutdown_tx) = start_simple_unsharded("multiple_aggregate").await;

    g.extend_recipe(

            ChangeList::from_str("CREATE TABLE test (number int, value1 int, value2 int);
         CREATE CACHE multiagg FROM SELECT sum(value1) AS s1, sum(value2) as s2 FROM test GROUP BY number;", Dialect::DEFAULT_MYSQL)
        .unwrap(),
    )
    .await
    .unwrap();

    let mut t = g.table("test").await.unwrap();
    let mut q = g
        .view("multiagg")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    t.insert_many(vec![
        vec![
            DfValue::from(1i32),
            DfValue::from(1i32),
            DfValue::from(5i32),
        ],
        vec![
            DfValue::from(1i32),
            DfValue::from(4i32),
            DfValue::from(2i32),
        ],
        vec![
            DfValue::from(2i32),
            DfValue::from(5i32),
            DfValue::from(7i32),
        ],
        vec![
            DfValue::from(2i32),
            DfValue::from(7i32),
            DfValue::from(1i32),
        ],
        vec![
            DfValue::from(3i32),
            DfValue::from(1i32),
            DfValue::from(3i32),
        ],
    ])
    .await
    .unwrap();

    sleep().await;

    let rows = q.lookup(&[0i32.into()], true).await.unwrap();

    let res = rows
        .into_iter()
        .map(|r| {
            (
                get_col!(q, r, "s1", Decimal).to_i32().unwrap(),
                get_col!(q, r, "s2", Decimal).to_i32().unwrap(),
            )
        })
        .sorted()
        .collect::<Vec<(i32, i32)>>();

    assert_eq!(res, vec![(1, 3), (5, 7), (12, 8)]);

    shutdown_tx.shutdown().await;
}

// multiple_aggregate_same_col tests multiple aggregators of different types operating on the same
// column.
#[tokio::test(flavor = "multi_thread")]
async fn multiple_aggregate_same_col() {
    let (mut g, shutdown_tx) = start_simple_unsharded("multiple_aggregate_same_col").await;

    g.extend_recipe(ChangeList::from_str(

            "CREATE TABLE test (number int, value int);
         CREATE CACHE multiaggsamecol FROM SELECT sum(value) AS s, avg(value) AS a FROM test GROUP BY number;", Dialect::DEFAULT_MYSQL)
        .unwrap(),
    )
    .await
    .unwrap();

    let mut t = g.table("test").await.unwrap();
    let mut q = g
        .view("multiaggsamecol")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    t.insert_many(vec![
        vec![DfValue::from(1i32), DfValue::from(1i32)],
        vec![DfValue::from(1i32), DfValue::from(4i32)],
        vec![DfValue::from(2i32), DfValue::from(5i32)],
        vec![DfValue::from(2i32), DfValue::from(7i32)],
        vec![DfValue::from(3i32), DfValue::from(1i32)],
    ])
    .await
    .unwrap();

    sleep().await;

    let rows = q.lookup(&[0i32.into()], true).await.unwrap();

    let res = rows
        .into_iter()
        .map(|r| {
            (
                get_col!(q, r, "s", Decimal).to_i32().unwrap(),
                get_col!(q, r, "a", f64),
            )
        })
        .sorted_by(|a, b| Ord::cmp(&a.0, &b.0))
        .collect::<Vec<(i32, f64)>>();

    assert_eq!(res, vec![(1, 1.), (5, 2.5), (12, 6.0)]);

    shutdown_tx.shutdown().await;
}

// multiple_aggregate_sum_sharded tests multiple aggregators of the same type, in this case sum(),
// operating over different columns from the same table in a sharded environment.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Ignoring sharded tests"]
async fn multiple_aggregate_sum_sharded() {
    let (mut g, shutdown_tx) = start_simple("multiple_aggregate_sharded").await;

    g.extend_recipe(ChangeList::from_str("CREATE TABLE test (number int, value1 int, value2 int);
         CREATE CACHE multiaggsharded FROM SELECT sum(value1) AS s1, sum(value2) as s2 FROM test GROUP BY number;", Dialect::DEFAULT_MYSQL).unwrap())
    .await
    .unwrap();

    let mut t = g.table("test").await.unwrap();
    let mut q = g
        .view("multiaggsharded")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    t.insert_many(vec![
        vec![
            DfValue::from(1i32),
            DfValue::from(1i32),
            DfValue::from(5i32),
        ],
        vec![
            DfValue::from(1i32),
            DfValue::from(4i32),
            DfValue::from(2i32),
        ],
        vec![
            DfValue::from(2i32),
            DfValue::from(5i32),
            DfValue::from(7i32),
        ],
        vec![
            DfValue::from(2i32),
            DfValue::from(7i32),
            DfValue::from(1i32),
        ],
        vec![
            DfValue::from(3i32),
            DfValue::from(1i32),
            DfValue::from(3i32),
        ],
    ])
    .await
    .unwrap();

    sleep().await;

    let rows = q.lookup(&[0i32.into()], true).await.unwrap();

    let res = rows
        .into_iter()
        .map(|r| {
            (
                get_col!(q, r, "s1", Decimal).to_i32().unwrap(),
                get_col!(q, r, "s2", Decimal).to_i32().unwrap(),
            )
        })
        .sorted()
        .collect::<Vec<(i32, i32)>>();

    assert_eq!(res, vec![(1, 3), (5, 7), (12, 8)]);

    shutdown_tx.shutdown().await;
}

// multiple_aggregate_same_col_sharded tests multiple aggregators of different types operating on
// the same column in a sharded environment.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Ignoring sharded tests"]
async fn multiple_aggregate_same_col_sharded() {
    let (mut g, shutdown_tx) = start_simple("multiple_aggregate_same_col_sharded").await;

    g.extend_recipe(ChangeList::from_str("CREATE TABLE test (number int, value int);
         CREATE CACHE multiaggsamecolsharded FROM SELECT sum(value) AS s, avg(value) AS a FROM test GROUP BY number;", Dialect::DEFAULT_MYSQL).unwrap())
    .await
    .unwrap();

    let mut t = g.table("test").await.unwrap();
    let mut q = g
        .view("multiaggsamecolsharded")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    t.insert_many(vec![
        vec![DfValue::from(1i32), DfValue::from(1i32)],
        vec![DfValue::from(1i32), DfValue::from(4i32)],
        vec![DfValue::from(2i32), DfValue::from(5i32)],
        vec![DfValue::from(2i32), DfValue::from(7i32)],
        vec![DfValue::from(3i32), DfValue::from(1i32)],
    ])
    .await
    .unwrap();

    sleep().await;

    let rows = q.lookup(&[0i32.into()], true).await.unwrap();

    let res = rows
        .into_iter()
        .map(|r| {
            (
                get_col!(q, r, "s", Decimal).to_i32().unwrap(),
                get_col!(q, r, "a", f64),
            )
        })
        .sorted_by(|a, b| Ord::cmp(&a.0, &b.0))
        .collect::<Vec<(i32, f64)>>();

    assert_eq!(res, vec![(1, 1.), (5, 2.5), (12, 6.0)]);

    shutdown_tx.shutdown().await;
}

// multiple_aggregate_over_two tests the case of more than two aggregate functions being used in
// the same select query. This effectively tests our ability to appropriately generate multiple
// MirNodeInner::JoinAggregates nodes and join them all together correctly.
#[tokio::test(flavor = "multi_thread")]
async fn multiple_aggregate_over_two() {
    let (mut g, shutdown_tx) = start_simple_unsharded("multiple_aggregate_over_two").await;

    g.extend_recipe(ChangeList::from_str("CREATE TABLE test (number int, value int);
         CREATE CACHE multiaggovertwo FROM SELECT sum(value) AS s, avg(value) AS a, count(value) AS c, max(value) as m FROM test GROUP BY number;", Dialect::DEFAULT_MYSQL).unwrap())
    .await
    .unwrap();

    let mut t = g.table("test").await.unwrap();
    let mut q = g
        .view("multiaggovertwo")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    t.insert_many(vec![
        vec![DfValue::from(1i32), DfValue::from(1i32)],
        vec![DfValue::from(1i32), DfValue::from(4i32)],
        vec![DfValue::from(2i32), DfValue::from(5i32)],
        vec![DfValue::from(2i32), DfValue::from(7i32)],
        vec![DfValue::from(3i32), DfValue::from(1i32)],
    ])
    .await
    .unwrap();

    sleep().await;

    let rows = q.lookup(&[0i32.into()], true).await.unwrap();

    let res = rows
        .into_iter()
        .map(|r| {
            (
                get_col!(q, r, "s", Decimal).to_i32().unwrap(),
                get_col!(q, r, "a", f64),
                get_col!(q, r, "c", i32),
                get_col!(q, r, "m", i32),
            )
        })
        .sorted_by(|a, b| Ord::cmp(&a.0, &b.0))
        .collect::<Vec<(i32, f64, i32, i32)>>();

    assert_eq!(res, vec![(1, 1., 1, 1), (5, 2.5, 2, 4), (12, 6.0, 2, 7)]);

    shutdown_tx.shutdown().await;
}

// multiple_aggregate_over_two_sharded tests the case of more than two aggregate functions being
// used in the same select query. This effectively tests our ability to appropriately generate
// multiple MirNodeInner::JoinAggregates nodes and join them all together correctly in a sharded
// environment.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Ignoring sharded tests"]
async fn multiple_aggregate_over_two_sharded() {
    let (mut g, shutdown_tx) = start_simple("multiple_aggregate_over_two_sharded").await;

    g.extend_recipe(ChangeList::from_str("CREATE TABLE test (number int, value int);
         CREATE CACHE multiaggovertwosharded FROM SELECT sum(value) AS s, avg(value) AS a, count(value) AS c, max(value) as m FROM test GROUP BY number;", Dialect::DEFAULT_MYSQL).unwrap())
    .await
    .unwrap();

    let mut t = g.table("test").await.unwrap();
    let mut q = g
        .view("multiaggovertwosharded")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    t.insert_many(vec![
        vec![DfValue::from(1i32), DfValue::from(1i32)],
        vec![DfValue::from(1i32), DfValue::from(4i32)],
        vec![DfValue::from(2i32), DfValue::from(5i32)],
        vec![DfValue::from(2i32), DfValue::from(7i32)],
        vec![DfValue::from(3i32), DfValue::from(1i32)],
    ])
    .await
    .unwrap();

    sleep().await;

    let rows = q.lookup(&[0i32.into()], true).await.unwrap();

    let res = rows
        .into_iter()
        .map(|r| {
            (
                get_col!(q, r, "s", Decimal).to_i32().unwrap(),
                get_col!(q, r, "a", f64),
                get_col!(q, r, "c", i32),
                get_col!(q, r, "m", i32),
            )
        })
        .sorted_by(|a, b| Ord::cmp(&a.0, &b.0))
        .collect::<Vec<(i32, f64, i32, i32)>>();

    assert_eq!(res, vec![(1, 1., 1, 1), (5, 2.5, 2, 4), (12, 6.0, 2, 7)]);

    shutdown_tx.shutdown().await;
}

// multiple_aggregate_with_expressions tests multiple aggregates involving arithmetic expressions
// that would modify the output of the resulting columns. This tests that when we join aggregates
// that we are ignoring projection nodes appropriately.
#[tokio::test(flavor = "multi_thread")]
async fn multiple_aggregate_with_expressions() {
    let (mut g, shutdown_tx) = start_simple_unsharded("multiple_aggregate_with_expressions").await;

    g.extend_recipe(ChangeList::from_str("CREATE TABLE test (number int, value int);
         CREATE CACHE multiaggwexpressions FROM SELECT sum(value) AS s, 5 * avg(value) AS a FROM test GROUP BY number;", Dialect::DEFAULT_MYSQL).unwrap())
    .await
    .unwrap();

    let mut t = g.table("test").await.unwrap();
    let mut q = g
        .view("multiaggwexpressions")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    t.insert_many(vec![
        vec![DfValue::from(1i32), DfValue::from(1i32)],
        vec![DfValue::from(1i32), DfValue::from(4i32)],
        vec![DfValue::from(2i32), DfValue::from(5i32)],
        vec![DfValue::from(2i32), DfValue::from(7i32)],
        vec![DfValue::from(3i32), DfValue::from(1i32)],
    ])
    .await
    .unwrap();

    sleep().await;

    let rows = q.lookup(&[0i32.into()], true).await.unwrap();

    let res = rows
        .into_iter()
        .map(|r| {
            (
                get_col!(q, r, "s", Decimal).to_i32().unwrap(),
                get_col!(q, r, "a", f64),
            )
        })
        .sorted_by(|a, b| Ord::cmp(&a.0, &b.0))
        .collect::<Vec<(i32, f64)>>();

    assert_eq!(res, vec![(1, 5.), (5, 12.5), (12, 30.0)]);

    shutdown_tx.shutdown().await;
}

// multiple_aggregate_with_expressions_sharded tests multiple aggregates involving arithmetic
// expressions that would modify the output of the resulting columns. This tests that when we join
// aggregates that we are ignoring projection nodes appropriately in a sharded environment
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Ignoring sharded tests"]
async fn multiple_aggregate_with_expressions_sharded() {
    let (mut g, shutdown_tx) = start_simple("multiple_aggregate_with_expressions_sharded").await;

    g.extend_recipe(ChangeList::from_str("CREATE TABLE test (number int, value int);
         CREATE CACHE multiaggwexpressionssharded FROM SELECT sum(value) AS s, 5 * avg(value) AS a FROM test GROUP BY number;", Dialect::DEFAULT_MYSQL).unwrap())
    .await
    .unwrap();

    let mut t = g.table("test").await.unwrap();
    let mut q = g
        .view("multiaggwexpressionssharded")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    t.insert_many(vec![
        vec![DfValue::from(1i32), DfValue::from(1i32)],
        vec![DfValue::from(1i32), DfValue::from(4i32)],
        vec![DfValue::from(2i32), DfValue::from(5i32)],
        vec![DfValue::from(2i32), DfValue::from(7i32)],
        vec![DfValue::from(3i32), DfValue::from(1i32)],
    ])
    .await
    .unwrap();

    sleep().await;

    let rows = q.lookup(&[0i32.into()], true).await.unwrap();

    let res = rows
        .into_iter()
        .map(|r| {
            (
                get_col!(q, r, "s", Decimal).to_i32().unwrap(),
                get_col!(q, r, "a", f64),
            )
        })
        .sorted_by(|a, b| Ord::cmp(&a.0, &b.0))
        .collect::<Vec<(i32, f64)>>();

    assert_eq!(res, vec![(1, 5.), (5, 12.5), (12, 30.0)]);

    shutdown_tx.shutdown().await;
}

// multiple_aggregate_reuse tests a scenario that would trigger reuse. It tests this by generating
// an initial select query with multiple aggregates, and then generates another one involving
// shared nodes. This tests that reuse is being used appropriately in the case of aggregate joins.
#[tokio::test(flavor = "multi_thread")]
async fn multiple_aggregate_reuse() {
    let (mut g, shutdown_tx) = start_simple_unsharded("multiple_aggregate_reuse").await;

    g.extend_recipe(ChangeList::from_str("CREATE TABLE test (number int, value int);
         CREATE CACHE multiaggfirstquery FROM SELECT sum(value) AS s, 5 * avg(value) AS a FROM test GROUP BY number;", Dialect::DEFAULT_MYSQL).unwrap())
    .await
    .unwrap();

    let mut t = g.table("test").await.unwrap();
    let mut q = g
        .view("multiaggfirstquery")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    t.insert_many(vec![
        vec![DfValue::from(1i32), DfValue::from(1i32)],
        vec![DfValue::from(1i32), DfValue::from(4i32)],
        vec![DfValue::from(2i32), DfValue::from(5i32)],
        vec![DfValue::from(2i32), DfValue::from(7i32)],
        vec![DfValue::from(3i32), DfValue::from(1i32)],
    ])
    .await
    .unwrap();

    sleep().await;

    let rows = q.lookup(&[0i32.into()], true).await.unwrap();

    let res = rows
        .into_iter()
        .map(|r| {
            (
                get_col!(q, r, "s", Decimal).to_i32().unwrap(),
                get_col!(q, r, "a", f64),
            )
        })
        .sorted_by(|a, b| Ord::cmp(&a.0, &b.0))
        .collect::<Vec<(i32, f64)>>();

    assert_eq!(res, vec![(1, 5.), (5, 12.5), (12, 30.0)]);

    // Now we install a new recipe with a different aggregate query that should force aggregate
    // children to update and not be re-used. We're intentionally re-using the same name for the
    // second aggregate so as much is as similar as possible, to rule out names being different
    // forcing a false re-use.
    g.extend_recipe(ChangeList::from_str("CREATE CACHE multiaggsecondquery FROM SELECT sum(value) AS s, max(value) AS a FROM test GROUP BY number;", Dialect::DEFAULT_MYSQL).unwrap())
    .await
    .unwrap();

    q = g
        .view("multiaggsecondquery")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    sleep().await;

    let rows = q.lookup(&[0i32.into()], true).await.unwrap();

    let res = rows
        .into_iter()
        .map(|r| {
            (
                get_col!(q, r, "s", Decimal).to_i32().unwrap(),
                get_col!(q, r, "a", i32),
            )
        })
        .sorted_by(|a, b| Ord::cmp(&a.0, &b.0))
        .collect::<Vec<(i32, i32)>>();

    assert_eq!(res, vec![(1, 1), (5, 4), (12, 7)]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
// Test is ignored due subquery alias name reuse issue
// https://readysettech.atlassian.net/browse/ENG-167.
#[ignore]
async fn reuse_subquery_alias_name() {
    let (mut g, shutdown_tx) = start_simple_unsharded("reuse_subquery_alias_name").await;

    // Install two views, 'q1' and 'q2', each using the same subquery alias name 't2_data'.
    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE t1 (id int, value int);
         CREATE TABLE t2 (value int, name text, bio text);
         CREATE VIEW q1 AS
            SELECT name
            FROM t1
            JOIN (SELECT value, name FROM t2) AS t2_data ON t1.value = t2_data.value;
         CREATE VIEW q2 AS
            SELECT bio
            FROM t1
            JOIN (SELECT value, name, bio FROM t2) AS t2_data ON t1.value = t2_data.value;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    // TODO Verify that q1 and q2 return correct values.

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
// Test is ignored due column parsing bug https://readysettech.atlassian.net/browse/ENG-170.
#[ignore]
async fn col_beginning_with_literal() {
    let (mut g, shutdown_tx) = start_simple_unsharded("col_beginning_with_literal").await;

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE t1 (id INT, null_hypothesis INT);
         CREATE VIEW q1 AS SELECT null_hypothesis FROM t1;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    // TODO Verify that q1 returns correct values.

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn simple_enum() {
    let (mut g, shutdown_tx) = start_simple_unsharded("simple_enum").await;

    let sql = "
        CREATE TABLE t1 (id INT, color ENUM('red', 'yellow', 'green'));
        CREATE CACHE c1 FROM SELECT color FROM t1 WHERE id = ? ORDER BY color;
    ";

    g.extend_recipe(ChangeList::from_str(sql, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    let mut mutator = g.table("t1").await.unwrap();
    let mut getter = g.view("c1").await.unwrap().into_reader_handle().unwrap();

    let color_idxs = HashMap::from([("red", 1), ("yellow", 2), ("green", 3)]);
    let rows = vec!["green", "red", "purple"];
    for (i, &color) in rows.iter().enumerate() {
        mutator
            .insert(vec![
                i.into(),
                DfValue::UnsignedInt(*color_idxs.get(color).unwrap_or(&0)),
            ])
            .await
            .unwrap();
    }

    let lookup_keys = (0..3)
        .map(|k| KeyComparison::Equal(vec1![k.into()]))
        .collect();
    let result = getter
        .multi_lookup(lookup_keys, true)
        .await
        .unwrap()
        .into_vec();

    let result_type = getter
        .schema()
        .unwrap()
        .col_types([0], SchemaType::ReturnedSchema)
        .unwrap()[0];

    assert_eq!(result.len(), 3);

    assert_eq!(
        result[0][0],
        DfValue::from("purple")
            .coerce_to(result_type, &DfType::Unknown)
            .unwrap()
    );
    assert_eq!(
        result[1][0],
        DfValue::from("red")
            .coerce_to(result_type, &DfType::Unknown)
            .unwrap()
    );
    assert_eq!(
        result[2][0],
        DfValue::from("green")
            .coerce_to(result_type, &DfType::Unknown)
            .unwrap()
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn round_int_to_int() {
    let (mut g, shutdown_tx) = start_simple_unsharded("round_int_to_int").await;

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE test (value int);
         CREATE CACHE roundinttoint FROM SELECT round(value, -3) as r FROM test;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut t = g.table("test").await.unwrap();
    let mut q = g
        .view("roundinttoint")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    t.insert(vec![DfValue::from(888i32)]).await.unwrap();

    sleep().await;

    let rows = q.lookup(&[0i32.into()], true).await.unwrap();

    let res = rows
        .into_iter()
        .map(|r| get_col!(q, r, "r", i32))
        .collect::<Vec<i32>>();

    assert_eq!(res, vec![1000]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn round_float_to_float() {
    let (mut g, shutdown_tx) = start_simple_unsharded("round_float_to_float").await;

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE test (value double);
         CREATE CACHE roundfloattofloat FROM SELECT round(value, 2) as r FROM test;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut t = g.table("test").await.unwrap();
    let mut q = g
        .view("roundfloattofloat")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    t.insert(vec![DfValue::try_from(2.2222_f64).unwrap()])
        .await
        .unwrap();

    sleep().await;

    let rows = q.lookup(&[0i32.into()], true).await.unwrap();

    let res = rows
        .into_iter()
        .map(|r| get_col!(q, r, "r", f64))
        .collect::<Vec<f64>>();

    assert_eq!(res, vec![2.22_f64]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn round_float() {
    let (mut g, shutdown_tx) = start_simple_unsharded("round_float").await;

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE test (value double);
         CREATE CACHE roundfloat FROM SELECT round(value, 0) as r FROM test;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut t = g.table("test").await.unwrap();
    let mut q = g
        .view("roundfloat")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    t.insert(vec![DfValue::try_from(2.2222_f64).unwrap()])
        .await
        .unwrap();

    sleep().await;

    let rows = q.lookup(&[0i32.into()], true).await.unwrap();

    let res = rows
        .into_iter()
        .map(|r| get_col!(q, r, "r", f64))
        .collect::<Vec<f64>>();

    assert_eq!(res, vec![2.]);

    shutdown_tx.shutdown().await;
}

// This test checks a behavior that MySQL has of coercing floats into ints if the expected field
// type should be int. This means we should be able to pass in a float as the precision and have it
// rounded to an int for us.
#[tokio::test(flavor = "multi_thread")]
async fn round_with_precision_float() {
    let (mut g, shutdown_tx) = start_simple_unsharded("round_with_precision_float").await;

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE test (value double);
         CREATE CACHE roundwithprecisionfloat FROM SELECT round(value, -1.0) as r FROM test;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut t = g.table("test").await.unwrap();
    let mut q = g
        .view("roundwithprecisionfloat")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    t.insert(vec![DfValue::try_from(123.0_f64).unwrap()])
        .await
        .unwrap();

    sleep().await;

    let rows = q.lookup(&[0i32.into()], true).await.unwrap();

    let res = rows
        .into_iter()
        .map(|r| get_col!(q, r, "r", f64))
        .collect::<Vec<f64>>();

    assert_eq!(res, vec![120.]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn round_bigint_to_bigint() {
    let (mut g, shutdown_tx) = start_simple_unsharded("round_bigint_to_bigint").await;

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE test (value bigint);
         CREATE CACHE roundbiginttobigint FROM SELECT round(value, -3) as r FROM test;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut t = g.table("test").await.unwrap();
    let mut q = g
        .view("roundbiginttobigint")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    t.insert(vec![DfValue::from(888i32)]).await.unwrap();

    sleep().await;

    let rows = q.lookup(&[0i32.into()], true).await.unwrap();

    let res = rows
        .into_iter()
        .map(|r| get_col!(q, r, "r", i64))
        .collect::<Vec<i64>>();

    assert_eq!(res, vec![1000]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn round_unsignedint_to_unsignedint() {
    let (mut g, shutdown_tx) = start_simple_unsharded("round_unsignedint_to_unsignedint").await;

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE test (value int unsigned);
         CREATE CACHE roundunsignedtounsigned FROM SELECT round(value, -3) as r FROM test;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut t = g.table("test").await.unwrap();
    let mut q = g
        .view("roundunsignedtounsigned")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    t.insert(vec![DfValue::from(888i32)]).await.unwrap();

    sleep().await;

    let rows = q.lookup(&[0i32.into()], true).await.unwrap();

    let res = rows
        .into_iter()
        .map(|r| get_col!(q, r, "r", u32))
        .collect::<Vec<u32>>();

    assert_eq!(res, vec![1000]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn round_unsignedbigint_to_unsignedbitint() {
    let (mut g, shutdown_tx) =
        start_simple_unsharded("round_unsignedbigint_to_unsignedbitint").await;

    g.extend_recipe(
        ChangeList::from_str("CREATE TABLE test (value bigint unsigned);
         CREATE CACHE roundunsignedbiginttounsignedbigint FROM SELECT round(value, -3) as r FROM test;", Dialect::DEFAULT_MYSQL)
        .unwrap(),
    )
    .await
    .unwrap();

    let mut t = g.table("test").await.unwrap();
    let mut q = g
        .view("roundunsignedbiginttounsignedbigint")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    t.insert(vec![DfValue::from(888i32)]).await.unwrap();

    sleep().await;

    let rows = q.lookup(&[0i32.into()], true).await.unwrap();

    let res = rows
        .into_iter()
        .map(|r| get_col!(q, r, "r", u64))
        .collect::<Vec<u64>>();

    assert_eq!(res, vec![1000]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn round_with_no_precision() {
    let (mut g, shutdown_tx) = start_simple_unsharded("round_with_no_precision").await;

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE test (value bigint unsigned);
         CREATE CACHE roundwithnoprecision FROM SELECT round(value) as r FROM test;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut t = g.table("test").await.unwrap();
    let mut q = g
        .view("roundwithnoprecision")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    t.insert(vec![DfValue::try_from(56.2578_f64).unwrap()])
        .await
        .unwrap();

    sleep().await;

    let rows = q.lookup(&[0i32.into()], true).await.unwrap();

    let res = rows
        .into_iter()
        .map(|r| get_col!(q, r, "r", f64))
        .collect::<Vec<f64>>();

    assert_eq!(res, vec![56.]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn distinct_select_works() {
    let (mut g, shutdown_tx) = start_simple_unsharded("distinct_select_works").await;

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE test (value int);
         CREATE CACHE distinctselect FROM SELECT DISTINCT value as v FROM test;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut t = g.table("test").await.unwrap();
    let mut q = g
        .view("distinctselect")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    t.insert_many(vec![
        vec![DfValue::from(1i32)],
        vec![DfValue::from(1i32)],
        vec![DfValue::from(2i32)],
        vec![DfValue::from(2i32)],
        vec![DfValue::from(3i32)],
    ])
    .await
    .unwrap();

    sleep().await;

    let rows = q.lookup(&[0i32.into()], true).await.unwrap();

    let res = rows
        .into_iter()
        .map(|r| get_col!(q, r, "v", i32))
        .sorted()
        .collect::<Vec<i32>>();

    assert_eq!(res, vec![1, 2, 3]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn partial_distinct() {
    let (mut g, shutdown_tx) = start_simple_unsharded("partial_distinct").await;

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE test (value int, k int);
          CREATE CACHE distinctselect FROM SELECT DISTINCT value FROM test WHERE k = ?;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut t = g.table("test").await.unwrap();
    let mut q = g
        .view("distinctselect")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    macro_rules! do_lookup {
        ($q: expr, $k: expr) => {{
            let rows = $q.lookup(&[($k as i32).into()], true).await.unwrap();
            rows.into_iter()
                .map(|r| get_col!(q, r, "value", i32))
                .sorted()
                .collect::<Vec<i32>>()
        }};
    }

    t.insert_many(vec![
        vec![DfValue::from(1i32), DfValue::from(0)],
        vec![DfValue::from(1i32), DfValue::from(0)],
        vec![DfValue::from(2i32), DfValue::from(0)],
        vec![DfValue::from(2i32), DfValue::from(1)],
        vec![DfValue::from(3i32), DfValue::from(1)],
    ])
    .await
    .unwrap();

    sleep().await;

    assert_eq!(do_lookup!(q, 0), vec![1, 2]);

    t.delete_row(vec![DfValue::from(1), DfValue::from(0)])
        .await
        .unwrap();
    sleep().await;
    assert_eq!(do_lookup!(q, 0), vec![1, 2]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn partial_distinct_multi() {
    let (mut g, shutdown_tx) = start_simple_unsharded("partial_distinct_multi").await;

    g.extend_recipe(ChangeList::from_str("CREATE TABLE test (value int, number int, k int);
          CREATE CACHE distinctselectmulti FROM SELECT DISTINCT value, SUM(number) as s FROM test WHERE k = ?;", Dialect::DEFAULT_MYSQL)
        .unwrap(),
    )
    .await
    .unwrap();

    eprintln!("{}", g.graphviz().await.unwrap());

    let mut t = g.table("test").await.unwrap();
    let mut q = g
        .view("distinctselectmulti")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    t.insert_many(vec![
        vec![DfValue::from(1i32), DfValue::from(2i32), DfValue::from(0)],
        vec![DfValue::from(1i32), DfValue::from(4i32), DfValue::from(0)],
        vec![DfValue::from(2i32), DfValue::from(2i32), DfValue::from(0)],
        vec![DfValue::from(2i32), DfValue::from(6i32), DfValue::from(1)],
        vec![DfValue::from(3i32), DfValue::from(2i32), DfValue::from(1)],
    ])
    .await
    .unwrap();

    sleep().await;

    let rows = q.lookup(&[(0_i32).into()], true).await.unwrap();
    let res = rows
        .into_iter()
        .map(|r| {
            (
                get_col!(q, r, "value", i32),
                get_col!(q, r, "s", Decimal).to_i32().unwrap(),
            )
        })
        .sorted()
        .collect::<Vec<(i32, i32)>>();

    assert_eq!(res, vec![(1, 6), (2, 2)]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn distinct_select_multi_col() {
    let (mut g, shutdown_tx) = start_simple_unsharded("distinct_select_multi_col").await;

    g.extend_recipe(
        ChangeList::from_str("CREATE TABLE test (value int, number int);
         CREATE CACHE distinctselectmulticol FROM SELECT DISTINCT value as v, number as n FROM test;"
            , Dialect::DEFAULT_MYSQL)
            .unwrap(),
    )
    .await
    .unwrap();

    let mut t = g.table("test").await.unwrap();
    let mut q = g
        .view("distinctselectmulticol")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    t.insert_many(vec![
        vec![DfValue::from(1i32), DfValue::from(4i32)],
        vec![DfValue::from(1i32), DfValue::from(4i32)],
        vec![DfValue::from(2i32), DfValue::from(5i32)],
        vec![DfValue::from(2i32), DfValue::from(5i32)],
        vec![DfValue::from(3i32), DfValue::from(6i32)],
        vec![DfValue::from(3i32), DfValue::from(7i32)],
    ])
    .await
    .unwrap();

    sleep().await;

    let rows = q.lookup(&[0i32.into()], true).await.unwrap();

    let res = rows
        .into_iter()
        .map(|r| (get_col!(q, r, "v", i32), get_col!(q, r, "n", i32)))
        .sorted()
        .collect::<Vec<(i32, i32)>>();

    assert_eq!(res, vec![(1, 4), (2, 5), (3, 6), (3, 7)]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn assign_nonreader_domains_to_nonreader_workers() {
    let authority_store = Arc::new(LocalAuthorityStore::new());
    let w1_authority = Arc::new(Authority::from(LocalAuthority::new_with_store(
        authority_store.clone(),
    )));
    let w2_authority = Arc::new(Authority::from(LocalAuthority::new_with_store(
        authority_store,
    )));
    let cluster_name = "assign_nonreader_domains_to_nonreader_workers";

    let (mut w1, shutdown_tx_1) = build_custom(
        cluster_name,
        Some(DEFAULT_SHARDING),
        true,
        w1_authority,
        true,
        None,
    )
    .await;

    let query = "CREATE TABLE test (id integer, name text);\
        CREATE CACHE testquery FROM SELECT * FROM test;";
    let result = w1
        .extend_recipe(ChangeList::from_str(query, Dialect::DEFAULT_MYSQL).unwrap())
        .await;

    assert!(matches!(
        result,
        Err(ReadySetError::RpcFailed {
            during: _,
            // This 'box' keyword appears to be experimental.
            // So if this test ever fails because of that, feel free to change this.
            source: box MigrationPlanFailed { .. },
        })
    ));

    let (_w2, shutdown_tx_2) = build_custom(
        cluster_name,
        Some(DEFAULT_SHARDING),
        false,
        w2_authority,
        false,
        None,
    )
    .await;

    sleep().await;

    let result = w1
        .extend_recipe(ChangeList::from_str(query, Dialect::DEFAULT_MYSQL).unwrap())
        .await;
    println!("{:?}", result);
    assert!(matches!(result, Ok(_)));

    tokio::join!(shutdown_tx_1.shutdown(), shutdown_tx_2.shutdown());
}

#[tokio::test(flavor = "multi_thread")]
async fn join_straddled_columns() {
    let (mut g, shutdown_tx) = start_simple_unsharded("join_straddled_columns").await;

    g.extend_recipe(ChangeList::from_str(
            "CREATE TABLE a (a1 int, a2 int);
         CREATE TABLE b (b1 int, b2 int);
         CREATE CACHE straddle FROM SELECT * FROM a INNER JOIN b ON a.a2 = b.b1 WHERE a.a1 = ? AND b.b2 = ?;", Dialect::DEFAULT_MYSQL)
        .unwrap(),
    )
    .await
    .unwrap();

    let mut a = g.table("a").await.unwrap();
    let mut b = g.table("b").await.unwrap();
    let mut q = g
        .view("straddle")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    eprintln!("{}", g.graphviz().await.unwrap());

    a.insert_many(vec![
        vec![DfValue::from(1i32), DfValue::from(2i32)],
        vec![DfValue::from(1i32), DfValue::from(4i32)],
    ])
    .await
    .unwrap();

    b.insert_many(vec![
        vec![DfValue::from(2i32), DfValue::from(1i32)],
        vec![DfValue::from(2i32), DfValue::from(2i32)],
    ])
    .await
    .unwrap();

    sleep().await;

    let rows = q.lookup(&[1i32.into(), 1i32.into()], true).await.unwrap();

    let res = rows
        .into_iter()
        .map(|r| (get_col!(q, r, "a1", i32), get_col!(q, r, "a2", i32)))
        .sorted()
        .collect::<Vec<(i32, i32)>>();

    assert_eq!(res, vec![(1, 2)]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore] // with full materialization, needs post-lookup filters or it returns too many rows
async fn straddled_join_range_query() {
    readyset_tracing::init_test_logging();
    let (mut g, shutdown_tx) = start_simple_unsharded("straddled_join_range_query").await;

    g.extend_recipe(ChangeList::from_str(
            "CREATE TABLE a (a1 int, a2 int);
         CREATE TABLE b (b1 int, b2 int);
         CREATE CACHE straddle FROM SELECT * FROM a INNER JOIN b ON a.a2 = b.b1 WHERE a.a1 > ? AND b.b2 > ?;", Dialect::DEFAULT_MYSQL)
        .unwrap(),
    )
    .await
    .unwrap();

    let mut a = g.table("a").await.unwrap();
    let mut b = g.table("b").await.unwrap();
    let mut q = g
        .view("straddle")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    a.insert_many(vec![
        vec![DfValue::from(1i32), DfValue::from(2i32)],
        vec![DfValue::from(1i32), DfValue::from(4i32)],
        vec![DfValue::from(2i32), DfValue::from(2i32)],
        vec![DfValue::from(2i32), DfValue::from(4i32)],
    ])
    .await
    .unwrap();

    b.insert_many(vec![
        vec![DfValue::from(2i32), DfValue::from(1i32)],
        vec![DfValue::from(2i32), DfValue::from(2i32)],
    ])
    .await
    .unwrap();

    sleep().await;

    eprintln!("{}", g.graphviz().await.unwrap());

    let rows = q
        .multi_lookup(
            vec![KeyComparison::Range((
                Bound::Excluded(vec1![1i32.into(), 1i32.into()]),
                Bound::Unbounded,
            ))],
            true,
        )
        .await
        .unwrap();

    let res = rows
        .into_iter()
        .map(|r| (get_col!(q, r, "a1", i32), get_col!(q, r, "a2", i32)))
        .sorted()
        .collect::<Vec<(i32, i32)>>();

    assert_eq!(res, vec![(2, 2)]);

    shutdown_tx.shutdown().await;
}

// TODO(ENG-927): Flaky test.
#[tokio::test(flavor = "multi_thread", worker_threads = 20)]
#[ignore]
async fn overlapping_range_queries() {
    readyset_tracing::init_test_logging();
    let (mut g, shutdown_tx) = start_simple_unsharded("straddled_join_range_query").await;

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE t (x int);
         CREATE CACHE q FROM SELECT x FROM t WHERE x >= ?",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut t = g.table("t").await.unwrap();

    let n = 1000i32;
    t.insert_many((0..n).map(|n| vec![DfValue::from(n)]))
        .await
        .unwrap();

    let (tx, rx) = mpsc::channel(n as _);

    let readers = (0..(n / 10)).map(|m| {
        let tx = tx.clone();
        let mut g = g.clone();
        tokio::spawn(async move {
            let mut q = g.view("q").await.unwrap().into_reader_handle().unwrap();
            let results = q
                .multi_lookup(
                    vec![KeyComparison::Range((
                        Bound::Included(vec1![DfValue::from(m * 10)]),
                        Bound::Unbounded,
                    ))],
                    true,
                )
                .await
                .unwrap();

            let ns = results
                .into_iter()
                .map(|r| i32::try_from(r[0].clone()).unwrap())
                .collect::<Vec<i32>>();
            tx.send(ns).await.unwrap();
        })
    });

    futures::future::join_all(readers)
        .await
        .into_iter()
        .for_each(|r| r.unwrap());

    let mut results = ReceiverStream::new(rx)
        .take((n / 10) as _)
        .collect::<Vec<_>>()
        .await;

    results.sort();
    assert_eq!(
        results,
        (0..(n / 10))
            .map(|m| ((m * 10)..n).collect::<Vec<_>>())
            .collect::<Vec<_>>()
    );

    shutdown_tx.shutdown().await;
}

// TODO(ENG-927): Flaky test.
#[tokio::test(flavor = "multi_thread", worker_threads = 20)]
#[ignore]
async fn overlapping_remapped_range_queries() {
    readyset_tracing::init_test_logging();
    let (mut g, shutdown_tx) = start_simple_unsharded("overlapping_remapped_range_queries").await;

    g.extend_recipe(ChangeList::from_str(
            "CREATE TABLE a (a1 int, a2 int);
         CREATE TABLE b (b1 int, b2 int);
         CREATE CACHE q FROM SELECT * FROM a INNER JOIN b ON a.a2 = b.b1 WHERE a.a1 > ? AND b.b2 > ?;", Dialect::DEFAULT_MYSQL)
        .unwrap(),
    )
    .await
    .unwrap();

    let mut a = g.table("a").await.unwrap();
    let mut b = g.table("b").await.unwrap();

    let n = 1000i32;
    a.insert_many((0..n).map(|n| vec![DfValue::from(n), DfValue::from(n)]))
        .await
        .unwrap();
    b.insert_many((0..n).map(|n| vec![DfValue::from(n), DfValue::from(n)]))
        .await
        .unwrap();

    let (tx, rx) = mpsc::channel(n as _);

    let readers = (0..(n / 10)).map(|m| {
        let tx = tx.clone();
        let mut g = g.clone();
        tokio::spawn(async move {
            let mut q = g.view("q").await.unwrap().into_reader_handle().unwrap();
            let results = q
                .multi_lookup(
                    vec![KeyComparison::Range((
                        Bound::Included(vec1![DfValue::from(m * 10), DfValue::from(m * 10)]),
                        Bound::Unbounded,
                    ))],
                    true,
                )
                .await
                .unwrap();

            let ns = results
                .into_iter()
                .map(|r| {
                    (
                        i32::try_from(r[0].clone()).unwrap(),
                        i32::try_from(r[0].clone()).unwrap(),
                    )
                })
                .collect::<Vec<(i32, i32)>>();
            tx.send(ns).await.unwrap();
        })
    });

    futures::future::join_all(readers)
        .await
        .into_iter()
        .for_each(|r| r.unwrap());

    let mut results = ReceiverStream::new(rx)
        .take((n / 10) as _)
        .collect::<Vec<_>>()
        .await;

    results.sort();
    assert_eq!(
        results,
        (0..(n / 10))
            .map(|m| ((m * 10)..n).map(|p| (p, p)).collect::<Vec<_>>())
            .collect::<Vec<_>>()
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn range_query_through_union() {
    readyset_tracing::init_test_logging();
    let (mut g, shutdown_tx) = start_simple_unsharded("range_query_through_union").await;

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE t (a int, b int);
         CREATE CACHE q FROM SELECT a, b FROM t WHERE (a = 1 OR a = 2) AND b > ?",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut t = g.table("t").await.unwrap();
    let mut q = g.view("q").await.unwrap().into_reader_handle().unwrap();

    t.insert_many(vec![
        vec![DfValue::from(1), DfValue::from(1)],
        vec![DfValue::from(2), DfValue::from(1)],
        vec![DfValue::from(1), DfValue::from(2)],
        vec![DfValue::from(2), DfValue::from(2)],
        vec![DfValue::from(3), DfValue::from(2)],
    ])
    .await
    .unwrap();

    let rows = q
        .multi_lookup(
            vec![KeyComparison::Range((
                Bound::Excluded(vec1![1.into()]),
                Bound::Unbounded,
            ))],
            true,
        )
        .await
        .unwrap();

    let res = rows
        .into_iter()
        .map(|r| (get_col!(q, r, "a", i32), get_col!(q, r, "b", i32)))
        .sorted()
        .collect::<Vec<(i32, i32)>>();

    assert_eq!(res, vec![(1, 2), (2, 2)]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn mixed_inclusive_range_and_equality() {
    readyset_tracing::init_test_logging();

    let (mut g, shutdown_tx) = {
        let mut builder = Builder::for_tests();
        builder.set_sharding(Some(DEFAULT_SHARDING));
        builder.set_persistence(get_persistence_params("mixed_inclusive_range_and_equality"));
        builder.set_allow_mixed_comparisons(true);
        builder
            .start_local_custom(Arc::new(Authority::from(LocalAuthority::new_with_store(
                Arc::new(LocalAuthorityStore::new()),
            ))))
            .await
            .unwrap()
    };

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE t (x INT, y INT, z INT, w INT);
         CREATE CACHE q FROM
         SELECT x, y, z, w FROM t
         WHERE x >= $1 AND y = $2 AND z >= $3 AND w = $4;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut t = g.table("t").await.unwrap();

    t.insert_many::<_, Vec<DfValue>>(vec![
        // matches
        vec![1.into(), 2.into(), 3.into(), 4.into()],
        vec![2.into(), 2.into(), 3.into(), 4.into()],
        vec![1.into(), 2.into(), 4.into(), 4.into()],
        vec![2.into(), 2.into(), 4.into(), 4.into()],
        // non-matches
        vec![2.into(), 3.into(), 4.into(), 5.into()],
        vec![2.into(), 2.into(), 4.into(), 5.into()],
        vec![0.into(), 2.into(), 4.into(), 4.into()],
        vec![2.into(), 1.into(), 2.into(), 4.into()],
    ])
    .await
    .unwrap();

    let mut q = g.view("q").await.unwrap().into_reader_handle().unwrap();

    // Columns should be (y, w, x, z)
    //
    // where x = 0
    //       y = 1
    //       z = 2
    //       w = 3
    //
    // Then we can do a lookup of [{yᵥ, wᵥ, xᵥ, zᵥ}, {yᵥ, wᵥ, ⁺∞, ⁺∞})
    // to get y = yᵥ AND w = wᵥ AND x >= xᵥ AND z >= zᵥ
    assert_eq!(
        q.key_map(),
        &[
            (
                ViewPlaceholder::OneToOne(4, nom_sql::BinaryOperator::Equal),
                3
            ),
            (
                ViewPlaceholder::OneToOne(2, nom_sql::BinaryOperator::Equal),
                1
            ),
            (
                ViewPlaceholder::OneToOne(1, nom_sql::BinaryOperator::GreaterOrEqual),
                0
            ),
            (
                ViewPlaceholder::OneToOne(3, nom_sql::BinaryOperator::GreaterOrEqual),
                2
            )
        ]
    );

    let rows = q
        .multi_lookup(
            vec![KeyComparison::from_range(
                &(vec1![
                    DfValue::from(4i32),
                    DfValue::from(2i32),
                    DfValue::from(1i32),
                    DfValue::from(3i32)
                ]
                    ..=vec1![
                        DfValue::from(4i32),
                        DfValue::from(2i32),
                        DfValue::from(i32::MAX),
                        DfValue::from(i32::MAX)
                    ]),
            )],
            true,
        )
        .await
        .unwrap();

    let res = rows
        .into_iter()
        .map(|r| {
            (
                get_col!(q, r, "x", i32),
                get_col!(q, r, "y", i32),
                get_col!(q, r, "z", i32),
                get_col!(q, r, "w", i32),
            )
        })
        .sorted()
        .collect::<Vec<_>>();

    assert_eq!(
        res,
        vec![(1, 2, 3, 4), (1, 2, 4, 4), (2, 2, 3, 4), (2, 2, 4, 4)]
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn group_by_agg_col_count() {
    let (mut g, shutdown_tx) = start_simple_unsharded("group_by_agg_col_count").await;

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE test (value int, number int);
         CREATE CACHE groupbyaggcolcount FROM SELECT count(value) as c FROM test GROUP BY value;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut t = g.table("test").await.unwrap();
    let mut q = g
        .view("groupbyaggcolcount")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    t.insert_many(vec![
        vec![DfValue::from(1i32), DfValue::from(4i32)],
        vec![DfValue::from(1i32), DfValue::from(4i32)],
        vec![DfValue::from(2i32), DfValue::from(6i32)],
        vec![DfValue::from(2i32), DfValue::from(3i32)],
        vec![DfValue::from(2i32), DfValue::from(3i32)],
        vec![DfValue::from(3i32), DfValue::from(2i32)],
        vec![DfValue::from(3i32), DfValue::from(7i32)],
        vec![DfValue::from(3i32), DfValue::from(7i32)],
        vec![DfValue::from(3i32), DfValue::from(7i32)],
    ])
    .await
    .unwrap();

    sleep().await;

    let rows = q.lookup(&[0i32.into()], true).await.unwrap();

    let res = rows
        .into_iter()
        .map(|r| get_col!(q, r, "c", i32))
        .sorted()
        .collect::<Vec<i32>>();

    assert_eq!(res, vec![2, 3, 4]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn group_by_agg_col_multi() {
    let (mut g, shutdown_tx) = start_simple_unsharded("group_by_agg_col_multi").await;

    g.extend_recipe(ChangeList::from_str("CREATE TABLE test (value int, number int);
         CREATE CACHE groupbyaggcolmulti FROM SELECT count(value) as c, avg(number) as a FROM test GROUP BY value;", Dialect::DEFAULT_MYSQL).unwrap())
    .await
    .unwrap();

    let mut t = g.table("test").await.unwrap();
    let mut q = g
        .view("groupbyaggcolmulti")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    t.insert_many(vec![
        vec![DfValue::from(1i32), DfValue::from(4i32)],
        vec![DfValue::from(1i32), DfValue::from(4i32)],
        vec![DfValue::from(2i32), DfValue::from(6i32)],
        vec![DfValue::from(2i32), DfValue::from(3i32)],
        vec![DfValue::from(2i32), DfValue::from(3i32)],
        vec![DfValue::from(3i32), DfValue::from(2i32)],
        vec![DfValue::from(3i32), DfValue::from(7i32)],
        vec![DfValue::from(3i32), DfValue::from(7i32)],
        vec![DfValue::from(3i32), DfValue::from(8i32)],
    ])
    .await
    .unwrap();

    sleep().await;

    let rows = q.lookup(&[0i32.into()], true).await.unwrap();

    let res = rows
        .into_iter()
        .map(|r| (get_col!(q, r, "c", i32), get_col!(q, r, "a", f64)))
        .sorted_by(|a, b| a.0.cmp(&b.0))
        .collect::<Vec<(i32, f64)>>();

    assert_eq!(res, vec![(2, 4.), (3, 4.), (4, 6.)]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn group_by_agg_col_with_join() {
    let (mut g, shutdown_tx) = start_simple_unsharded("group_by_agg_col_with_join").await;
    let sql = "
        # base tables
        CREATE TABLE test (id int, number int, PRIMARY KEY(id));
        CREATE TABLE test2 (test_id int, value int);

        # read queries
        CREATE CACHE groupbyaggcolwithjoin FROM SELECT count(number) as c, avg(test2.value) AS a \
                    FROM test \
                    INNER JOIN test2 \
                    ON (test.id = test2.test_id)
                    GROUP BY number;
    ";

    g.extend_recipe(ChangeList::from_str(sql, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();
    let mut test = g.table("test").await.unwrap();
    let mut test2 = g.table("test2").await.unwrap();
    let mut q = g
        .view("groupbyaggcolwithjoin")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();

    test.insert_many(vec![
        vec![0i64.into(), 10i64.into()],
        vec![1i64.into(), 10i64.into()],
        vec![2i64.into(), 10i64.into()],
    ])
    .await
    .unwrap();
    test2
        .insert_many(vec![
            vec![0i64.into(), 20i64.into()],
            vec![1i64.into(), 20i64.into()],
            vec![2i64.into(), 20i64.into()],
        ])
        .await
        .unwrap();

    sleep().await;

    let rows = q.lookup(&[0i32.into()], true).await.unwrap();

    let res = rows
        .into_iter()
        .map(|r| (get_col!(q, r, "c", i32), get_col!(q, r, "a", f64)))
        .sorted_by(|a, b| a.0.cmp(&b.0))
        .collect::<Vec<(i32, f64)>>();

    assert_eq!(res, vec![(3, 20.)]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn count_emit_zero() {
    let (mut g, shutdown_tx) = start_simple_unsharded("count_emit_zero").await;
    let sql = "
        # base tables
        CREATE TABLE test (id int);

        # read queries
        CREATE CACHE countemitzero FROM SELECT count(id) as c FROM test GROUP BY id;
        CREATE CACHE countemitzeronogroup FROM SELECT count(*) as c FROM test;
        CREATE CACHE countemitzeromultiple FROM SELECT COUNT(id) AS c, COUNT(*) AS c2 FROM test;
        CREATE CACHE countemitzerowithcolumn FROM SELECT id, COUNT(*) AS c FROM test;
        CREATE CACHE countemitzerowithotheraggregations FROM SELECT COUNT(*) AS c, SUM(id) AS s, MIN(id) AS m FROM test;
    ";
    g.extend_recipe(ChangeList::from_str(sql, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    // With no data in the table, we should get no results with a GROUP BY
    let mut q = g
        .view("countemitzero")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    let rows = q.lookup(&[0i32.into()], true).await.unwrap();
    let res = rows
        .into_iter()
        .map(|r| i32::try_from(&r[0]).unwrap())
        .sorted()
        .collect::<Vec<i32>>();
    assert_eq!(res, Vec::<i32>::new());

    // With no data in the table, we should get results _without_ a GROUP BY
    let mut q = g
        .view("countemitzeronogroup")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    let rows = q.lookup(&[0i32.into()], true).await.unwrap();
    let res = rows
        .into_iter()
        .map(|r| i32::try_from(&r[0]).unwrap())
        .sorted()
        .collect::<Vec<i32>>();
    assert_eq!(res, vec![0]);

    // With no data in the table, we should get results with two COUNT()s and no GROUP BY
    let mut q = g
        .view("countemitzeromultiple")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    let rows = q.lookup(&[0i32.into()], true).await.unwrap();
    let res = rows
        .into_iter()
        .map(|r| r.into_iter().map(|v| i32::try_from(v).unwrap()).collect())
        .sorted()
        .collect::<Vec<Vec<i32>>>();
    assert_eq!(res, vec![vec![0, 0]]);

    // With no data in the table, we should get a NULL for any columns, and a 0 for COUNT()
    let mut q = g
        .view("countemitzerowithcolumn")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    let rows = q.lookup(&[0i32.into()], true).await.unwrap();
    let res = rows.into_iter().collect::<Vec<Vec<DfValue>>>();
    assert_eq!(res, vec![vec![DfValue::None, DfValue::Int(0)]]);

    // With no data in the table, we should get a 0 for COUNT(), and a NULL for other aggregations
    let mut q = g
        .view("countemitzerowithotheraggregations")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    let rows = q.lookup(&[0i32.into()], true).await.unwrap();
    let res = rows.into_iter().collect::<Vec<Vec<DfValue>>>();
    assert_eq!(
        res,
        vec![vec![DfValue::Int(0), DfValue::None, DfValue::None]]
    );

    // Now let's add some data to ensure count is still correct.
    let mut test = g.table("test").await.unwrap();
    test.insert_many(vec![
        vec![0i64.into()],
        vec![0i64.into()],
        vec![0i64.into()],
    ])
    .await
    .unwrap();
    sleep().await;

    let mut q = g
        .view("countemitzero")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    let rows = q.lookup(&[0i32.into()], true).await.unwrap();
    let res = rows
        .into_iter()
        .map(|r| i32::try_from(&r[0]).unwrap())
        .sorted()
        .collect::<Vec<i32>>();
    assert_eq!(res, vec![3]);

    let mut q = g
        .view("countemitzeronogroup")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    let rows = q.lookup(&[0i32.into()], true).await.unwrap();
    let res = rows
        .into_iter()
        .map(|r| i32::try_from(&r[0]).unwrap())
        .sorted()
        .collect::<Vec<i32>>();
    assert_eq!(res, vec![3]);

    let mut q = g
        .view("countemitzeromultiple")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    let rows = q.lookup(&[0i32.into()], true).await.unwrap();
    let res = rows
        .into_iter()
        .map(|r| r.into_iter().map(|v| i32::try_from(v).unwrap()).collect())
        .sorted()
        .collect::<Vec<Vec<i32>>>();
    assert_eq!(res, vec![vec![3, 3]]);

    let mut q = g
        .view("countemitzerowithcolumn")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    let rows = q.lookup(&[0i32.into()], true).await.unwrap();
    let res = rows.into_iter().collect::<Vec<Vec<DfValue>>>();
    assert_eq!(res, vec![vec![DfValue::Int(0), DfValue::Int(3)]]);

    let mut q = g
        .view("countemitzerowithotheraggregations")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    let rows = q.lookup(&[0i32.into()], true).await.unwrap();
    let res = rows.into_iter().collect::<Vec<Vec<DfValue>>>();
    assert_eq!(
        res,
        vec![vec![
            DfValue::Int(3),
            Decimal::from(0).into(),
            DfValue::Int(0)
        ]]
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn partial_join_on_one_parent() {
    let (mut g, shutdown_tx) = start_simple_unsharded("partial_join_on_one_parent").await;
    g.extend_recipe(
        ChangeList::from_str(
            "
        CREATE TABLE t1 (jk INT, val INT);
        CREATE TABLE t2 (jk INT, pk INT PRIMARY KEY);
    ",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut t1 = g.table("t1").await.unwrap();
    let mut t2 = g.table("t2").await.unwrap();

    t1.insert_many(
        iter::once(vec![DfValue::from(1i32), DfValue::from(1i32)])
            .cycle()
            .take(5),
    )
    .await
    .unwrap();

    t2.insert_many((1i32..=5).map(|pk| vec![DfValue::from(1i32), DfValue::from(pk)]))
        .await
        .unwrap();

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE CACHE q FROM SELECT t1.val FROM t2 JOIN t1 ON t2.jk = t1.jk WHERE t1.val = ?",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut q = g.view("q").await.unwrap().into_reader_handle().unwrap();

    let res1 = q
        .lookup(&[DfValue::from(1i32)], true)
        .await
        .unwrap()
        .into_vec();
    assert_eq!(res1.len(), 25);

    t2.delete(vec![DfValue::from(1i32)]).await.unwrap();

    sleep().await;

    let res2 = q
        .lookup(&[DfValue::from(1i32)], true)
        .await
        .unwrap()
        .into_vec();
    assert_eq!(res2.len(), 20);

    shutdown_tx.shutdown().await;
}

const LIMIT: usize = 10;

async fn aggressive_eviction_setup() -> (crate::Handle, ShutdownSender) {
    let (mut g, shutdown_tx) = build(
        "aggressive_eviction",
        None,
        Some((15000, Duration::from_millis(4))),
    )
    .await;

    g.extend_recipe(ChangeList::from_str(format!(r"
        CREATE TABLE `articles` (
            `id` int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `creation_time` timestamp NOT NULL DEFAULT current_timestamp(),
            `keywords` varchar(40) NOT NULL,
            `title` varchar(128) NOT NULL,
            `short_text` varchar(512) NOT NULL,
            `url` varchar(128) NOT NULL
        );

        CREATE TABLE `users` (
            `id` int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY
        );

        CREATE TABLE `recommendations` (
            `user_id` int(11) NOT NULL,
            `article_id` int(11) NOT NULL
        );

        CREATE CACHE w FROM SELECT A.id, A.title, A.keywords, A.creation_time, A.short_text, A.url FROM articles AS A, recommendations AS R WHERE ((A.id = R.article_id) AND (R.user_id = ?)) LIMIT {LIMIT};

        CREATE CACHE v FROM SELECT A.id, A.title, A.keywords, A.creation_time, A.short_text, A.url FROM articles AS A, recommendations AS R WHERE ((A.id = R.article_id) AND (R.user_id > ?)) LIMIT {LIMIT};
        "), Dialect::DEFAULT_MYSQL).unwrap()
    )
    .await
    .unwrap();

    let mut users = g.table("users").await.unwrap();
    let mut articles = g.table("articles").await.unwrap();
    let mut recommendations = g.table("recommendations").await.unwrap();

    users
        .insert_many((0i32..30).map(|id| vec![DfValue::from(id)]))
        .await
        .unwrap();

    articles
        .insert_many((0i32..20).map(|id| {
            vec![
                DfValue::from(id),
                DfValue::from("2020-01-01 12:30:45"),
                DfValue::from("asdasdasd"),
                DfValue::from("asdasdsadsadas"),
                DfValue::from("asdasdasdasd"),
                DfValue::from("asdjashdkjsahd"),
            ]
        }))
        .await
        .unwrap();

    recommendations
        .insert_many((0i32..30).flat_map(|id| {
            (0i32..20).map(move |article| vec![DfValue::from(id), DfValue::from(article)])
        }))
        .await
        .unwrap();

    (g, shutdown_tx)
}

async fn aggressive_eviction_impl() {
    let (mut g, shutdown_tx) = aggressive_eviction_setup().await;
    let mut view = g.view("w").await.unwrap().into_reader_handle().unwrap();

    for i in 0..500 {
        let offset = i % 10;
        let keys: Vec<_> = (offset..offset + 20)
            .map(|k| KeyComparison::Equal(vec1::Vec1::new(DfValue::Int(k))))
            .collect();

        let vq = ViewQuery::from((keys.clone(), true));

        let r = view.raw_lookup(vq).await.unwrap().into_vec();
        assert_eq!(r.len(), LIMIT);
    }

    shutdown_tx.shutdown().await;
}

async fn aggressive_eviction_range_impl() {
    let (mut g, shutdown_tx) = aggressive_eviction_setup().await;

    let mut view = g.view("v").await.unwrap().into_reader_handle().unwrap();

    for i in (0..500).rev() {
        let offset = i % 10;
        let vq = ViewQuery::from((
            vec![KeyComparison::Range((
                Bound::Included(vec1![DfValue::Int(offset)]),
                Bound::Excluded(vec1![DfValue::Int(offset + 20)]),
            ))],
            true,
        ));

        let r = view.raw_lookup(vq).await.unwrap().into_vec();
        assert_eq!(r.len(), LIMIT);
    }

    shutdown_tx.shutdown().await;
}

rusty_fork_test! {
    #[test]
    #[ignore = "Flaky (REA-2880)"]
    fn aggressive_eviction() {
        if skip_with_flaky_finder() {
            return;
        }

        // #[tokio::test] doesn't play well with rusty_fork_test, so have to do this manually
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(aggressive_eviction_impl());
    }

    #[test]
    fn aggressive_eviction_range() {
        if skip_with_flaky_finder() {
            return;
        }

        // #[tokio::test] doesn't play well with rusty_fork_test, so have to do this manually
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(aggressive_eviction_range_impl());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn partial_ingress_above_full_reader() {
    readyset_tracing::init_test_logging();
    let (mut g, shutdown_tx) = start_simple_unsharded("partial_ingress_above_full_reader").await;
    g.extend_recipe(
        ChangeList::from_str("CREATE TABLE t1 (a INT, b INT);", Dialect::DEFAULT_MYSQL).unwrap(),
    )
    .await
    .unwrap();
    g.extend_recipe(
        ChangeList::from_str("CREATE TABLE t2 (c int, d int);", Dialect::DEFAULT_MYSQL).unwrap(),
    )
    .await
    .unwrap();
    g.extend_recipe(ChangeList::from_str("CREATE CACHE q1 FROM select t1.a, t1.b, t2.c, t2.d from t1 inner join t2 on t1.a = t2.c where t1.b = ?;", Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();
    g.extend_recipe(ChangeList::from_str(
        "CREATE CACHE q2 FROM select t1.a, t1.b, t2.c, t2.d from t1 inner join t2 on t1.a = t2.c;"
            , Dialect::DEFAULT_MYSQL)
            .unwrap(),
    )
    .await
    .unwrap();

    let mut m1 = g.table("t1").await.unwrap();
    let mut m2 = g.table("t2").await.unwrap();

    m1.insert(vec![1.into(), 2.into()]).await.unwrap();
    m2.insert(vec![1.into(), 1.into()]).await.unwrap();
    m2.insert(vec![3.into(), 3.into()]).await.unwrap();

    let mut g1 = g.view("q1").await.unwrap().into_reader_handle().unwrap();
    let r1 = g1.lookup(&[2i64.into()], true).await.unwrap().into_vec();

    let mut g2 = g.view("q2").await.unwrap().into_reader_handle().unwrap();
    let r2 = g2.lookup(&[0i64.into()], true).await.unwrap().into_vec();

    assert_eq!(
        r1,
        vec![vec![
            DfValue::Int(1),
            DfValue::Int(2),
            DfValue::Int(1),
            DfValue::Int(1)
        ]]
    );
    assert_eq!(
        r2,
        vec![vec![
            DfValue::Int(1),
            DfValue::Int(2),
            DfValue::Int(1),
            DfValue::Int(1)
        ]]
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn reroutes_recursively() {
    readyset_tracing::init_test_logging();
    let (mut g, shutdown_tx) = start_simple_unsharded("reroutes_twice").await;

    let sql = "
        create table t1 (a int, b int);
        create table t2 (c int, d int);
        create table t3 (e int, f int);
        create table t4 (g int, h int);
        ";
    g.extend_recipe(ChangeList::from_str(sql, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    let sql = "
        CREATE CACHE q1 FROM SELECT t2.c, t2.d, t3.e, t3.f FROM t2 INNER JOIN t3 ON t2.c = t3.e WHERE t2.c = ?;
        CREATE CACHE q2 FROM SELECT t1.a, t1.b, q1.c, q1.d FROM t1 INNER JOIN q1 on t1.a = q1.c WHERE t1.b = ?;
        CREATE CACHE q3 FROM SELECT t4.g, t4.h, q2.a, q2.b FROM t4 INNER JOIN q2 on t4.g = q2.a WHERE t4.g = ?;
        ";
    g.extend_recipe(ChangeList::from_str(sql, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    let sql2 = "
        CREATE CACHE q4 FROM SELECT t4.g, t4.h, q2.a, q2.b FROM t4 INNER JOIN q2 on t4.g = q2.a;
       ";
    g.extend_recipe(ChangeList::from_str(sql2, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();
    eprintln!("{}", g.graphviz().await.unwrap());
    let mut m1 = g.table("t1").await.unwrap();
    m1.insert(vec![1.into(), 2.into()]).await.unwrap();
    let mut m2 = g.table("t2").await.unwrap();
    m2.insert(vec![1.into(), 1.into()]).await.unwrap();
    let mut m3 = g.table("t3").await.unwrap();
    m3.insert(vec![1.into(), 1.into()]).await.unwrap();
    let mut m4 = g.table("t4").await.unwrap();
    m4.insert(vec![1.into(), 1.into()]).await.unwrap();

    sleep().await;

    let mut getter = g.view("q4").await.unwrap().into_reader_handle().unwrap();
    let res = getter
        .lookup(&[0i64.into()], true)
        .await
        .unwrap()
        .into_vec();
    assert_eq!(res[0][0], 1.into());
    assert_eq!(res[0][1], 1.into());
    assert_eq!(res[0][2], 1.into());
    assert_eq!(res[0][3], 2.into());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn reroutes_two_children_at_once() {
    readyset_tracing::init_test_logging();
    let (mut g, shutdown_tx) = start_simple_unsharded("reroutes_two_children_at_once").await;
    let sql = "
        create table t1 (a int, b int);
        create table t2 (c int, d int);
        ";
    g.extend_recipe(ChangeList::from_str(sql, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();
    let sql1 = "
        CREATE CACHE q1 FROM SELECT t1.a, t1.b, t2.c, t2.d FROM t1 INNER JOIN t2 ON t1.a = t2.c WHERE t1.b = ?;
        ";
    g.extend_recipe(ChangeList::from_str(sql1, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();
    eprintln!("{}", g.graphviz().await.unwrap());

    let sql2 = "
        CREATE CACHE q2 FROM SELECT t1.a, t1.b, t2.c, t2.d FROM t1 INNER JOIN t2 ON t1.a = t2.c;
        CREATE CACHE q3 FROM SELECT t1.a, t1.b, t2.d FROM t1 INNER JOIN t2 ON t1.a = t2.c;
        ";
    g.extend_recipe(ChangeList::from_str(sql2, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();
    eprintln!("{}", g.graphviz().await.unwrap());

    let mut m1 = g.table("t1").await.unwrap();
    let mut m2 = g.table("t2").await.unwrap();

    m1.insert(vec![1.into(), 2.into()]).await.unwrap();
    m2.insert(vec![1.into(), 1.into()]).await.unwrap();
    m2.insert(vec![3.into(), 3.into()]).await.unwrap();

    let mut g1 = g.view("q1").await.unwrap().into_reader_handle().unwrap();
    let r1 = g1.lookup(&[2i64.into()], true).await.unwrap().into_vec();

    let mut g2 = g.view("q2").await.unwrap().into_reader_handle().unwrap();
    let r2 = g2.lookup(&[0i64.into()], true).await.unwrap().into_vec();

    let mut g3 = g.view("q3").await.unwrap().into_reader_handle().unwrap();
    let r3 = g3.lookup(&[0i64.into()], true).await.unwrap().into_vec();

    assert_eq!(
        r1,
        vec![vec![
            DfValue::Int(1),
            DfValue::Int(2),
            DfValue::Int(1),
            DfValue::Int(1)
        ]]
    );
    assert_eq!(
        r2,
        vec![vec![
            DfValue::Int(1),
            DfValue::Int(2),
            DfValue::Int(1),
            DfValue::Int(1)
        ]]
    );
    assert_eq!(
        r3,
        vec![vec![DfValue::Int(1), DfValue::Int(2), DfValue::Int(1)]]
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn reroutes_same_migration() {
    readyset_tracing::init_test_logging();
    let (mut g, shutdown_tx) = start_simple_reuse_unsharded("reroutes_same_migration").await;
    let sql = "
        create table t1 (a int, b int);
        create table t2 (c int, d int);
        CREATE CACHE q1 FROM SELECT t1.a, t1.b, t2.c, t2.d FROM t1 INNER JOIN t2 ON t1.a = t2.c WHERE t1.b = ?;
        CREATE CACHE q2 FROM SELECT t1.a, t1.b, t2.c, t2.d FROM t1 INNER JOIN t2 ON t1.a = t2.c;
        CREATE CACHE q3 FROM SELECT t1.b, t2.d FROM t1 INNER JOIN t2 ON t1.a = t2.c;
        ";
    g.extend_recipe(ChangeList::from_str(sql, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();
    eprintln!("{}", g.graphviz().await.unwrap());

    let mut m1 = g.table("t1").await.unwrap();
    let mut m2 = g.table("t2").await.unwrap();

    m1.insert(vec![1.into(), 2.into()]).await.unwrap();
    m2.insert(vec![1.into(), 1.into()]).await.unwrap();
    m2.insert(vec![3.into(), 3.into()]).await.unwrap();

    let mut g1 = g.view("q1").await.unwrap().into_reader_handle().unwrap();
    let r1 = g1.lookup(&[2i64.into()], true).await.unwrap();

    let mut g2 = g.view("q2").await.unwrap().into_reader_handle().unwrap();
    let r2 = g2.lookup(&[0i64.into()], true).await.unwrap();

    let mut g3 = g.view("q3").await.unwrap().into_reader_handle().unwrap();
    let r3 = g3.lookup(&[0i64.into()], true).await.unwrap();

    assert_eq!(
        r1.into_vec(),
        vec![vec![
            DfValue::Int(1),
            DfValue::Int(2),
            DfValue::Int(1),
            DfValue::Int(1)
        ]]
    );
    assert_eq!(
        r2.into_vec(),
        vec![vec![
            DfValue::Int(1),
            DfValue::Int(2),
            DfValue::Int(1),
            DfValue::Int(1)
        ]]
    );
    assert_eq!(r3.into_vec(), vec![vec![DfValue::Int(2), DfValue::Int(1)]]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn reroutes_dependent_children() {
    readyset_tracing::init_test_logging();
    let (mut g, shutdown_tx) = start_simple_unsharded("reroutes_dependent_children").await;
    let sql = "
        create table t1 (a int, b int);
        create table t2 (c int, d int);
        ";
    g.extend_recipe(ChangeList::from_str(sql, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();
    let sql1 = "
        CREATE CACHE q1 FROM SELECT t1.a, t1.b, t2.c, t2.d FROM t1 INNER JOIN t2 ON t1.a = t2.c WHERE t1.b = ?;
        ";
    g.extend_recipe(ChangeList::from_str(sql1, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();
    let sql2 = "
        CREATE CACHE q2 FROM SELECT t1.a, t1.b, t2.c, t2.d FROM t1 INNER JOIN t2 ON t1.a = t2.c;
        CREATE CACHE q3 FROM SELECT q2.a, q2.c FROM q2;
        ";
    g.extend_recipe(ChangeList::from_str(sql2, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();
    eprintln!("{}", g.graphviz().await.unwrap());

    let mut m1 = g.table("t1").await.unwrap();
    let mut m2 = g.table("t2").await.unwrap();

    m1.insert(vec![1.into(), 2.into()]).await.unwrap();
    m2.insert(vec![1.into(), 1.into()]).await.unwrap();
    m2.insert(vec![3.into(), 3.into()]).await.unwrap();

    let mut g1 = g.view("q1").await.unwrap().into_reader_handle().unwrap();
    let r1 = g1.lookup(&[2i64.into()], true).await.unwrap().into_vec();

    let mut g2 = g.view("q2").await.unwrap().into_reader_handle().unwrap();
    let r2 = g2.lookup(&[0i64.into()], true).await.unwrap().into_vec();

    let mut g3 = g.view("q3").await.unwrap().into_reader_handle().unwrap();
    let r3 = g3.lookup(&[0i64.into()], true).await.unwrap().into_vec();

    assert_eq!(
        r1,
        vec![vec![
            DfValue::Int(1),
            DfValue::Int(2),
            DfValue::Int(1),
            DfValue::Int(1)
        ]]
    );
    assert_eq!(
        r2,
        vec![vec![
            DfValue::Int(1),
            DfValue::Int(2),
            DfValue::Int(1),
            DfValue::Int(1)
        ]]
    );
    assert_eq!(r3, vec![vec![DfValue::Int(1), DfValue::Int(1)]]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn reroutes_count() {
    readyset_tracing::init_test_logging();
    let (mut g, shutdown_tx) = start_simple_reuse_unsharded("reroutes_count").await;
    let sql = "
            create table votes (user INT, id INT);
            CREATE CACHE q1 FROM select count(user) from votes where id = ? group by id;
            ";
    g.extend_recipe(ChangeList::from_str(sql, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();
    let sql2 = "
            CREATE CACHE q2 FROM select count(user) from votes group by id;";
    g.extend_recipe(ChangeList::from_str(sql2, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    let mut m = g.table("votes").await.unwrap();
    let mut g1 = g.view("q1").await.unwrap().into_reader_handle().unwrap();
    let mut g2 = g.view("q2").await.unwrap().into_reader_handle().unwrap();

    m.insert(vec![1.into(), 1.into()]).await.unwrap();
    m.insert(vec![1.into(), 2.into()]).await.unwrap();
    m.insert(vec![1.into(), 3.into()]).await.unwrap();
    m.insert(vec![2.into(), 1.into()]).await.unwrap();

    sleep().await;

    let r1 = g1.lookup(&[1i64.into()], true).await.unwrap();
    let r2 = g2.lookup(&[0i64.into()], true).await.unwrap();
    assert_eq!(r1.into_vec(), vec![vec![DfValue::Int(2)]]);
    assert_eq!(
        r2.into_vec(),
        vec![
            vec![DfValue::Int(1)],
            vec![DfValue::Int(1)],
            vec![DfValue::Int(2)]
        ]
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_diamond_union() {
    readyset_tracing::init_test_logging();

    let (mut g, shutdown_tx) = start_simple_unsharded("double_diamond_union").await;
    let create_table = "
        # base tables
        CREATE TABLE table_1 (column_1 INT);
    ";
    g.extend_recipe(ChangeList::from_str(create_table, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    let mut table_1 = g.table("table_1").await.unwrap();

    table_1
        .insert_many(([0, 6]).map(|column_1_value| vec![DfValue::from(column_1_value)]))
        .await
        .unwrap();

    let create_query = "
        # read query
        CREATE CACHE multi_diamond_union FROM SELECT table_1.column_1 AS alias_1, table_1.column_1 AS alias_2, table_1.column_1 AS alias_3
            FROM table_1 WHERE (
                ((table_1.column_1 IS NULL) OR (table_1.column_1 IS NOT NULL))
                AND table_1.column_1 NOT BETWEEN 1 AND 5
                AND ((table_1.column_1 IS NULL) OR (table_1.column_1 IS NOT NULL))
                AND table_1.column_1 NOT BETWEEN 1 AND 5
            );
    ";

    g.extend_recipe(ChangeList::from_str(create_query, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    let mut q = g
        .view("multi_diamond_union")
        .await
        .unwrap()
        .into_reader_handle()
        .unwrap();
    let rows = q.lookup(&[0i32.into()], true).await.unwrap();
    let res = rows
        .into_iter()
        .map(|r| i32::try_from(&r[0]).unwrap())
        .sorted()
        .collect::<Vec<i32>>();
    let expected = vec![0, 6];
    assert_eq!(res, expected);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn forbid_full_materialization() {
    let (mut g, shutdown_tx) = {
        let mut builder = Builder::for_tests();
        builder.set_sharding(Some(DEFAULT_SHARDING));
        builder.set_persistence(get_persistence_params("forbid_full_materialization"));
        builder.forbid_full_materialization();
        builder
            .start_local_custom(Arc::new(Authority::from(LocalAuthority::new_with_store(
                Arc::new(LocalAuthorityStore::new()),
            ))))
            .await
            .unwrap()
    };
    g.extend_recipe(
        ChangeList::from_str("CREATE TABLE t (col INT)", Dialect::DEFAULT_MYSQL).unwrap(),
    )
    .await
    .unwrap();
    let res = g
        .extend_recipe(
            ChangeList::from_str(
                "CREATE CACHE q FROM SELECT * FROM t",
                Dialect::DEFAULT_MYSQL,
            )
            .unwrap(),
        )
        .await;
    assert!(res.is_err());
    let err = res.err().unwrap();
    assert!(err
        .to_string()
        .contains("Creation of fully materialized query is forbidden"));
    assert!(err.caused_by_unsupported());

    shutdown_tx.shutdown().await;
}

// This test replicates the `extend_recipe` path used when we need to resnapshot. The
// snapshotted DDL may vary slightly from the DDL propagated through the replicator but
// should not cause the extend_recipe to fail.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn overwrite_with_changed_recipe() {
    let (mut g, shutdown_tx) = {
        let mut builder = Builder::for_tests();
        builder.set_sharding(Some(DEFAULT_SHARDING));
        builder
            .start_local_custom(Arc::new(Authority::from(LocalAuthority::new_with_store(
                Arc::new(LocalAuthorityStore::new()),
            ))))
            .await
            .unwrap()
    };
    g.extend_recipe(
        ChangeList::from_str("CREATE TABLE t (col INT)", Dialect::DEFAULT_MYSQL).unwrap(),
    )
    .await
    .unwrap();
    g.extend_recipe(
        ChangeList::from_str(
            "CREATE CACHE q FROM SELECT * FROM t",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let res = g
        .extend_recipe(
            ChangeList::from_str(
                "CREATE TABLE t (col INT COMMENT 'hi')",
                Dialect::DEFAULT_MYSQL,
            )
            .unwrap(),
        )
        .await;
    res.unwrap();

    shutdown_tx.shutdown().await;
}

/// Tests that whenever we have at least two workers (including the leader), and the leader dies,
/// then the recovery is successful and all the nodes are correctly materialized.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Flaky (ENG-2009)"]
async fn it_recovers_fully_materialized() {
    let authority_store = Arc::new(LocalAuthorityStore::new());

    let dir = tempfile::tempdir().unwrap();
    let persistence_params = PersistenceParameters::new(
        DurabilityMode::Permanent,
        Some("it_recovers_fully_materialized".to_string()),
        1,
        Some(dir.path().into()),
    );

    let start = || {
        let authority = Arc::new(Authority::from(LocalAuthority::new_with_store(
            authority_store.clone(),
        )));

        let mut g = Builder::for_tests();
        g.set_persistence(persistence_params.clone());
        g.set_sharding(None);
        g.start(authority)
    };
    {
        let (mut g, shutdown_tx) = start().await.unwrap();
        g.backend_ready().await;

        {
            let sql = "
                CREATE TABLE t (x INT);
                CREATE VIEW tv AS SELECT x, COUNT(*) FROM t GROUP BY x ORDER BY x;
            ";
            g.extend_recipe(ChangeList::from_str(sql, Dialect::DEFAULT_MYSQL).unwrap())
                .await
                .unwrap();

            let mut mutator = g.table("t").await.unwrap();

            for i in 1..10 {
                mutator.insert(vec![(i % 3).into()]).await.unwrap();
            }
        }

        // Let writes propagate:
        sleep().await;
        shutdown_tx.shutdown().await;
    }

    let (mut g, shutdown_tx) = start().await.unwrap();
    g.backend_ready().await;

    {
        let mut getter = g.view("tv").await.unwrap().into_reader_handle().unwrap();

        // Make sure that the new graph contains the old writes
        let result = getter.lookup(&[0.into()], true).await.unwrap().into_vec();
        assert_eq!(result.len(), 3);
        assert_eq!(
            result,
            vec![
                vec![0.into(), 3.into()],
                vec![1.into(), 3.into()],
                vec![2.into(), 3.into()],
            ]
        );
    }
    // We add more stuff, to be sure everything is still working
    {
        let mut mutator = g.table("t").await.unwrap();

        for i in 1..10 {
            mutator.insert(vec![(i % 3).into()]).await.unwrap();
        }
    }
    {
        let mut getter = g.view("tv").await.unwrap().into_reader_handle().unwrap();
        // Make sure that the new graph contains the old writes
        let result = getter.lookup(&[0.into()], true).await.unwrap().into_vec();
        assert_eq!(
            result,
            vec![
                vec![0.into(), 6.into()],
                vec![1.into(), 6.into()],
                vec![2.into(), 6.into()],
            ]
        );
    }

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn simple_drop_tables() {
    let (mut g, shutdown_tx) = start_simple_unsharded("simple_drop_tables").await;

    let create_table = "
        # base tables
        CREATE TABLE table_1 (column_1 INT);
        CREATE TABLE table_2 (column_2 INT);
        CREATE CACHE t1 FROM SELECT * FROM table_1;
    ";
    g.extend_recipe(ChangeList::from_str(create_table, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();
    g.table("table_1").await.unwrap();
    g.table("table_2").await.unwrap();
    g.view("t1").await.unwrap();

    let drop_table = "DROP TABLE table_1, table_2;";
    // let drop_table = "CREATE TABLE table_4 (column_4 INT);";
    g.extend_recipe(ChangeList::from_str(drop_table, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    sleep().await;

    assert_table_not_found(g.table("table_1").await, "table_1");
    assert_table_not_found(g.table("table_2").await, "table_2");
    assert_view_not_found(g.view("t1").await, "t1");

    let create_new_table = "CREATE TABLE table_3 (column_3 INT);";
    g.extend_recipe(ChangeList::from_str(create_new_table, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();
    g.table("table_3").await.unwrap();

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn join_drop_tables() {
    let (mut g, shutdown_tx) = start_simple_unsharded("simple_drop_tables").await;

    let create_table = "
        # base tables
        CREATE TABLE table_1 (column_1 INT, column_2 INT);
        CREATE TABLE table_2 (column_1 INT, column_2 INT);
        CREATE TABLE table_3 (column_1 INT, column_2 INT);
        CREATE CACHE t1 FROM SELECT table_1.column_1, table_3.column_1 FROM table_1 JOIN table_3 ON table_1.column_2 = table_3.column_2;
    ";
    g.extend_recipe(ChangeList::from_str(create_table, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();
    g.table("table_1").await.unwrap();
    g.table("table_2").await.unwrap();
    g.table("table_3").await.unwrap();
    g.view("t1").await.unwrap();

    let drop_table = "DROP TABLE table_1, table_2;";
    g.extend_recipe(ChangeList::from_str(drop_table, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    sleep().await;

    assert_table_not_found(g.table("table_1").await, "table_1");
    assert_table_not_found(g.table("table_2").await, "table_2");
    g.table("table_3").await.unwrap();
    assert_view_not_found(g.view("t1").await, "t1");

    let create_new_table = "CREATE TABLE table_1 (column_1 INT);";
    g.extend_recipe(ChangeList::from_str(create_new_table, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();
    g.table("table_1").await.unwrap();

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn simple_drop_tables_with_data() {
    let (mut g, shutdown_tx) = start_simple_unsharded("simple_drop_tables_with_data").await;

    let create_table = "
        # base tables
        CREATE TABLE table_1 (column_1 INT);
        CREATE TABLE table_2 (column_2 INT);
        CREATE CACHE t1 FROM SELECT * FROM table_1;
    ";
    g.extend_recipe(ChangeList::from_str(create_table, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    let mut table_1 = g.table("table_1").await.unwrap();
    table_1.insert(vec![11.into()]).await.unwrap();
    table_1.insert(vec![12.into()]).await.unwrap();

    let mut view = g.view("t1").await.unwrap().into_reader_handle().unwrap();
    eventually!(run_test: {
        view.lookup(&[0.into()], true).await.unwrap().into_vec()
    }, then_assert: |results| {
        assert!(!results.is_empty());
        assert_eq!(results[0][0], 11.into());
        assert_eq!(results[1][0], 12.into());
    });

    let drop_table = "DROP TABLE table_1, table_2;";
    g.extend_recipe(ChangeList::from_str(drop_table, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();
    assert_table_not_found(g.table("table_1").await, "table_1");
    assert_table_not_found(g.table("table_2").await, "table_2");
    assert_view_not_found(g.view("t1").await, "t1");

    let recreate_table = "CREATE TABLE table_1 (column_1 INT);";
    g.extend_recipe(ChangeList::from_str(recreate_table, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();
    g.table("table_1").await.unwrap();
    assert_view_not_found(g.view("t1").await, "t1");

    let recreate_query = "CREATE CACHE t2 FROM SELECT * FROM table_1";
    g.extend_recipe(ChangeList::from_str(recreate_query, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    sleep().await;

    let mut view = g.view("t2").await.unwrap().into_reader_handle().unwrap();
    let results = view.lookup(&[0.into()], true).await.unwrap().into_vec();
    assert!(results.is_empty());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn simple_drop_tables_with_persisted_data() {
    let mut builder = Builder::for_tests();
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    builder.set_sharding(None);
    builder.set_persistence(PersistenceParameters::new(
        DurabilityMode::Permanent,
        Some("simple_drop_tables_with_persisted_data".to_string()),
        1,
        Some(path.clone()),
    ));
    let (mut g, shutdown_tx) = builder.start_local().await.unwrap();

    let create_table = "
        # base tables
        CREATE TABLE table_1 (column_1 INT);
        CREATE TABLE table_2 (column_2 INT);
        CREATE CACHE t1 FROM SELECT * FROM table_1;
    ";
    g.extend_recipe(ChangeList::from_str(create_table, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    assert!(path.exists());

    let mut table_1_path = path.clone();
    table_1_path.push("simple_drop_tables_with_persisted_data-table_1-0.db");
    let mut table_2_path = path.clone();
    table_2_path.push("simple_drop_tables_with_persisted_data-table_2-0.db");
    eventually!(table_1_path.exists());
    eventually!(table_2_path.exists());

    let mut table_1 = g.table("table_1").await.unwrap();
    table_1.insert(vec![11.into()]).await.unwrap();
    table_1.insert(vec![12.into()]).await.unwrap();

    let mut view = g.view("t1").await.unwrap().into_reader_handle().unwrap();
    let results = view.lookup(&[0.into()], true).await.unwrap().into_vec();
    assert!(!results.is_empty());
    assert_eq!(results[0][0], 11.into());
    assert_eq!(results[1][0], 12.into());

    let drop_table = "DROP TABLE table_1, table_2;";
    g.extend_recipe(ChangeList::from_str(drop_table, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();
    assert_table_not_found(g.table("table_1").await, "table_1");
    assert_table_not_found(g.table("table_2").await, "table_2");
    assert_view_not_found(g.view("t1").await, "t1");

    eventually!(!table_1_path.exists());
    eventually!(!table_2_path.exists());

    let recreate_table = "CREATE TABLE table_1 (column_1 INT);";
    g.extend_recipe(ChangeList::from_str(recreate_table, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();
    g.table("table_1").await.unwrap();
    assert_view_not_found(g.view("t1").await, "t1");

    let recreate_query = "CREATE CACHE t2 FROM SELECT * FROM table_1";
    g.extend_recipe(ChangeList::from_str(recreate_query, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    sleep().await;

    let mut view = g.view("t2").await.unwrap().into_reader_handle().unwrap();
    let results = view.lookup(&[0.into()], true).await.unwrap();
    assert!(results.into_vec().is_empty());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn create_and_drop_table() {
    let (mut g, shutdown_tx) = start_simple_unsharded("create_and_drop_table").await;
    let create_table = "
        # base tables
        CREATE TABLE table_1 (column_1 INT);
        CREATE TABLE table_2 (column_2 INT);
        CREATE TABLE table_3 (column_3 INT);
        DROP TABLE table_1, table_2;
        CREATE TABLE table_1 (column_1 INT);
        CREATE TABLE table_4 (column_4 INT);
    ";
    g.extend_recipe(ChangeList::from_str(create_table, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    g.table("table_1").await.unwrap();
    assert_table_not_found(g.table("table_2").await, "table_2");
    g.table("table_3").await.unwrap();
    g.table("table_4").await.unwrap();

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_and_recreate_different_columns() {
    let (mut g, shutdown_tx) = start_simple_unsharded("create_and_drop_table").await;
    let create_table = "
        # base tables
        CREATE TABLE table_1 (column_1 INT);
        DROP TABLE table_1;
        CREATE TABLE table_1 (column_1 INT, column_2 INT);

        CREATE CACHE t1 FROM SELECT * FROM table_1;
    ";
    g.extend_recipe(ChangeList::from_str(create_table, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    let mut table = g.table("table_1").await.unwrap();
    table.insert(vec![11.into(), 12.into()]).await.unwrap();

    let mut view = g.view("t1").await.unwrap().into_reader_handle().unwrap();
    let results = view.lookup(&[0.into()], true).await.unwrap().into_vec();
    assert!(!results.is_empty());
    assert_eq!(results[0][0], 11.into());
    assert_eq!(results[0][1], 12.into());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn simple_dry_run() {
    let (mut g, shutdown_tx) = start_simple_unsharded("simple_dry_run").await;
    let query = "
        # base tables
        CREATE TABLE table_1 (column_1 INT);
        CREATE CACHE t1 FROM SELECT * FROM table_1;
    ";
    let res = g
        .dry_run(ChangeList::from_str(query, Dialect::DEFAULT_MYSQL).unwrap())
        .await;
    res.unwrap();
    g.table("table_1").await.unwrap_err();
    g.view("t1").await.unwrap_err();

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn simple_dry_run_unsupported() {
    let (mut g, shutdown_tx) = start_simple_unsharded("simple_dry_run").await;
    let query = "
        # base tables
        CREATE TABLE table_1 (column_1 INT);
        CREATE CACHE t1 FROM SELECT * FROM table_1;
    ";
    g.extend_recipe(ChangeList::from_str(query, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    g.table("table_1").await.unwrap();
    g.view("t1").await.unwrap();

    let unsupported_query = "CREATE CACHE FROM SELECT 1";
    let res = g
        .dry_run(ChangeList::from_str(unsupported_query, Dialect::DEFAULT_MYSQL).unwrap())
        .await;
    assert!(matches!(
        res,
        Err(RpcFailed {
            source: box SelectQueryCreationFailed {
                source: box ReadySetError::Unsupported(_),
                ..
            },
            ..
        })
    ));

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn multiple_simultaneous_migrations() {
    let (mut g, shutdown_tx) = start_simple_unsharded("multiple_simultaneous_migrations").await;
    let mut g2 = g.clone();

    g.extend_recipe(ChangeList::from_change(
        Change::CreateTable(
            parse_create_table(nom_sql::Dialect::MySQL, "CREATE TABLE t (x int, y int)").unwrap(),
        ),
        Dialect::DEFAULT_MYSQL,
    ))
    .await
    .unwrap();

    let mut t = g.table("t").await.unwrap();

    t.insert_many(vec![
        vec![DfValue::from(1), DfValue::from(2)],
        vec![DfValue::from(3), DfValue::from(4)],
    ])
    .await
    .unwrap();

    let (r1, r2) = join!(
        g.extend_recipe(ChangeList::from_change(
            Change::CreateCache(CreateCache {
                name: Some("q1".into()),
                statement: Box::new(
                    parse_select_statement(nom_sql::Dialect::MySQL, "SELECT * FROM t WHERE x = ?")
                        .unwrap()
                ),
                always: false
            }),
            Dialect::DEFAULT_MYSQL
        )),
        g2.extend_recipe(ChangeList::from_change(
            Change::CreateCache(CreateCache {
                name: Some("q2".into()),
                statement: Box::new(
                    parse_select_statement(nom_sql::Dialect::MySQL, "SELECT * FROM t WHERE y = ?")
                        .unwrap()
                ),
                always: false
            }),
            Dialect::DEFAULT_MYSQL
        ))
    );
    r1.unwrap();
    r2.unwrap();

    let mut q1 = g.view("q1").await.unwrap().into_reader_handle().unwrap();
    let mut q2 = g.view("q1").await.unwrap().into_reader_handle().unwrap();

    let res1 = q1.lookup(&[1.into()], true).await.unwrap().into_vec();
    let res2 = q2.lookup(&[3.into()], true).await.unwrap().into_vec();

    assert_eq!(res1, vec![vec![1.into(), 2.into()]]);
    assert_eq!(res2, vec![vec![3.into(), 4.into()]]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_view() {
    let (mut g, shutdown_tx) = start_simple_unsharded("drop_view").await;

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE t1 (id int);
         CREATE CACHE t1_view FROM SELECT * FROM t1;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE CACHE t1_select FROM SELECT * FROM t1_view",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    g.extend_recipe(ChangeList::from_str("DROP CACHE t1_select;", Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    g.extend_recipe(ChangeList::from_str("DROP VIEW t1_view;", Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    let select_from_view_res = g
        .extend_recipe(
            ChangeList::from_str(
                "CREATE CACHE t1_select FROM SELECT * FROM t1_view",
                Dialect::DEFAULT_MYSQL,
            )
            .unwrap(),
        )
        .await;
    let err = select_from_view_res.unwrap_err();
    assert!(err.to_string().contains("t1_view"));

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_view_schema_qualified() {
    readyset_tracing::init_test_logging();
    let (mut g, shutdown_tx) = start_simple_unsharded("drop_view_schema_qualified").await;

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE public.t1 (id int);
             CREATE VIEW public.t1_view AS SELECT * FROM public.t1;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE CACHE t1_select FROM SELECT * FROM t1_view",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap()
        .with_schema_search_path(vec!["public".into()]),
    )
    .await
    .unwrap();

    g.extend_recipe(
        ChangeList::from_str("DROP VIEW public.t1_view;", Dialect::DEFAULT_MYSQL).unwrap(),
    )
    .await
    .unwrap();

    g.view("t1_select").await.unwrap_err();

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn read_from_dropped_query() {
    let (mut g, shutdown_tx) = start_simple_unsharded("read_from_dropped_query").await;

    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE t1 (id int);
         CREATE CACHE q FROM SELECT id FROM t1;",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut view = g.view("q").await.unwrap().into_reader_handle().unwrap();

    g.extend_recipe(ChangeList::from_str("DROP TABLE t1;", Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    let view_res = view.lookup(&[0.into()], true).await;
    assert!(view_res.is_err());
    assert!(view_res.err().unwrap().caused_by_view_destroyed());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn double_create_table_with_multiple_modifications() {
    let (mut g, shutdown_tx) = start_simple_unsharded("double_create_table_add_column").await;

    let create_table = "
        # base tables
        CREATE TABLE table_1 (column_1 INT, column_2 TEXT, column_3 TEXT, column_4 TEXT);
        CREATE CACHE t1 FROM SELECT * FROM table_1;
    ";
    g.extend_recipe(ChangeList::from_str(create_table, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    let mut table = g.table("table_1").await.unwrap();
    table
        .insert(vec![11.into(), "12".into(), "13".into(), "14".into()])
        .await
        .unwrap();
    table
        .insert(vec![21.into(), "22".into(), "23".into(), "24".into()])
        .await
        .unwrap();

    let new_table = "CREATE TABLE table_1 (alternative_col_1 TEXT);";
    g.extend_recipe(ChangeList::from_str(new_table, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    // Altering a table currently means we delete and recreate the whole thing.
    g.view("t1").await.unwrap_err();

    let recreate_view = "CREATE CACHE t1 FROM SELECT * FROM table_1;";
    g.extend_recipe(ChangeList::from_str(recreate_view, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    let mut view = g.view("t1").await.unwrap().into_reader_handle().unwrap();
    let results = view.lookup(&[0.into()], true).await.unwrap().into_vec();
    assert!(results.is_empty());

    let mut table = g.table("table_1").await.unwrap();
    // This should fail as we currently have different columns than before
    table
        .insert(vec![11.into(), "12".into(), "13".into(), "14".into()])
        .await
        .unwrap_err();

    table
        .insert_many(vec![vec!["11".into()], vec!["21".into()]])
        .await
        .unwrap();

    sleep().await;
    let results = view.lookup(&[0.into()], true).await.unwrap().into_vec();
    assert!(!results.is_empty());
    assert_eq!(results[0][0], "11".into());
    assert_eq!(results[1][0], "21".into());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn double_identical_create_table() {
    let (mut g, shutdown_tx) = start_simple_unsharded("double_create_table_add_column").await;

    let create_table = "
        # base tables
        CREATE TABLE table_1 (column_1 INT);
        CREATE CACHE t1 FROM SELECT * FROM table_1;
    ";
    g.extend_recipe(ChangeList::from_str(create_table, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    let mut table = g.table("table_1").await.unwrap();
    table
        .insert_many(vec![vec![1.into()], vec![2.into()], vec![3.into()]])
        .await
        .unwrap();

    let new_table = "CREATE TABLE table_1 (column_1 INT);";
    g.extend_recipe(ChangeList::from_str(new_table, Dialect::DEFAULT_MYSQL).unwrap())
        .await
        .unwrap();

    let mut view = g.view("t1").await.unwrap().into_reader_handle().unwrap();
    let results = view.lookup(&[0.into()], true).await.unwrap().into_vec();
    assert!(!results.is_empty());
    assert_eq!(results[0][0], 1.into());
    assert_eq!(results[1][0], 2.into());
    assert_eq!(results[2][0], 3.into());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn multiple_schemas_explicit() {
    readyset_tracing::init_test_logging();
    let (mut g, shutdown_tx) = start_simple_unsharded("multiple_schemas_explicit").await;
    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE schema_1.t (id int, val int);
         CREATE TABLE schema_2.t (id int, val int);
         CREATE CACHE q FROM
         SELECT schema_1.t.val AS val1, schema_2.t.val as val2
         FROM schema_1.t JOIN schema_2.t
         ON schema_1.t.id = schema_2.t.id",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut schema_1_t = g
        .table(Relation {
            schema: Some("schema_1".into()),
            name: "t".into(),
        })
        .await
        .unwrap();
    let mut schema_2_t = g
        .table(Relation {
            schema: Some("schema_2".into()),
            name: "t".into(),
        })
        .await
        .unwrap();
    let mut q = g.view("q").await.unwrap().into_reader_handle().unwrap();

    schema_1_t
        .insert(vec![DfValue::from(1), DfValue::from("schema_1")])
        .await
        .unwrap();
    schema_2_t
        .insert(vec![DfValue::from(1), DfValue::from("schema_2")])
        .await
        .unwrap();

    eprintln!("{}", g.graphviz().await.unwrap());

    sleep().await;

    let res = q.lookup(&[0.into()], true).await.unwrap().into_vec();
    assert_eq!(
        res,
        vec![vec![DfValue::from("schema_1"), DfValue::from("schema_2")]]
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn multiple_aggregates_and_predicates() {
    let (mut g, shutdown_tx) = start_simple_unsharded("multiple_aggregates_and_predicates").await;
    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE t (a int, b int, c int);
            CREATE CACHE q FROM
                SELECT count(t.a), min(t.b) FROM t WHERE (t.a) = (t.c) AND (t.b) = (t.c)",
            Dialect::DEFAULT_MYSQL,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let mut t = g.table("t").await.unwrap();
    t.insert_many(vec![
        vec![DfValue::None, DfValue::from(5), DfValue::from(5)],
        vec![DfValue::from(4), DfValue::from(4), DfValue::from(4)],
        vec![DfValue::from(1), DfValue::from(3), DfValue::from(3)],
        vec![DfValue::from(2), DfValue::from(1), DfValue::from(2)],
    ])
    .await
    .unwrap();

    let mut q = g.view("q").await.unwrap().into_reader_handle().unwrap();
    let res = q.lookup(&[0.into()], true).await.unwrap().into_vec();
    assert_eq!(res, vec![vec![DfValue::from(1), DfValue::from(4)]]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cascade_drop_view() {
    let (mut g, shutdown_tx) = start_simple_unsharded("cascade_drop_view").await;
    g.extend_recipe(
        ChangeList::from_str(
            "CREATE TABLE t1 (a int, b int);
            CREATE TABLE t2 (c int, d int);
            CREATE TABLE t3 (e int, f int);
            CREATE VIEW v1 AS SELECT a, b FROM t1;
            CREATE VIEW v2 AS SELECT a, c FROM v1 JOIN t2 ON v1.b = t2.d;
            CREATE VIEW v3 AS SELECT a, e FROM v2 JOIN t3 ON v2.c = t3.f;
            CREATE CACHE q FROM SELECT a, c, e FROM v1 JOIN v2 ON v1.a = v2.a JOIN v3 ON v2.a = v3.a",
            Dialect::DEFAULT_MYSQL,
        )
            .unwrap(),
    )
        .await
        .unwrap();

    let mut t1 = g.table("t1").await.unwrap();
    g.table("t2").await.unwrap();
    g.table("t3").await.unwrap();
    g.view("q").await.unwrap();

    t1.insert_many(vec![
        vec![DfValue::from(1), DfValue::from(2)],
        vec![DfValue::from(3), DfValue::from(4)],
    ])
    .await
    .unwrap();

    g.extend_recipe(ChangeList::from_change(
        Change::Drop {
            name: "v2".into(),
            if_exists: false,
        },
        Dialect::DEFAULT_MYSQL,
    ))
    .await
    .unwrap();

    g.table("t1").await.unwrap();
    g.table("t2").await.unwrap();
    g.table("t3").await.unwrap();
    g.view("q").await.unwrap_err();

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn views_out_of_order() {
    let (mut g, shutdown_tx) = start_simple_unsharded("cascade_drop_view").await;
    g.extend_recipe(
        ChangeList::from_str("CREATE TABLE t1 (x int);", Dialect::DEFAULT_MYSQL).unwrap(),
    )
    .await
    .unwrap();

    // two views, one of which references the other, migrated out of order (with the referencing
    // view added before the referenced view)
    g.extend_recipe(ChangeList::from_change(
        Change::CreateView(
            parse_create_view(
                nom_sql::Dialect::MySQL,
                "CREATE VIEW v2 AS SELECT x FROM v1",
            )
            .unwrap(),
        ),
        Dialect::DEFAULT_MYSQL,
    ))
    .await
    .unwrap();

    g.extend_recipe(ChangeList::from_change(
        Change::CreateView(
            parse_create_view(
                nom_sql::Dialect::MySQL,
                "CREATE VIEW v1 AS SELECT x FROM t1",
            )
            .unwrap(),
        ),
        Dialect::DEFAULT_MYSQL,
    ))
    .await
    .unwrap();

    g.extend_recipe(ChangeList::from_change(
        Change::create_cache(
            "q",
            parse_select_statement(nom_sql::Dialect::MySQL, "SELECT x FROM v2").unwrap(),
            false,
        ),
        Dialect::DEFAULT_MYSQL,
    ))
    .await
    .unwrap();

    shutdown_tx.shutdown().await;
}
