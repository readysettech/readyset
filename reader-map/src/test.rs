use std::thread;
use std::time::Duration;

use super::*;

#[test]
fn default_size() {
    // By ensuring this type is a ZST, we ensure that BTreeValue is a zero-cost wrapper
    // around the wrapped value.
    assert_eq!(std::mem::size_of::<DefaultInsertionOrder>(), 0);
}

/// A helper method that calls eviction, and also collects the list of keys that got evicted and
/// returns them
fn evict<K, V, I>(w: &mut WriteHandle<K, V, I>, ratio: f64) -> Vec<K>
where
    K: Ord + Hash + Clone + 'static,
    V: Ord + Clone,
    I: InsertionOrder<V>,
{
    let mut evicted = Vec::new();

    let _ = w.evict_keys(EvictionQuantity::Ratio(ratio), |k, _| {
        evicted.push(k.clone());
        0
    });

    evicted
}

fn check_ts_mut(last: &mut u64, now: u64) {
    assert!(now > *last, "{} > {} failed", now, *last);
    *last = now;
    thread::sleep(Duration::from_millis(2));
}

fn avg_ts<I>(r: &ReadHandle<char, (char, usize), I>) -> u64
where
    I: InsertionOrder<(char, usize)>,
{
    ('a'..='z')
        .map(|x| match r.get_test(&x).unwrap() {
            None => 0,
            Some(v) => v.eviction_meta().value(),
        })
        .sum::<u64>()
        / (r.len() as u64)
}

#[test]
fn eviction_lru() {
    let (mut w, r) = Options::default()
        .with_index_type(IndexType::HashMap)
        .with_eviction_strategy(EvictionStrategy::new_lru())
        .construct();

    for (a, b) in ('a'..='z').zip(0..=26) {
        w.insert(a, (a, b));
    }
    w.publish();

    let mut last = r.get(&'a').unwrap().unwrap().eviction_meta().value();
    let orig = r.get(&'b').unwrap().unwrap().eviction_meta().value();
    thread::sleep(Duration::from_millis(2));

    let now = r.get(&'a').unwrap().unwrap().eviction_meta().value();
    check_ts_mut(&mut last, now);

    let now = r.get(&'a').unwrap().unwrap().eviction_meta().value();
    check_ts_mut(&mut last, now);

    let handle = r.enter().unwrap();
    for _ in 0..10 {
        let now = handle.get(&'a').unwrap().eviction_meta().value();
        check_ts_mut(&mut last, now);
    }
    drop(handle);

    let now = r.get_test(&'b').unwrap().unwrap().eviction_meta().value();
    assert_eq!(orig, now);

    // Access keys at different times.
    for k in 'a'..='z' {
        r.get(&k).unwrap().unwrap();
        thread::sleep(Duration::from_millis(2));
    }

    // Check that if we evict a bunch of keys, the average age goes down, i.e. that average
    // timestamp goes up.
    let old_ts = avg_ts(&r);
    evict(&mut w, 0.5);
    w.publish();
    let new_ts = avg_ts(&r);
    assert!(new_ts > old_ts);
}

fn check_ts(last: u64, now: u64) {
    assert!(now > last, "{now} > {last} failed");
    thread::sleep(Duration::from_millis(2));
}

#[test]
fn eviction_range_lru() -> Result<()> {
    let (mut w, r) = Options::default()
        .with_index_type(IndexType::BTreeMap)
        .with_eviction_strategy(EvictionStrategy::new_lru())
        .construct();

    w.insert_range('a'..='z');
    for (v, k) in ('a'..='z').enumerate() {
        w.insert(k, (k, v));
    }
    w.publish();

    let mut last = 0;
    let mut now = 0;
    thread::sleep(Duration::from_millis(2));

    for (_, v) in r.enter()?.range(&('q'..='z')).unwrap() {
        now = v.eviction_meta().value();
        check_ts(last, now);
    }
    last = now;

    for (_, v) in r.enter()?.range(&('k'..='s')).unwrap() {
        now = v.eviction_meta().value();
        check_ts(last, now);
    }
    last = now;

    for (_, v) in r.enter()?.range(&('a'..='m')).unwrap() {
        now = v.eviction_meta().value();
        check_ts(last, now);
    }
    last = now;

    for (_, v) in r.enter()?.range(&('x'..='z')).unwrap() {
        now = v.eviction_meta().value();
        check_ts(last, now);
    }

    // Check that if we evict a bunch of keys, the average age goes down, i.e. that average
    // timestamp goes up.
    let old_ts = avg_ts(&r);
    evict(&mut w, 0.5);
    w.publish();
    let new_ts = avg_ts(&r);
    assert!(new_ts > old_ts);

    Ok(())
}
