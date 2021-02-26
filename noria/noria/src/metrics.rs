//! Data types representing metrics dumped from a running Noria instance

pub use metrics::Key;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

/// A dumped metric's kind.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum DumpedMetricKind {
    /// Counters that can be incremented or decremented
    Counter,

    /// Gauges whose values can be explicitly set
    Gauge,
}

/// A dumped metric's value.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DumpedMetric {
    /// Labels associated with this metric value.
    pub labels: HashMap<String, String>,
    /// The actual value.
    pub value: f64,
    /// The kind of this metric.
    pub kind: DumpedMetricKind,
}

/// A dump of metrics that implements `Serialize`.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MetricsDump {
    /// The actual metrics.
    pub metrics: HashMap<String, Vec<DumpedMetric>>,
}

fn convert_key(k: Key) -> (String, HashMap<String, String>) {
    let key_data = k.into_owned();
    let (name_parts, labels) = key_data.into_parts();
    let name = name_parts.to_string();
    let labels = labels
        .into_iter()
        .map(|l| {
            let (k, v) = l.into_parts();
            (k.into_owned(), v.into_owned())
        })
        .collect();
    (name, labels)
}

impl MetricsDump {
    /// Build a [`MetricsDump`] from a map containing values for counters, and another map
    /// containing values for gauges
    pub fn from_metrics(counters: HashMap<Key, u64>, gauges: HashMap<Key, f64>) -> Self {
        let mut ret = HashMap::new();
        for (key, val) in counters.into_iter() {
            let (name, labels) = convert_key(key);
            let ent = ret.entry(name).or_insert_with(Vec::new);
            ent.push(DumpedMetric {
                labels,
                // It's going to be serialized to JSON anyway, so who cares
                value: val as f64,
                kind: DumpedMetricKind::Counter,
            });
        }
        for (key, val) in gauges.into_iter() {
            let (name, labels) = convert_key(key);
            let ent = ret.entry(name).or_insert_with(Vec::new);
            ent.push(DumpedMetric {
                labels,
                value: val,
                kind: DumpedMetricKind::Gauge,
            });
        }
        Self { metrics: ret }
    }

    /// Return the sum of all the reported values for the given metric, if present
    pub fn total<K>(&self, metric: &K) -> Option<f64>
    where
        String: Borrow<K>,
        K: Hash + Eq + ?Sized,
    {
        Some(self.metrics.get(metric)?.iter().map(|m| m.value).sum())
    }

    /// Return an iterator over all the metric keys in this [`MetricsDump`]
    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.metrics.keys()
    }
}
