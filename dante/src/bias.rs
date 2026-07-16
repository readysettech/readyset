//! Relative tag weights that scale pattern selection.
//!
//! A [`TagBias`] softly steers which patterns get drawn by default, though an
//! explicit `0.0` weight on a tag hard-excludes every candidate carrying it.
//! [`crate::compat::SelectionFilter`] remains the structural exclusion mechanism.

use std::collections::BTreeMap;

use crate::entropy::Entropy;

/// Relative weights over the tags of one dimension.
///
/// Weights are relative, not normalized. Malformed weights (negative, NaN,
/// infinite) count as `0.0`; steer with large finite weights instead.
#[derive(Debug, Clone, PartialEq)]
pub struct TagBias {
    /// Effective weight for a candidate none of whose tags carry an explicit
    /// weight. `1.0` is neutral; `0.0` restricts selection to explicitly
    /// weighted tags.
    pub default_weight: f64,
    /// Explicit per-tag weights. `0.0` excludes every candidate carrying the
    /// tag.
    pub weights: BTreeMap<String, f64>,
}

impl Default for TagBias {
    fn default() -> Self {
        Self {
            default_weight: 1.0,
            weights: BTreeMap::new(),
        }
    }
}

impl TagBias {
    /// Effective weight for a candidate carrying `tags`: the geometric mean of its
    /// explicitly weighted tags, `0.0` if any tag is weighted to zero, else `default_weight`.
    pub fn weight_for_tags(&self, tags: &[&str]) -> f64 {
        let mut product = 1.0f64;
        let mut weighted = 0usize;
        for tag in tags {
            if let Some(weight) = self.weights.get(*tag) {
                let weight = sanitize(*weight);
                if weight == 0.0 {
                    return 0.0;
                }
                product *= weight;
                weighted += 1;
            }
        }
        if weighted == 0 {
            sanitize(self.default_weight)
        } else {
            sanitize(product.powf(1.0 / weighted as f64))
        }
    }
}

/// A malformed weight (negative, NaN, infinite) counts as zero.
fn sanitize(weight: f64) -> f64 {
    if weight.is_finite() && weight > 0.0 {
        weight
    } else {
        0.0
    }
}

/// Weighted draw over `candidates` with pre-computed non-negative finite
/// `weights` (parallel slices). Returns `None` when no candidate has positive
/// weight.
pub(crate) fn choose_weighted_f64<'a, T>(
    entropy: &mut Entropy<'_>,
    candidates: &'a [T],
    weights: &[f64],
) -> Option<&'a T> {
    let total: f64 = weights.iter().sum();
    if !total.is_finite() || total <= 0.0 {
        return None;
    }
    let mut pick = entropy.range(0.0..total);
    let mut last_positive = None;
    for (candidate, weight) in candidates.iter().zip(weights) {
        if *weight <= 0.0 {
            continue;
        }
        if pick < *weight {
            return Some(candidate);
        }
        pick -= weight;
        last_positive = Some(candidate);
    }
    // Floating-point rounding can leave `pick` marginally past the last
    // positive-weight candidate; that candidate is the correct draw.
    last_positive
}

#[cfg(test)]
mod tests {
    use super::*;

    type Weights = &'static [(&'static str, f64)];

    #[test]
    fn weight_for_tags_computes_expected_weight() {
        // (default_weight, weights, tags, expected, description)
        let cases: &[(f64, Weights, &[&str], f64, &str)] = &[
            (
                1.0,
                &[],
                &["a", "b"],
                1.0,
                "neutral bias weighs a tagged candidate at default",
            ),
            (
                1.0,
                &[],
                &[],
                1.0,
                "neutral bias weighs an untagged candidate at default",
            ),
            (
                1.0,
                &[("a", 0.0), ("b", 3.0)],
                &["a", "b"],
                0.0,
                "explicit zero on any tag excludes the candidate",
            ),
            (
                1.0,
                &[("a", 0.0), ("b", 3.0)],
                &["b"],
                3.0,
                "explicit weight applies when the zero-weighted tag is absent",
            ),
            (
                1.0,
                &[("a", 2.0), ("b", 8.0)],
                &["a", "b"],
                4.0,
                "multiple explicit tags combine by geometric mean",
            ),
            (
                1.0,
                &[("a", 2.0), ("b", 8.0)],
                &["a", "unweighted"],
                2.0,
                "an unweighted tag does not dilute a weighted one",
            ),
            (
                0.0,
                &[("a", 1.5)],
                &["a", "b"],
                1.5,
                "an explicitly weighted tag applies regardless of default_weight",
            ),
            (
                0.0,
                &[("a", 1.5)],
                &["b"],
                0.0,
                "default_weight 0 excludes a candidate with no explicitly weighted tag",
            ),
            (
                0.0,
                &[("a", 1.5)],
                &[],
                0.0,
                "default_weight 0 excludes an untagged candidate",
            ),
        ];
        for (default_weight, weights, tags, expected, desc) in cases {
            let bias = TagBias {
                default_weight: *default_weight,
                weights: weights.iter().map(|(t, w)| (t.to_string(), *w)).collect(),
            };
            assert_eq!(bias.weight_for_tags(tags), *expected, "{desc}");
        }
    }

    #[test]
    fn malformed_weights_count_as_zero() {
        for bad in [-1.0, f64::NAN, f64::INFINITY] {
            let bias = TagBias {
                weights: BTreeMap::from([("a".to_string(), bad)]),
                ..TagBias::default()
            };
            assert_eq!(bias.weight_for_tags(&["a"]), 0.0, "weight {bad} must be 0");
        }
        let bias = TagBias {
            default_weight: f64::NAN,
            weights: BTreeMap::new(),
        };
        assert_eq!(bias.weight_for_tags(&["a"]), 0.0);
    }

    #[test]
    fn overflowing_geometric_mean_counts_as_zero() {
        let bias = TagBias {
            weights: BTreeMap::from([("a".to_string(), 1e200), ("b".to_string(), 1e200)]),
            ..TagBias::default()
        };
        assert_eq!(bias.weight_for_tags(&["a", "b"]), 0.0);
    }
}
