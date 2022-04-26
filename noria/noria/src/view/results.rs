use noria_data::DataType;
use streaming_iterator::StreamingIterator;

use crate::ReadReplyStats;

/// This is similar to [`std::num::NonZeroUsize`] except the unused value is [`usize::MAX`], which
/// means that it will be used as a [`None`] for a wrapping [`Option`]
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
#[repr(transparent)]
#[rustc_layout_scalar_valid_range_end(18_446_744_073_709_551_614)]
struct NonMaxUsize(usize);

impl NonMaxUsize {
    fn advance(&mut self) {
        // Safe, since it will simply turn into a None if gets to MAX
        unsafe { self.0 += 1 };
    }

    fn zero() -> Self {
        // Safe, since in valid range
        unsafe { NonMaxUsize(0) }
    }
}

/// A result set from a Noria query.
#[derive(Debug, PartialEq, Eq)]
pub struct Results {
    results: Vec<Vec<DataType>>,
    /// When present, contains stats related to the operation
    pub stats: Option<ReadReplyStats>,
}

impl Results {
    // NOTE: should be pub(crate), but that triggers:
    // https://github.com/rust-lang/rust/issues/69785
    #[doc(hidden)]
    pub fn new(results: Vec<Vec<DataType>>) -> Self {
        Self {
            results,
            stats: None,
        }
    }

    #[doc(hidden)]
    pub fn with_stats(results: Vec<Vec<DataType>>, stats: ReadReplyStats) -> Self {
        Self {
            results,
            stats: Some(stats),
        }
    }

    #[doc(hidden)]
    pub fn into_data(self) -> Vec<Vec<DataType>> {
        self.results
    }
}

/// A ['StreamingIterator`] over rows of a noria select response
#[derive(Debug)]
pub enum ResultIterator {
    /// Owned results returned from noria server
    OwnedResults(OwnedResultIterator),
}

/// Iterator over owned results returned from noria server
#[derive(Debug)]
pub struct OwnedResultIterator {
    // Encapsulated data
    data: Vec<Results>,
    // Current position in the data vector
    set: Option<NonMaxUsize>,
    row: Option<NonMaxUsize>,
}

impl ResultIterator {
    /// Create from owned data
    pub fn owned(data: Vec<Results>) -> Self {
        ResultIterator::OwnedResults(OwnedResultIterator {
            data,
            set: None,
            row: None,
        })
    }

    /// Convert into a vector of [`Results`]
    pub fn into_results(self) -> Vec<Results> {
        match self {
            ResultIterator::OwnedResults(OwnedResultIterator { data, .. }) => data,
        }
    }

    /// Get aggregated stats for all results in the set
    pub fn total_stats(&self) -> Option<ReadReplyStats> {
        match self {
            ResultIterator::OwnedResults(OwnedResultIterator { data, .. }) => data
                .iter()
                .map(|r| &r.stats)
                .fold(None, |total, cur| match cur {
                    Some(stats) => Some(ReadReplyStats {
                        cache_misses: stats.cache_misses
                            + total.map(|s| s.cache_misses).unwrap_or(0),
                    }),
                    None => total,
                }),
        }
    }
}

impl StreamingIterator for OwnedResultIterator {
    type Item = [DataType];

    #[inline(always)]
    fn advance(&mut self) {
        let set = self.set.get_or_insert(NonMaxUsize::zero());
        let row = match self.row.as_mut() {
            Some(row) => {
                row.advance();
                row
            }
            None => self.row.get_or_insert(NonMaxUsize::zero()),
        };
        while let Some(rows) = self.data.get(set.0) {
            if rows.results.get(row.0).is_some() {
                break;
            }
            set.advance();
            *row = NonMaxUsize::zero();
        }
    }

    #[inline(always)]
    fn get(&self) -> Option<&Self::Item> {
        self.set
            .and_then(|set| {
                self.row
                    .and_then(|row| self.data.get(set.0).and_then(|s| s.results.get(row.0)))
            })
            .map(|v| v.as_slice())
    }
}

impl StreamingIterator for ResultIterator {
    type Item = [DataType];

    #[inline(always)]
    fn advance(&mut self) {
        match self {
            ResultIterator::OwnedResults(i) => i.advance(),
        }
    }

    #[inline(always)]
    fn get(&self) -> Option<&Self::Item> {
        match self {
            ResultIterator::OwnedResults(i) => i.get(),
        }
    }
}

impl IntoIterator for ResultIterator {
    type Item = Vec<DataType>;
    type IntoIter = impl Iterator<Item = Vec<DataType>>;

    /// Convert to an iterator over owned rows (rows are cloned)
    fn into_iter(self) -> Self::IntoIter {
        self.map_deref(|i| i.to_vec())
    }
}

impl ResultIterator {
    /// Collect the results into a vector (rows are cloned)
    pub fn into_vec(self) -> Vec<Vec<DataType>> {
        self.into_iter().collect()
    }
}

impl From<ResultIterator> for Vec<Vec<DataType>> {
    fn from(iter: ResultIterator) -> Self {
        iter.into_vec()
    }
}

#[cfg(test)]
mod test {
    use super::NonMaxUsize;

    #[test]
    fn test_usize_max() {
        assert_eq!(
            std::mem::size_of::<NonMaxUsize>(),
            std::mem::size_of::<usize>()
        );
        assert_eq!(
            std::mem::size_of::<Option<NonMaxUsize>>(),
            std::mem::size_of::<usize>()
        );

        assert!(unsafe { Some(NonMaxUsize(usize::MAX)) }.is_none());
        assert!(unsafe { Some(NonMaxUsize(usize::MAX - 1)) }.is_some());
        assert!(unsafe { Some(NonMaxUsize(0)) }.is_some());
    }
}
