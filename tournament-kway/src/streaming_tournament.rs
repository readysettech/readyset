use streaming_iterator::StreamingIterator;

use crate::comparator::{Comparator, GreaterComparator, LessComparator};

/// A tournament that implements [`StreamingIterator`] and merges [`StreamingIterator`]s
#[derive(Clone, Debug)]
pub struct StreamingTournament<T, C> {
    /// Array of contestants
    contestants: Vec<T>,
    /// The tree of the competition results, where an entry is the index of the winning contestant
    /// in the [`contestants`] vector, or [`None`] if no contestants left in the lane.
    /// The tree is actually a binary tree, inline in a [`Vec`] to avoid fragmentation and
    /// allocations.
    /// The last entry in the vector corresponds to the root of the tree, and holds the winner of
    /// the competition.
    tree: Vec<Option<usize>>,
    /// The comparison predicate
    comparator: C,
}

impl<T> StreamingTournament<T, LessComparator<T::Item>>
where
    T: StreamingIterator,
    T::Item: Ord,
{
    /// A tournament that rates entries from smallest to largest.
    /// The provided iterators must yeild data from smallest to largest or the results are
    /// undefined.
    pub fn from_iters_min<I: IntoIterator<Item = T>>(iters: I) -> Self {
        StreamingTournament::from_iters(iters, LessComparator::default())
    }
}

impl<T> StreamingTournament<T, GreaterComparator<T::Item>>
where
    T: StreamingIterator,
    T::Item: Ord,
{
    /// A tournament that rates entries from largest to smallest.
    /// The provided iterators must yeild data from largest to smallest or the results are
    /// undefined.
    pub fn from_iters_max<I: IntoIterator<Item = T>>(iters: I) -> Self {
        StreamingTournament::from_iters(iters, GreaterComparator::default())
    }
}

impl<T, C> StreamingTournament<T, C>
where
    T: StreamingIterator,
    C: Comparator<T::Item>,
{
    /// Create a new tournament from a set of [`StreamingTournament`]s and a custom comparator.
    /// The iterators mush have the data sorted with the same semantics as are used by the provided
    /// comparator.
    ///
    /// # Examples
    ///
    /// ```
    /// use streaming_iterator::StreamingIterator;
    /// use tournament_kway::{Comparator, StreamingTournament};
    ///
    /// struct CompareIgnoringCase {}
    ///
    /// impl Comparator<&str> for CompareIgnoringCase {
    ///     fn cmp(&self, a: &&str, b: &&str) -> core::cmp::Ordering {
    ///         a.to_lowercase().cmp(&b.to_lowercase())
    ///     }
    /// }
    ///
    /// let data = vec![vec!["aa", "bb"], vec!["AA", "BB"]];
    /// let tournament = StreamingTournament::from_iters(
    ///     data.iter().map(streaming_iterator::convert_ref),
    ///     CompareIgnoringCase {},
    /// );
    /// assert_eq!(
    ///     tournament.cloned().collect::<Vec<_>>(),
    ///     ["aa", "AA", "bb", "BB"]
    /// );
    /// ```
    pub fn from_iters<I: IntoIterator<Item = T>>(iters: I, comparator: C) -> Self {
        let contestants = iters
            .into_iter()
            .filter_map(|mut iter| {
                // Instantiate the contestants, filter out the empty ones while at it
                iter.advance();
                if iter.get().is_some() {
                    Some(iter)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        StreamingTournament {
            contestants,
            tree: vec![],
            comparator,
        }
    }

    /// "Play" the first round of games and populate the game tree
    fn build_tree(&mut self) {
        if self.contestants.is_empty() {
            return;
        }

        self.tree.reserve(self.contestants.len());

        // Building the competition tree is pretty straightforward.
        // First we play a game between adjacent pairs of competitors, if there is a competitor
        // without a pair, it automatically wins its match. This gives us the leafs of the tree.
        // After that is done we play a game between pairs of winner at each level, until we have a
        // single game that determines the overall winner.

        for i in (0..self.contestants.len()).step_by(2) {
            self.tree.push(self.play_game(Some(i), Some(i + 1)));
        }

        let mut games = self.tree.len();
        let mut pos = 0;

        while games > 1 {
            for i in (0..games).step_by(2) {
                let c0 = self.tree[pos + i];
                let c1 = if i + 1 >= games {
                    None
                } else {
                    self.tree[pos + i + 1]
                };
                self.tree.push(self.play_game(c0, c1));
            }
            // Advance the "level" pointer by the number of games in the previous level.
            pos += games;
            // The number of games at each level is half of that at the previous level, rounded up
            // for uneven number of games
            games = (games + 1) / 2;
        }
    }

    /// Decide who the winner in a game between two contestants, by applying the comparison
    /// predicate to the "score" (the next element) of each contestant.
    fn play_game(&self, p0: Option<usize>, p1: Option<usize>) -> Option<usize> {
        match (
            p0.and_then(|p0| self.contestants.get(p0).and_then(|i| i.get())),
            p1.and_then(|p1| self.contestants.get(p1).and_then(|i| i.get())),
        ) {
            (Some(c0), Some(c1)) => {
                let game_result = self.comparator.cmp(c0, c1);
                if game_result.is_lt()
                    || (
                        game_result.is_eq() && p0 < p1
                        // Not strictly needed, but prefer to keep internal order between iterators
                    )
                {
                    p0
                } else {
                    p1
                }
            }
            (None, None) => None,
            (Some(_), None) => p0,
            (None, Some(_)) => p1,
        }
    }
}

impl<T, F> StreamingIterator for StreamingTournament<T, F>
where
    T: StreamingIterator,
    F: Comparator<T::Item>,
{
    type Item = T::Item;

    fn advance(&mut self) {
        if self.tree.is_empty() {
            // Initial call to advance
            self.build_tree();
            return;
        }

        // Advance the current winner first
        let mut winner = match self.tree.last().and_then(|i| i.map(|i| i)) {
            None => return,
            Some(winner) => {
                self.contestants[winner].advance();
                winner
            }
        };

        // Then replay all the games up the chain
        // One's contestant is always its parity neighbor in the tree
        let mut contestant = winner ^ 1;

        self.tree[winner / 2] = self.play_game(Some(winner), Some(contestant));

        let mut games = (self.contestants.len() + 1) / 2;
        let mut pos = 0;
        while games > 1 {
            winner /= 2;
            // One's contestant is always its parity neighbor in the tree
            contestant = winner ^ 1;

            let new_winner = if contestant >= games {
                // Don't accidentally go into the next level
                self.play_game(self.tree[pos + winner], None)
            } else {
                self.play_game(self.tree[pos + winner], self.tree[pos + contestant])
            };

            self.tree[pos + games + winner / 2] = new_winner;

            pos += games;
            games = (games + 1) / 2;
        }
    }

    fn get(&self) -> Option<&<Self as StreamingIterator>::Item> {
        self.tree
            .last()
            .and_then(|i| i.and_then(|i| self.contestants[i].get()))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.contestants
            .iter()
            .fold((0usize, Some(0usize)), |(lower, upper), i| {
                let (l, u) = i.size_hint();
                (lower + l, upper.zip(u).map(|(u1, u2)| u1 + u2))
            })
    }
}

#[cfg(test)]
mod tests {
    use streaming_iterator::StreamingIterator;
    use test_strategy::proptest;

    use crate::StreamingTournament;

    #[proptest]
    fn test_usize_min(mut inputs: Vec<Vec<usize>>) {
        inputs.iter_mut().for_each(|i| i.sort());
        let tournament_result =
            StreamingTournament::from_iters_min(inputs.iter().map(streaming_iterator::convert_ref))
                .cloned()
                .collect::<Vec<_>>();

        let mut sort_result = inputs.iter().flatten().cloned().collect::<Vec<_>>();
        sort_result.sort();

        assert_eq!(tournament_result, sort_result);
    }

    #[proptest]
    fn test_usize_max(mut inputs: Vec<Vec<usize>>) {
        inputs.iter_mut().for_each(|i| i.sort_by(|a, b| b.cmp(a)));
        let tournament_result =
            StreamingTournament::from_iters_max(inputs.iter().map(streaming_iterator::convert_ref))
                .cloned()
                .collect::<Vec<_>>();

        let mut sort_result = inputs.iter().flatten().cloned().collect::<Vec<_>>();
        sort_result.sort_by(|a, b| b.cmp(a));

        assert_eq!(tournament_result, sort_result);
    }

    #[proptest]
    fn test_arr_min(mut inputs: Vec<Vec<[u8; 4]>>) {
        inputs.iter_mut().for_each(|i| i.sort());
        let tournament_result =
            StreamingTournament::from_iters_min(inputs.iter().map(streaming_iterator::convert_ref))
                .cloned()
                .collect::<Vec<_>>();

        let mut sort_result = inputs.iter().flatten().cloned().collect::<Vec<_>>();
        sort_result.sort();

        assert_eq!(tournament_result, sort_result);
    }

    #[proptest]
    fn test_arr_max(mut inputs: Vec<Vec<[u8; 4]>>) {
        inputs.iter_mut().for_each(|i| i.sort_by(|a, b| b.cmp(a)));
        let tournament_result =
            StreamingTournament::from_iters_max(inputs.iter().map(streaming_iterator::convert_ref))
                .cloned()
                .collect::<Vec<_>>();

        let mut sort_result = inputs.iter().flatten().cloned().collect::<Vec<_>>();
        sort_result.sort_by(|a, b| b.cmp(a));

        assert_eq!(tournament_result, sort_result);
    }
}
