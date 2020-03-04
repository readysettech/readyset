pub(super) trait MakeKey<A> {
    fn from_row(key: &[usize], row: &[A]) -> Self;
    fn from_key(key: &[A]) -> Self;
}

impl<A: Clone> MakeKey<A> for (A, A) {
    #[inline(always)]
    fn from_row(key: &[usize], row: &[A]) -> Self {
        debug_assert_eq!(key.len(), 2);
        (row[key[0]].clone(), row[key[1]].clone())
    }
    #[inline(always)]
    fn from_key(key: &[A]) -> Self {
        debug_assert_eq!(key.len(), 2);
        (key[0].clone(), key[1].clone())
    }
}

impl<A: Clone> MakeKey<A> for (A, A, A) {
    #[inline(always)]
    fn from_row(key: &[usize], row: &[A]) -> Self {
        debug_assert_eq!(key.len(), 3);
        (
            row[key[0]].clone(),
            row[key[1]].clone(),
            row[key[2]].clone(),
        )
    }
    #[inline(always)]
    fn from_key(key: &[A]) -> Self {
        debug_assert_eq!(key.len(), 3);
        (key[0].clone(), key[1].clone(), key[2].clone())
    }
}

impl<A: Clone> MakeKey<A> for (A, A, A, A) {
    #[inline(always)]
    fn from_row(key: &[usize], row: &[A]) -> Self {
        debug_assert_eq!(key.len(), 4);
        (
            row[key[0]].clone(),
            row[key[1]].clone(),
            row[key[2]].clone(),
            row[key[3]].clone(),
        )
    }
    #[inline(always)]
    fn from_key(key: &[A]) -> Self {
        debug_assert_eq!(key.len(), 4);
        (
            key[0].clone(),
            key[1].clone(),
            key[2].clone(),
            key[3].clone(),
        )
    }
}

impl<A: Clone> MakeKey<A> for (A, A, A, A, A) {
    #[inline(always)]
    fn from_row(key: &[usize], row: &[A]) -> Self {
        debug_assert_eq!(key.len(), 5);
        (
            row[key[0]].clone(),
            row[key[1]].clone(),
            row[key[2]].clone(),
            row[key[3]].clone(),
            row[key[4]].clone(),
        )
    }
    #[inline(always)]
    fn from_key(key: &[A]) -> Self {
        debug_assert_eq!(key.len(), 5);
        (
            key[0].clone(),
            key[1].clone(),
            key[2].clone(),
            key[3].clone(),
            key[4].clone(),
        )
    }
}

impl<A: Clone> MakeKey<A> for (A, A, A, A, A, A) {
    #[inline(always)]
    fn from_row(key: &[usize], row: &[A]) -> Self {
        debug_assert_eq!(key.len(), 6);
        (
            row[key[0]].clone(),
            row[key[1]].clone(),
            row[key[2]].clone(),
            row[key[3]].clone(),
            row[key[4]].clone(),
            row[key[5]].clone(),
        )
    }
    #[inline(always)]
    fn from_key(key: &[A]) -> Self {
        debug_assert_eq!(key.len(), 6);
        (
            key[0].clone(),
            key[1].clone(),
            key[2].clone(),
            key[3].clone(),
            key[4].clone(),
            key[5].clone(),
        )
    }
}
