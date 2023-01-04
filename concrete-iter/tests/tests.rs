use concrete_iter::concrete_iter;

#[test]
fn returning_references() {
    #[concrete_iter]
    fn iter_slice<'a>(v: &'a [u32]) -> impl Iterator<Item = &'a u32> + 'a {
        concrete_iter!(v)
    }

    assert_eq!(iter_slice(&[1, 2, 3]).collect::<Vec<_>>(), vec![&1, &2, &3])
}
