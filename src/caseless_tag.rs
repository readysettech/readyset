/// Case-insensitive tag! variant for nom
///
/// By Paul English (https://gist.github.com/log0ymxm/d9c3fc9598cf2d92b8ae89a9ce5341d8)
///
/// This implementation has some performance issues related to extra memory allocations
/// (see https://github.com/Geal/nom/issues/251), but it works for the moment.

macro_rules! caseless_tag (
    ($i:expr, $inp: expr) => (
        {
            #[inline(always)]
            fn as_lower(b: &str) -> String {
                let s = b.to_string();
                s.to_lowercase()
            }

            let expected = $inp;
            let lower = as_lower(&expected);
            let bytes = lower.as_bytes();

            caseless_tag_bytes!($i,bytes)
        }
    );
);

macro_rules! caseless_tag_bytes (
    ($i:expr, $bytes: expr) => (
        {
            use std::cmp::min;
            let len = $i.len();
            let blen = $bytes.len();
            let m   = min(len, blen);
            let reduced = &$i[..m];

            let s = str::from_utf8(reduced).unwrap();
            let s2 = s.to_string();
            let lowered = s2.to_lowercase();
            let lowered_bytes = lowered.as_bytes();

            let b       = &$bytes[..m];

            let res: IResult<_,_> = if lowered_bytes != b {
                IResult::Error(Err::Position(ErrorKind::Tag, $i))
            } else if m < blen {
                IResult::Incomplete(Needed::Size(blen))
            } else {
                IResult::Done(&$i[blen..], reduced)
            };
            res
        }
    );
);

#[test]
fn test_caseless_tag() {
    use nom::{IResult, Err, ErrorKind, Needed};
    use std::str;

    named!(x, caseless_tag!("AbCD"));
    let r = x(&b"abcdefGH"[..]);
    assert_eq!(r, IResult::Done(&b"efGH"[..], &b"abcd"[..]));
    let r = x(&b"aBcdefGH"[..]);
    assert_eq!(r, IResult::Done(&b"efGH"[..], &b"aBcd"[..]));
}
