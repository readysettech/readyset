use std::cmp::Ordering;
use std::fmt::{self, Display};
use std::str::FromStr;

use fallible_iterator::FallibleIterator;
use ndarray::{ArrayBase, ArrayD, ArrayViewD, Data, IxDyn, RawData};
use nom_locate::LocatedSpan;
use nom_sql::NomSqlError;
use postgres_protocol::types::ArrayDimension;
use proptest::arbitrary::Arbitrary;
use proptest::prop_oneof;
use readyset_errors::{invalid_query_err, ReadySetError, ReadySetResult};
use serde::{Deserialize, Serialize};
use smallvec::{smallvec, SmallVec};
use tokio_postgres::types::{to_sql_checked, FromSql, IsNull, Kind, ToSql};

use crate::{DfType, DfValue, DfValueKind};

/// Internal representation of PostgreSQL arrays
///
/// PostgreSQL arrays:
///
/// 1. Are n-dimensional, but always rectangular
/// 2. Default to a lower bound of 1, but support changing dimensionality
/// 3. Are always homogeneously typed
///
/// This struct supports the first two features, but does not enforce the third.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Array {
    /// The lower bounds for each of the dimensions
    ///
    /// These values are subtracted from the corresponding indexes when doing indexing operations
    /// via [`Self::get`]
    lower_bounds: SmallVec<[i32; 2]>,

    /// The actual contents of the array, represented as a dynamically-dimensioned array (including
    /// shape)
    contents: ArrayD<DfValue>,
}

impl Array {
    /// Construct an [`Array`] from the given contents, and list of lower bounds for each of the
    /// dimensions in contents.
    ///
    /// If `lower_bounds` does not have the same length as the number of contents, returns an error
    #[inline]
    pub fn from_lower_bounds_and_contents<L>(
        lower_bounds: L,
        contents: ArrayD<DfValue>,
    ) -> ReadySetResult<Self>
    where
        L: Into<SmallVec<[i32; 2]>>,
    {
        let lower_bounds = lower_bounds.into();
        if lower_bounds.len() != contents.ndim() {
            return Err(invalid_query_err!(
                "Specified array dimensions do not match array contents",
            ));
        }

        Ok(Self {
            lower_bounds,
            contents,
        })
    }

    /// Returns the number of dimensions in the array.
    ///
    /// This function will never return 0
    #[inline]
    pub fn num_dimensions(&self) -> usize {
        self.contents.ndim()
    }

    /// Returns `true` if the array contains no elements.
    pub fn is_empty(&self) -> bool {
        self.contents.is_empty()
    }

    /// Returns the total number of elements in the array.
    pub fn total_len(&self) -> usize {
        self.contents.len()
    }

    /// Look up a value at the given index in the array, with indices supplied starting at the lower
    /// bounds provided when constructing the array.
    ///
    /// If the supplied slice of indexes has a length other than
    /// [`self.num_dimensions()`][Array::num_dimensions] or if any of the indices are out-of-bounds,
    /// returns `None`
    #[inline]
    pub fn get(&self, ixs: &[isize]) -> Option<&DfValue> {
        let ixs = ixs
            .iter()
            .zip(&*self.lower_bounds)
            .map(|(ix, lb)| usize::try_from(*ix - (*lb as isize)).ok())
            .collect::<Option<Vec<_>>>()?;

        self.contents.get(ixs.as_slice())
    }

    /// Returns an iterator over references to all the values in the array, iterating the innermost
    /// dimension first.
    pub fn values(&self) -> impl Iterator<Item = &DfValue> + '_ {
        self.contents.iter()
    }

    /// Returns an iterator over mutable references to all the values in the array, iterating the
    /// innermost dimension first
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut DfValue> + '_ {
        self.contents.iter_mut()
    }

    /// Returns an iterator over subviews of the array's outermost dimension.
    pub fn outer_dimension(&self) -> impl Iterator<Item = ArrayView> {
        self.contents
            .outer_iter()
            .map(|contents| ArrayView { contents })
    }

    /// Returns `true` if the array does not contain a mix of inferred types.
    pub fn is_homogeneous(&self) -> bool {
        let mut iter = self.values();

        let expected_type: DfType = match iter.next() {
            Some(first) => first.infer_dataflow_type(),
            None => return true,
        };

        iter.all(|v| v.infer_dataflow_type() == expected_type)
    }

    /// Coerce the values within this array to the given new member type, which can either be an
    /// arbitrarily-nested array type or a scalar type.
    pub(crate) fn coerce_to(
        &self,
        new_member_type: &DfType,
        from_member_type: &DfType,
    ) -> ReadySetResult<Self> {
        // Postgresql doesn't validate array nesting levels in type cast expressions:
        //
        // localhost/postgres=# select '[-1:0][3:4]={{1,2},{3,4}}'::int[][][];
        //            int4
        // ---------------------------
        //  [-1:0][3:4]={{1,2},{3,4}}
        // (1 row)

        let new_member_type = new_member_type.innermost_array_type();
        let from_member_type = from_member_type.innermost_array_type();

        let mut arr = self.clone();
        for v in arr.values_mut() {
            *v = v.coerce_to(new_member_type, from_member_type)?;
        }
        Ok(arr)
    }

    /// Create a [`Vec`] of [`str`] references, which are obtained by calling [`try_from`] on each
    /// of the [`DfValue`] elements contained in `self`.
    ///
    /// Useful for dealing with arrays of string values in a more easily usable form.
    pub fn to_str_vec(&self) -> ReadySetResult<Vec<&str>> {
        self.values()
            .map(<&str>::try_from)
            .collect::<Result<_, _>>()
    }
}

impl Ord for Array {
    fn cmp(&self, other: &Self) -> Ordering {
        self.contents
            .shape()
            .cmp(other.contents.shape())
            .then_with(|| self.lower_bounds.cmp(&other.lower_bounds))
            .then_with(|| self.contents.iter().cmp(other.contents.iter()))
    }
}

impl PartialOrd for Array {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Display for Array {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.lower_bounds.iter().any(|b| *b != 1) {
            for (lower_bound, len) in self.lower_bounds.iter().zip(self.contents.shape()) {
                let upper_bound = (*lower_bound as isize) + (*len as isize - 1);
                write!(f, "[{}:{}]", lower_bound, upper_bound)?;
            }
            write!(f, "=")?;
        }

        fn print_array<V>(f: &mut fmt::Formatter, arr: ArrayBase<V, IxDyn>) -> fmt::Result
        where
            V: RawData<Elem = DfValue> + Data,
        {
            write!(f, "{{")?;
            if arr.ndim() == 1 {
                for (i, val) in arr.iter().enumerate() {
                    if i != 0 {
                        write!(f, ",")?;
                    }

                    if let Ok(s) = <&str>::try_from(val) {
                        write!(f, "\"{}\"", s.replace('"', "\\\""))?;
                    } else {
                        write!(f, "{}", val)?;
                    }
                }
            } else {
                let next_level = arr.outer_iter();
                for (i, arr) in next_level.enumerate() {
                    if i != 0 {
                        write!(f, ",")?;
                    }
                    print_array(f, arr)?;
                }
            }
            write!(f, "}}")
        }

        print_array(f, self.contents.view())
    }
}

impl FromStr for Array {
    type Err = ReadySetError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mk_err = |message| ReadySetError::ArrayParseError {
            input: s.to_owned(),
            message,
        };

        let (rem, res) = parse::array(LocatedSpan::new(s.as_bytes())).map_err(|e| {
            mk_err(match e {
                nom::Err::Incomplete(n) => format!("Incomplete input; needed {:?}", n),
                nom::Err::Error(NomSqlError { input, kind })
                | nom::Err::Failure(NomSqlError { input, kind }) => {
                    format!("{:?}: at {}", kind, String::from_utf8_lossy(&input))
                }
            })
        })?;
        if !rem.is_empty() {
            return Err(mk_err("Junk after closing right brace".to_string()));
        }

        Ok(res)
    }
}

impl Arbitrary for Array {
    type Parameters = ();
    type Strategy = proptest::strategy::BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        use proptest::collection::vec;
        use proptest::prelude::*;

        (
            (1_usize..=3_usize),
            any::<DfValueKind>().prop_filter("Nested Array or PassThrough", |dtk| {
                !matches!(*dtk, DfValueKind::Array | DfValueKind::PassThrough)
            }),
        )
            .prop_flat_map(|(ndims, kind)| {
                (
                    vec(prop_oneof![Just(1i32), any::<i32>()], ndims),
                    vec(1_usize..=3usize, ndims),
                )
                    .prop_flat_map(move |(lbs, lens)| {
                        let total_elems: usize = lens.iter().copied().product();
                        vec(any_with::<DfValue>(Some(kind)), total_elems).prop_map(move |vals| {
                            Self {
                                lower_bounds: lbs.clone().into(),
                                contents: ArrayD::from_shape_vec(IxDyn(&lens), vals).unwrap(),
                            }
                        })
                    })
            })
            .boxed()
    }
}

impl From<Vec<DfValue>> for Array {
    fn from(vs: Vec<DfValue>) -> Self {
        Self {
            lower_bounds: smallvec![1], // postgres arrays start at 1
            contents: ArrayD::from_shape_vec(IxDyn(&[vs.len()]), vs).unwrap(),
        }
    }
}

impl From<ArrayD<DfValue>> for Array {
    fn from(contents: ArrayD<DfValue>) -> Self {
        Self {
            lower_bounds: smallvec![1; contents.ndim()], // postgres arrays start at 1
            contents,
        }
    }
}

impl<'a> FromSql<'a> for Array {
    fn from_sql(
        ty: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let member_type = match ty.kind() {
            Kind::Array(member) => member,
            _ => panic!("Expected array type"),
        };

        let arr = postgres_protocol::types::array_from_sql(raw)?;
        let mut lower_bounds = vec![];
        let mut lengths = vec![];
        arr.dimensions().for_each(|dim| {
            lower_bounds.push(dim.lower_bound);
            lengths.push(dim.len as usize);
            Ok(())
        })?;

        let values = arr
            .values()
            .map(|v| DfValue::from_sql_nullable(member_type, v))
            .collect::<Vec<_>>()?;

        if values.is_empty() && lengths.is_empty() {
            lengths.push(0);
            lower_bounds.push(1); // default lower bound
        }

        Ok(Array {
            lower_bounds: lower_bounds.into(),
            contents: ArrayD::from_shape_vec(IxDyn(lengths.as_slice()), values)?,
        })
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        match ty.kind() {
            Kind::Array(member) => <DfValue as FromSql>::accepts(member),
            _ => false,
        }
    }
}

impl ToSql for Array {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        let member_type = match ty.kind() {
            Kind::Array(member) => member,
            _ => panic!("Expected array type"),
        };

        postgres_protocol::types::array_to_sql(
            self.lower_bounds
                .iter()
                .zip(self.contents.shape())
                .map(|(lower_bound, length)| ArrayDimension {
                    len: *length as _,
                    lower_bound: *lower_bound,
                }),
            member_type.oid(),
            self.values(),
            |e, o| match e.to_sql(member_type, o)? {
                IsNull::Yes => Ok(postgres_protocol::IsNull::Yes),
                IsNull::No => Ok(postgres_protocol::IsNull::No),
            },
            out,
        )?;

        Ok(IsNull::No)
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool
    where
        Self: Sized,
    {
        match ty.kind() {
            Kind::Array(member) => <DfValue as ToSql>::accepts(member),
            _ => false,
        }
    }

    to_sql_checked!();
}

/// A shared view into an [`Array`] dimension.
pub struct ArrayView<'v> {
    contents: ArrayViewD<'v, DfValue>,
}

impl ArrayView<'_> {
    /// Returns an iterator over shared references to all the values in the array view, iterating
    /// the innermost dimension first.
    pub fn values(&self) -> impl Iterator<Item = &DfValue> {
        self.contents.iter()
    }
}

mod parse {
    use std::iter;

    use ndarray::{ArrayD, IxDyn};
    use nom::branch::alt;
    use nom::bytes::complete::{is_not, tag};
    use nom::character::complete::{digit1, multispace0};
    use nom::combinator::{map, map_parser, not, opt, peek};
    use nom::error::ErrorKind;
    use nom::multi::{many1, separated_list1};
    use nom::sequence::{delimited, pair, preceded, terminated, tuple};
    use nom_locate::LocatedSpan;
    use nom_sql::{embedded_literal, Dialect, NomSqlError, NomSqlResult, QuotingStyle};

    use super::Array;
    use crate::DfValue;

    enum ArrayOrNested {
        Array(Vec<DfValue>),
        Nested(Vec<ArrayOrNested>),
    }

    struct Bounds(Vec<(i32, i32)>);

    impl ArrayOrNested {
        fn shape(&self) -> Result<Vec<usize>, &'static str> {
            match self {
                ArrayOrNested::Array(vs) => Ok(vec![vs.len()]),
                ArrayOrNested::Nested(n) => {
                    let mut n_iter = n.iter();
                    let dims = match n_iter.next() {
                        Some(next) => next.shape()?,
                        None => return Ok(vec![0]),
                    };

                    for next in n_iter {
                        if next.shape()? != dims {
                            return Err(
                                "Multidimensional arrays must have sub-arrays with matching \
                                 dimensions.",
                            );
                        }
                    }

                    Ok(iter::once(n.len()).chain(dims).collect())
                }
            }
        }

        fn flatten(self) -> Vec<DfValue> {
            let mut out = vec![];
            fn flatten_inner(aon: ArrayOrNested, out: &mut Vec<DfValue>) {
                match aon {
                    ArrayOrNested::Array(vs) => out.extend(vs),
                    ArrayOrNested::Nested(ns) => {
                        for nested in ns {
                            flatten_inner(nested, out)
                        }
                    }
                }
            }
            flatten_inner(self, &mut out);
            out
        }
    }

    pub(super) fn array(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Array> {
        let fail = || {
            nom::Err::Error(NomSqlError {
                input: i,
                kind: ErrorKind::Fail,
            })
        };

        let (i, _) = multispace0(i)?;
        let (i, bounds) = opt(terminated(
            bounds,
            delimited(multispace0, tag("="), multispace0),
        ))(i)?;
        let (i, aon) = array_or_nested(i)?;
        let (i, _) = multispace0(i)?;

        let shape = aon.shape().map_err(|_e| fail())?;

        let lower_bounds = if let Some(Bounds(bounds)) = bounds {
            if bounds.len() != shape.len() {
                return Err(fail());
            }

            for ((lower, upper), actual_length) in bounds.iter().zip(&shape) {
                if *upper < *lower {
                    return Err(fail());
                }

                if ((*upper - *lower) as usize + 1) != *actual_length {
                    return Err(fail());
                }
            }

            bounds.iter().map(|(lb, _)| *lb).collect()
        } else {
            vec![1; shape.len()]
        };

        let vals = aon.flatten();

        Ok((
            i,
            Array::from_lower_bounds_and_contents(
                lower_bounds,
                ArrayD::from_shape_vec(IxDyn(&shape), vals).expect("Already validated shape"),
            )
            .expect("Already validated lower bounds"),
        ))
    }

    fn bounds(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Bounds> {
        map(
            many1(delimited(multispace0, single_dimension_bounds, multispace0)),
            Bounds,
        )(i)
    }

    fn single_dimension_bounds(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], (i32, i32)> {
        // intentionally no spaces here, as postgresql doesn't allow spaces within bounds
        let (i, _) = tag("[")(i)?;
        let (i, lower) = bound_value(i)?;
        let (i, _) = tag(":")(i)?;
        let (i, upper) = bound_value(i)?;
        let (i, _) = tag("]")(i)?;
        Ok((i, (lower, upper)))
    }

    fn bound_value(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], i32> {
        let (i, sign) = opt(tag("-"))(i)?;
        let (i, num) = map_parser(digit1, nom::character::complete::i32)(i)?;
        Ok((i, if sign.is_some() { -num } else { num }))
    }

    fn array_or_nested(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], ArrayOrNested> {
        let (i, _) = multispace0(i)?;
        let (i, _) = tag("{")(i)?;
        let (i, _) = multispace0(i)?;
        let (i, res) = alt((
            map(
                separated_list1(tuple((multispace0, tag(","), multispace0)), literal),
                ArrayOrNested::Array,
            ),
            map(
                separated_list1(tuple((multispace0, tag(","), multispace0)), array_or_nested),
                ArrayOrNested::Nested,
            ),
            map(peek(preceded(multispace0, tag("}"))), |_| {
                ArrayOrNested::Array(vec![])
            }),
        ))(i)?;
        let (i, _) = multispace0(i)?;
        let (i, _) = tag("}")(i)?;
        let (i, _) = multispace0(i)?;
        Ok((i, res))
    }

    fn literal(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], DfValue> {
        alt((
            map(
                terminated(
                    embedded_literal(Dialect::PostgreSQL, QuotingStyle::Double),
                    peek(pair(multispace0, alt((tag(","), tag("}"))))),
                ),
                |lit| {
                    DfValue::try_from(lit)
                        .expect("Only parsing literals that can be converted to DfValue")
                },
            ),
            unquoted_string_literal,
        ))(i)
    }

    fn unquoted_string_literal(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], DfValue> {
        let (i, _) = not(peek(tag("\"")))(i)?;
        map(is_not("{},\"\\"), |i: LocatedSpan<_>| DfValue::from(*i))(i)
    }
}

#[cfg(test)]
mod tests {
    use proptest::arbitrary::any;
    use proptest::strategy::Strategy;
    use readyset_util::ord_laws;
    use test_strategy::proptest;

    use super::*;

    fn non_numeric_array() -> impl Strategy<Value = Array> {
        any::<Array>().prop_filter("Numeric Array", |arr| {
            !arr.values().any(|dt| matches!(dt, DfValue::Numeric(_)))
        })
    }

    ord_laws!(
        // see [note: mixed-type-comparisons]
        #[strategy(non_numeric_array())]
        Array
    );

    #[test]
    fn from_vec() {
        let vals = vec![DfValue::from(1), DfValue::from(2), DfValue::from(3)];
        let arr = Array::from(vals.clone());
        assert_eq!(arr.num_dimensions(), 1);
        assert_eq!(arr.contents.into_raw_vec_and_offset().0, vals)
    }

    #[test]
    fn get_with_alternate_lower_bound() {
        let arr = Array {
            lower_bounds: smallvec![-5, 4],
            contents: ArrayD::from_shape_vec(
                IxDyn(&[2, 3]),
                vec![
                    // row 1
                    DfValue::from(1),
                    DfValue::from(2),
                    DfValue::from(3),
                    // row 2
                    DfValue::from(4),
                    DfValue::from(5),
                    DfValue::from(6),
                ],
            )
            .unwrap(),
        };

        assert_eq!(arr.get(&[-4, 5]), Some(&DfValue::from(5)));
    }

    #[test]
    fn print_1d_array() {
        let arr = Array::from(vec![
            DfValue::from("a"),
            DfValue::from("b"),
            DfValue::from("c"),
        ]);
        assert_eq!(arr.to_string(), r#"{"a","b","c"}"#);
    }

    #[test]
    fn print_2d_array() {
        let arr = Array::from(
            ArrayD::from_shape_vec(
                IxDyn(&[2, 2]),
                vec![
                    // row 1
                    DfValue::from(1),
                    DfValue::from(2),
                    // row 2
                    DfValue::from(3),
                    DfValue::from(4),
                ],
            )
            .unwrap(),
        );
        assert_eq!(arr.to_string(), "{{1,2},{3,4}}")
    }

    #[test]
    fn print_3d_array() {
        let arr = Array::from(
            ArrayD::from_shape_vec(
                IxDyn(&[2, 2, 1]),
                vec![
                    DfValue::from(1),
                    DfValue::from(2),
                    DfValue::from(3),
                    DfValue::from(4),
                ],
            )
            .unwrap(),
        );
        assert_eq!(arr.to_string(), "{{{1},{2}},{{3},{4}}}")
    }

    #[test]
    fn print_2d_array_with_alternate_lower_bound() {
        let arr = Array {
            lower_bounds: smallvec![-5, 4],
            contents: ArrayD::from_shape_vec(
                IxDyn(&[2, 3]),
                vec![
                    // row 1
                    DfValue::from(1),
                    DfValue::from(2),
                    DfValue::from(3),
                    // row 2
                    DfValue::from(4),
                    DfValue::from(5),
                    DfValue::from(6),
                ],
            )
            .unwrap(),
        };

        assert_eq!(arr.to_string(), "[-5:-4][4:6]={{1,2,3},{4,5,6}}");
    }

    #[test]
    fn parse_1d_int_array() {
        let arr = Array::from_str("{1,2 , 3} ").unwrap();
        assert_eq!(
            arr,
            Array::from(vec![DfValue::from(1), DfValue::from(2), DfValue::from(3)])
        );
    }

    #[test]
    #[ignore = "ENG-1416"]
    fn parse_array_big_int() {
        let arr = Array::from_str("{9223372036854775808}").unwrap();
        assert_eq!(
            arr,
            Array::from(vec![DfValue::from(9223372036854775808_u64)])
        );
    }

    #[test]
    fn parse_2d_int_array() {
        let arr = Array::from_str("{{1,2} , {3, 4 }} ").unwrap();
        assert_eq!(
            arr,
            Array::from(
                ArrayD::from_shape_vec(
                    IxDyn(&[2, 2]),
                    vec![
                        DfValue::from(1),
                        DfValue::from(2),
                        DfValue::from(3),
                        DfValue::from(4),
                    ]
                )
                .unwrap()
            )
        );
    }

    #[test]
    fn parse_2d_string_array() {
        let arr = Array::from_str(r#"{{"a","b"},  { "c" ,  "d"}}"#).unwrap();
        assert_eq!(
            arr,
            Array::from(
                ArrayD::from_shape_vec(
                    IxDyn(&[2, 2]),
                    vec![
                        DfValue::from("a"),
                        DfValue::from("b"),
                        DfValue::from("c"),
                        DfValue::from("d"),
                    ]
                )
                .unwrap()
            )
        );
    }

    #[test]
    fn parse_2d_int_array_with_alt_lower_bounds() {
        let arr = Array::from_str("[-1:0][3:4]={{1,2} , {3, 4 }} ").unwrap();
        assert_eq!(
            arr,
            Array::from_lower_bounds_and_contents(
                vec![-1, 3],
                ArrayD::from_shape_vec(
                    IxDyn(&[2, 2]),
                    vec![
                        DfValue::from(1),
                        DfValue::from(2),
                        DfValue::from(3),
                        DfValue::from(4),
                    ]
                )
                .unwrap()
            )
            .unwrap()
        );
    }

    #[proptest]
    #[ignore = "DfValue <-> Literal doesn't round trip (ENG-1416)"]
    fn display_parse_round_trip(arr: Array) {
        let s = arr.to_string();
        let res = Array::from_str(&s).unwrap();
        assert_eq!(res, arr);
    }

    #[test]
    fn parse_unquoted_string_array() {
        assert_eq!(
            "{a,b,c}".parse::<Array>().unwrap(),
            Array::from(vec![
                DfValue::from("a"),
                DfValue::from("b"),
                DfValue::from("c")
            ])
        );
    }

    #[test]
    fn parse_unquoted_numeric_string() {
        assert_eq!(
            Array::from_str("{2a, 3b}").unwrap(),
            Array::from(vec![DfValue::from("2a"), DfValue::from("3b"),])
        );
    }

    #[test]
    fn outer_dimension_3d_array() {
        let arr = Array::from(
            ArrayD::from_shape_vec(
                IxDyn(&[2, 2, 1]),
                vec![
                    DfValue::from(1),
                    DfValue::from(2),
                    DfValue::from(3),
                    DfValue::from(4),
                ],
            )
            .unwrap(),
        );

        assert_eq!(
            arr.outer_dimension()
                .map(|r| r.values().cloned().collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![
                vec![DfValue::from(1), DfValue::from(2)],
                vec![DfValue::from(3), DfValue::from(4)]
            ]
        );
    }
}
