#![feature(never_type)]
//! A deterministic, exhaustive, parametric generator for SQL queries, and associated DDL.
//!
//! The intent of this library is to provide a hook for generating SQL queries both
//! *deterministically*, via exhaustively iterating over all permutations of all sets of operations
//! that are supported, while also allowing *randomly* generating queries (aka "fuzz testing"),
//! permuting over parameters to operations with a larger state space.
//!
//! This serves a dual purpose:
//!
//! - Deterministically generating queries allows us to write benchmark suites that run on every
//!   commit, and give us an isolated comparative metric of how our performance changes over time
//! - Randomly generating queries and seed data allows us to generate test cases (with the
//!   `noria-logictest` crate elsewhere in the repository) to evaluate the correctness of our system
//!   and catch regressions.
//!
//! Alongside the library component of this crate is a command-line interface with a runtime for
//! running benchmarks on generated queries against noria and collecting metrics - see the
//! documentation for `main.rs` for more information.
//!
//! # Examples
//!
//! Generating a simple query, with a single query parameter and a single inner join:
//!
//! ```rust
//! use nom_sql::JoinOperator;
//! use query_generator::{GeneratorState, QueryOperation, QuerySeed};
//!
//! let mut gen = GeneratorState::default();
//! let query = gen.generate_query(QuerySeed::new(
//!     vec![
//!         QueryOperation::SingleParameter,
//!         QueryOperation::Join(JoinOperator::InnerJoin),
//!     ],
//!     vec![],
//! ));
//! let query_str = format!("{}", query.statement);
//! assert_eq!(
//!     query_str,
//!     "SELECT `table_1`.`column_2` AS `alias_1`, `table_2`.`column_2` AS `alias_2` \
//! FROM `table_1` \
//! INNER JOIN `table_2` ON (`table_1`.`column_1` = `table_2`.`column_1`) \
//! WHERE (`table_1`.`column_1` = ?)"
//! );
//! ```
//!
//! # Architecture
//!
//! - There's a [`QueryOperation`] enum which enumerates, in some sense, the individual "operations"
//!   that can be performed as part of a SQL query
//! - Each [`QueryOperation`] knows how to [add itself to a SQL query][0]
//!   - To support that, there's a [`GeneratorState`] struct, to which mutable references get passed
//!     around, which knows how to summon up [new tables][1] and [columns][2] for use in queries
//! - Many [`QueryOperation`]s have extra fields, such as [`QueryOperation::TopK::limit`], which are
//!   hardcoded when exhaustively permuting combinations of operations, but allowed to be generated
//!   *randomly* when generating random queries via the [`Arbitrary`] impl
//! - The set of [`QueryOperation`]s for a query, plus the set of [`Subquery`]s that that query
//!   contains, are wrapped up together into a [`QuerySeed`] struct, which is passed to
//!   [`GeneratorState::generate_query`] to actually generate a SQL query
//!
//! [0]: QueryOperation::add_to_query
//! [1]: GeneratorState::fresh_table_mut
//! [2]: TableSpec::fresh_column
//! [3]: QueryOperation::permute

mod distribution_annotation;

use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::convert::{TryFrom, TryInto};
use std::error::Error;
use std::hash::Hash;
use std::iter::{self, FromIterator};
use std::ops::{Bound, DerefMut};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::anyhow;
use bit_vec::BitVec;
use chrono::{FixedOffset, NaiveDate, NaiveTime, TimeZone};
use clap::Parser;
use derive_more::{Display, From, Into};
pub use distribution_annotation::DistributionAnnotation;
use eui48::{MacAddress, MacAddressFormat};
use itertools::{Either, Itertools};
use launchpad::intervals::{BoundPair, IterBoundPair};
use lazy_static::lazy_static;
use nom_sql::analysis::{contains_aggregate, ReferredColumns};
use nom_sql::{
    BinaryOperator, Column, ColumnConstraint, ColumnSpecification, CommonTableExpression,
    CreateTableStatement, Expression, FieldDefinitionExpression, FunctionExpression, InValue,
    ItemPlaceholder, JoinClause, JoinConstraint, JoinOperator, JoinRightSide, LimitClause, Literal,
    OrderClause, OrderType, SelectStatement, SqlType, Table, TableKey,
};
use noria_data::DataType;
use parking_lot::Mutex;
use proptest::arbitrary::{any, any_with, Arbitrary};
use proptest::strategy::{BoxedStrategy, Strategy};
use rand::distributions::{Distribution, Standard};
use rand::Rng;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use strum::IntoEnumIterator;
use strum_macros::EnumIter;
use test_strategy::Arbitrary;
use zipf::ZipfDistribution;

/// Generate a constant value with the given [`SqlType`]
///
/// The following SqlTypes do not have a representation as a [`DataType`] and will panic if passed:
///
/// - [`SqlType::Date`]
/// - [`SqlType::Enum`]
/// - [`SqlType::Bool`]
fn value_of_type(typ: &SqlType) -> DataType {
    match typ {
        SqlType::Char(_)
        | SqlType::Varchar(_)
        | SqlType::Blob
        | SqlType::Longblob
        | SqlType::Mediumblob
        | SqlType::Tinyblob
        | SqlType::Tinytext
        | SqlType::Mediumtext
        | SqlType::Longtext
        | SqlType::Text
        | SqlType::Binary(_)
        | SqlType::Varbinary(_) => {
            // It is safe to transform an "a" String into a DataType.
            #[allow(clippy::unwrap_used)]
            DataType::try_from("a").unwrap()
        }
        SqlType::ByteArray => {
            // Zero is an interesting value, because it can only occur for
            // byte arrays, since character strings don't allow zero
            // octets.
            DataType::ByteArray(Arc::new(vec![0u8]))
        }
        SqlType::Int(_) => 1i32.into(),
        SqlType::Bigint(_) => 1i64.into(),
        SqlType::UnsignedInt(_) | SqlType::Serial => 1u32.into(),
        SqlType::UnsignedBigint(_) | SqlType::BigSerial => 1u64.into(),
        SqlType::Tinyint(_) => 1i8.into(),
        SqlType::UnsignedTinyint(_) => 1u8.into(),
        SqlType::Smallint(_) => 1i16.into(),
        SqlType::UnsignedSmallint(_) => 1u16.into(),
        SqlType::Double | SqlType::Float | SqlType::Real | SqlType::Decimal(_, _) => {
            1.5.try_into().unwrap()
        }
        SqlType::Numeric(_) => DataType::from(Decimal::new(15, 1)),
        SqlType::DateTime(_) | SqlType::Timestamp => {
            NaiveDate::from_ymd(2020, 1, 1).and_hms(12, 30, 45).into()
        }
        SqlType::TimestampTz => DataType::from(
            FixedOffset::west(18_000)
                .ymd(2020, 1, 1)
                .and_hms(12, 30, 45),
        ),
        SqlType::Time => NaiveTime::from_hms(12, 30, 45).into(),
        SqlType::Date => NaiveDate::from_ymd(2020, 1, 1).into(),
        SqlType::Bool => 1i32.into(),
        SqlType::Enum(_) => unimplemented!(),
        SqlType::Json | SqlType::Jsonb => "{}".into(),
        SqlType::MacAddr => "01:23:45:67:89:AF".into(),
        SqlType::Uuid => "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11".into(),
        SqlType::Bit(size_opt) => {
            DataType::from(BitVec::with_capacity(size_opt.unwrap_or(1) as usize))
        }
        SqlType::Varbit(_) => DataType::from(BitVec::new()),
    }
}

/// Generate a random value with the given [`SqlType`]. The length of the value
/// is pulled from a uniform distribution over the set of possible ranges.
///
/// The following SqlTypes do not have a representation as a [`DataType`] and will panic if passed:
///
/// - [`SqlType::Date`]
/// - [`SqlType::Enum`]
/// - [`SqlType::Bool`]
fn random_value_of_type(typ: &SqlType) -> DataType {
    let mut rng = rand::thread_rng();
    match typ {
        SqlType::Char(Some(x)) | SqlType::Varchar(Some(x)) => {
            let length: usize = rng.gen_range(1..=*x).into();
            // It is safe to transform an String of consecutive a's into a DataType.
            #[allow(clippy::unwrap_used)]
            DataType::try_from("a".repeat(length)).unwrap()
        }
        SqlType::Tinyblob | SqlType::Tinytext => {
            // 2^8 bytes
            let length: usize = rng.gen_range(1..256);
            // It is safe to transform an String of consecutive a's into a DataType.
            #[allow(clippy::unwrap_used)]
            DataType::try_from("a".repeat(length)).unwrap()
        }
        SqlType::Blob
        | SqlType::Text
        | SqlType::Char(None)
        | SqlType::Varchar(None)
        | SqlType::Binary(None) => {
            // 2^16 bytes
            let length: usize = rng.gen_range(1..65536);
            // It is safe to transform an String of consecutive a's into a DataType.
            #[allow(clippy::unwrap_used)]
            DataType::try_from("a".repeat(length)).unwrap()
        }
        SqlType::Mediumblob | SqlType::Mediumtext => {
            // 2^24 bytes
            // Currently capped at 65536 as these are generated in memory.
            let length: usize = rng.gen_range(1..65536);
            // It is safe to transform an String of consecutive a's into a DataType.
            #[allow(clippy::unwrap_used)]
            DataType::try_from("a".repeat(length)).unwrap()
        }
        SqlType::Longblob | SqlType::Longtext => {
            // 2^32 bytes
            // Currently capped at 65536 as these are generated in memory.
            let length: usize = rng.gen_range(1..65536);
            // It is safe to transform an String of consecutive a's into a DataType.
            #[allow(clippy::unwrap_used)]
            DataType::try_from("a".repeat(length)).unwrap()
        }
        SqlType::Binary(Some(x)) | SqlType::Varbinary(x) => {
            // Convert to bytes and generate string data to match.
            let length: usize = rng.gen_range(1..*x / 8).into();
            // It is safe to transform an String of consecutive a's into a DataType.
            #[allow(clippy::unwrap_used)]
            DataType::try_from("a".repeat(length)).unwrap()
        }
        SqlType::ByteArray => {
            let length = rng.gen_range(1..10);
            let mut array = Vec::new();
            for _ in 0..length {
                array.push(rng.gen::<u8>());
            }
            DataType::ByteArray(Arc::new(array))
        }
        SqlType::Int(_) => rng.gen::<i32>().into(),
        SqlType::Bigint(_) => rng.gen::<i64>().into(),
        SqlType::UnsignedInt(_) => rng.gen::<u32>().into(),
        SqlType::UnsignedBigint(_) => rng.gen::<u32>().into(),
        SqlType::Tinyint(_) => rng.gen::<i8>().into(),
        SqlType::UnsignedTinyint(_) => rng.gen::<u8>().into(),
        SqlType::Smallint(_) => rng.gen::<i16>().into(),
        SqlType::UnsignedSmallint(_) => rng.gen::<u16>().into(),
        SqlType::Double | SqlType::Float | SqlType::Real | SqlType::Decimal(_, _) => {
            1.5.try_into().unwrap()
        }
        SqlType::Numeric(_) => DataType::from(Decimal::new(15, 1)),
        SqlType::DateTime(_) | SqlType::Timestamp => {
            // Generate a random month and day within the same year.
            NaiveDate::from_ymd(2020, rng.gen_range(1..12), rng.gen_range(1..28))
                .and_hms(12, 30, 45)
                .into()
        }
        SqlType::TimestampTz => DataType::from(
            FixedOffset::west(18_000)
                .ymd(2020, rng.gen_range(1..12), rng.gen_range(1..28))
                .and_hms(12, 30, 45),
        ),
        SqlType::Time => NaiveTime::from_hms(12, 30, 45).into(),
        SqlType::Date => {
            NaiveDate::from_ymd(2020, rng.gen_range(1..12), rng.gen_range(1..28)).into()
        }
        SqlType::Bool => DataType::from(rng.gen_bool(0.5)),
        SqlType::Enum(_) => unimplemented!(),
        SqlType::Json | SqlType::Jsonb => DataType::from(format!(
            "{{\"k\":\"{}\"}}",
            "a".repeat(rng.gen_range(1..255))
        )),
        SqlType::MacAddr => {
            let mut bytes = [0_u8; 6];
            rng.fill(&mut bytes);
            // We know the length and format of the bytes, so this should always be parsable as a
            // `MacAddress`.
            #[allow(clippy::unwrap_used)]
            DataType::from(
                MacAddress::from_bytes(&bytes[..])
                    .unwrap()
                    .to_string(MacAddressFormat::HexString),
            )
        }
        SqlType::Uuid => {
            let mut bytes = [0_u8, 16];
            rng.fill(&mut bytes);
            // We know the length and format of the bytes, so this should always be parsable as a
            // `UUID`.
            #[allow(clippy::unwrap_used)]
            DataType::from(uuid::Uuid::from_slice(&bytes[..]).unwrap().to_string())
        }
        SqlType::Bit(size_opt) => DataType::from(BitVec::from_iter(
            rng.sample_iter(Standard)
                .take(size_opt.unwrap_or(1) as usize)
                .collect::<Vec<bool>>(),
        )),
        SqlType::Varbit(max_size) => {
            let size = rng.gen_range(0..max_size.unwrap_or(u16::MAX));
            DataType::from(BitVec::from_iter(
                rng.sample_iter(Standard)
                    .take(size as usize)
                    .collect::<Vec<bool>>(),
            ))
        }
        SqlType::Serial => (rng.gen::<u32>() + 1).into(),
        SqlType::BigSerial => (rng.gen::<u64>() + 1).into(),
    }
}

/// Generate a random value from a uniform distribution with the given integer
/// [`SqlType`] for a given range of values.If the range of `min` and `max`
/// exceeds the storage of the type, this truncates to fit.
fn uniform_random_value(min: &DataType, max: &DataType) -> DataType {
    let mut rng = rand::thread_rng();
    match (min, max) {
        (DataType::Int(i), DataType::Int(j)) => rng.gen_range(*i..*j).into(),
        (DataType::UnsignedInt(i), DataType::UnsignedInt(j)) => rng.gen_range(*i..*j).into(),
        (_, _) => unimplemented!("DataTypes unsupported for random uniform value generation"),
    }
}

/// Generate a unique value with the given [`SqlType`] from a monotonically increasing counter,
/// `idx`.
///
/// This is an injective function (from `(idx, typ)` to the resultant [`DataType`]).
///
/// The following SqlTypes do not have a representation as a [`DataType`] and will panic if passed:
///
/// - [`SqlType::Date`]
/// - [`SqlType::Enum`]
/// - [`SqlType::Bool`]
fn unique_value_of_type(typ: &SqlType, idx: u32) -> DataType {
    match typ {
        SqlType::Char(_)
        | SqlType::Varchar(_)
        | SqlType::Blob
        | SqlType::Longblob
        | SqlType::Mediumblob
        | SqlType::Tinyblob
        | SqlType::Tinytext
        | SqlType::Mediumtext
        | SqlType::Longtext
        | SqlType::Text
        | SqlType::Binary(_)
        | SqlType::Varbinary(_) => {
            // It is safe to transform an u32 String representation into a DataType.
            #[allow(clippy::unwrap_used)]
            DataType::try_from(format!("{}", idx)).unwrap()
        }
        SqlType::Int(_) => (idx as i32).into(),
        SqlType::Bigint(_) => (idx as i64).into(),
        SqlType::UnsignedInt(_) => (idx as u32).into(),
        SqlType::UnsignedBigint(_) => (idx as u64).into(),
        SqlType::Tinyint(_) => (idx as i8).into(),
        SqlType::UnsignedTinyint(_) => (idx).into(),
        SqlType::Smallint(_) => (idx as i16).into(),
        SqlType::UnsignedSmallint(_) => (idx as u16).into(),
        SqlType::Double | SqlType::Float | SqlType::Real | SqlType::Decimal(_, _) => {
            (1.5 + idx as f64).try_into().unwrap()
        }
        SqlType::Numeric(_) => DataType::from(Decimal::new((15 + idx) as i64, 2)),
        SqlType::DateTime(_) | SqlType::Timestamp => NaiveDate::from_ymd(2020, 1, 1)
            .and_hms(12, idx as _, 30)
            .into(),
        SqlType::TimestampTz => DataType::from(
            FixedOffset::west(18_000)
                .ymd(2020, 1, 1)
                .and_hms(12, idx as _, 30),
        ),
        SqlType::Date => unimplemented!(),
        SqlType::Enum(_) => unimplemented!(),
        SqlType::Bool => unimplemented!(),
        SqlType::ByteArray => unimplemented!(),
        SqlType::Time => NaiveTime::from_hms(12, idx as _, 30).into(),
        SqlType::Json | SqlType::Jsonb => DataType::from(format!("{{\"k\": {}}}", idx)),
        SqlType::MacAddr => {
            let b1: u8 = ((idx >> 24) & 0xff) as u8;
            let b2: u8 = ((idx >> 16) & 0xff) as u8;
            let b3: u8 = ((idx >> 8) & 0xff) as u8;
            let b4: u8 = (idx & 0xff) as u8;
            let bytes = [b1, b2, b3, b4, u8::MIN, u8::MAX];
            // We know the length and format of the bytes, so this should always be parsable as a
            // `MacAddress`.
            #[allow(clippy::unwrap_used)]
            DataType::from(
                MacAddress::from_bytes(&bytes[..])
                    .unwrap()
                    .to_string(MacAddressFormat::HexString),
            )
        }
        SqlType::Uuid => {
            let mut bytes = [u8::MAX; 16];
            bytes[0] = ((idx >> 24) & 0xff) as u8;
            bytes[1] = ((idx >> 16) & 0xff) as u8;
            bytes[2] = ((idx >> 8) & 0xff) as u8;
            bytes[3] = (idx & 0xff) as u8;
            // We know the length and format of the bytes, so this should always be parsable as a
            // `UUID`.
            #[allow(clippy::unwrap_used)]
            DataType::from(uuid::Uuid::from_slice(&bytes[..]).unwrap().to_string())
        }
        SqlType::Bit(_) | SqlType::Varbit(_) => {
            let mut bytes = [u8::MAX; 4];
            bytes[0] = ((idx >> 24) & 0xff) as u8;
            bytes[1] = ((idx >> 16) & 0xff) as u8;
            bytes[2] = ((idx >> 8) & 0xff) as u8;
            bytes[3] = (idx & 0xff) as u8;
            DataType::from(BitVec::from_bytes(&bytes[..]))
        }
        SqlType::Serial => (idx + 1).into(),
        SqlType::BigSerial => ((idx + 1) as u64).into(),
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash, From, Into, Display, Clone)]
#[repr(transparent)]
pub struct TableName(String);

impl Borrow<String> for TableName {
    fn borrow(&self) -> &String {
        &self.0
    }
}

impl Borrow<str> for TableName {
    fn borrow(&self) -> &str {
        self.0.as_str()
    }
}

impl From<TableName> for Table {
    fn from(name: TableName) -> Self {
        Table {
            name: name.into(),
            alias: None,
            schema: None,
        }
    }
}

impl<'a> From<&'a TableName> for &'a str {
    fn from(tn: &'a TableName) -> Self {
        &tn.0
    }
}

impl From<&str> for TableName {
    fn from(tn: &str) -> Self {
        TableName(tn.into())
    }
}

impl FromStr for TableName {
    type Err = !;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::from(s))
    }
}

#[derive(
    Debug, Eq, PartialEq, Ord, PartialOrd, Hash, From, Into, Display, Clone, Serialize, Deserialize,
)]
#[repr(transparent)]
pub struct ColumnName(String);

impl From<ColumnName> for Column {
    fn from(name: ColumnName) -> Self {
        Self {
            name: name.into(),
            table: None,
        }
    }
}

impl From<&str> for ColumnName {
    fn from(col: &str) -> Self {
        Self(col.into())
    }
}

impl FromStr for ColumnName {
    type Err = !;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::from(s))
    }
}

impl From<nom_sql::Column> for ColumnName {
    fn from(col: nom_sql::Column) -> Self {
        col.name.into()
    }
}

/// Try to find the [`ColumnSpecification`] for the primary key of the given create table statement
///
/// TODO(grfn): Ideally, this would reuse the `key_def_coalescing` rewrite pass, but that's buried
/// deep inside noria-server - if we ever get a chance to extract rewrite passes to their own crate,
/// this should be updated to use that
pub fn find_primary_keys(stmt: &CreateTableStatement) -> Option<&ColumnSpecification> {
    stmt.fields
        .iter()
        // Look for a column with a PRIMARY KEY constraint on the spec first
        .find(|f| {
            f.constraints
                .iter()
                .any(|c| *c == ColumnConstraint::PrimaryKey)
        })
        // otherwise, find a column corresponding to a standalone PRIMARY KEY table constraint
        .or_else(|| {
            stmt.keys
                .iter()
                .flatten()
                .find_map(|k| match k {
                    // TODO(grfn): This doesn't support compound primary keys
                    TableKey::PrimaryKey { columns, .. } => columns.first(),
                    _ => None,
                })
                .and_then(|col| stmt.fields.iter().find(|f| f.column == *col))
        })
}

/// Variants and their parameters used to construct
/// their respective ColumnGenerator.
#[derive(Debug, PartialEq, Clone)]
pub enum ColumnGenerationSpec {
    /// Generates a unique value for every row.
    Unique,
    /// Generates a unique value starting at an index.
    UniqueFrom(u32),
    /// Generates a new unique value every n rows.
    UniqueRepeated(u32),
    /// Generates an integer in the specified range.
    Uniform(DataType, DataType),
    /// Non-repeating Uniform, an optional batch size can be specified to
    /// reset the distribution after n rows are generated.
    ///
    /// As this repeatedly pulls from a uniform distribution until we
    /// receive a value we have not yet seen in a batch, the batch
    /// size should be much smaller than the size of the distribution.
    UniformWithoutReplacement {
        min: DataType,
        max: DataType,
        batch_size: Option<u32>,
    },
    /// Generates a random value for the row.
    Random,
    /// Generate a random string from a regex
    RandomString(String),
    /// Generates an integer in the specified range. Cannot be used for
    /// non discrete integer DataTypes.
    Zipfian {
        min: DataType,
        max: DataType,
        alpha: f64,
    },
}

impl ColumnGenerationSpec {
    pub fn generator_for_col(&self, col_type: SqlType) -> ColumnGenerator {
        match self {
            ColumnGenerationSpec::Unique => ColumnGenerator::Unique(col_type.into()),
            ColumnGenerationSpec::UniqueFrom(index) => {
                ColumnGenerator::Unique(UniqueGenerator::new(col_type, *index, 1))
            }
            ColumnGenerationSpec::UniqueRepeated(n) => {
                ColumnGenerator::Unique(UniqueGenerator::new(col_type, 0, *n))
            }
            ColumnGenerationSpec::Uniform(a, b) => ColumnGenerator::Uniform(UniformGenerator {
                min: a.clone(),
                max: b.clone(),
                with_replacement: true,
                batch_size: None,
                pulled: HashSet::new(),
            }),
            ColumnGenerationSpec::UniformWithoutReplacement {
                min: a,
                max: b,
                batch_size: opt_n,
            } => ColumnGenerator::Uniform(UniformGenerator {
                min: a.clone(),
                max: b.clone(),
                with_replacement: false,
                batch_size: *opt_n,
                pulled: HashSet::new(),
            }),
            ColumnGenerationSpec::Random => ColumnGenerator::Random(col_type.into()),
            ColumnGenerationSpec::RandomString(r) => ColumnGenerator::RandomString(r.into()),
            ColumnGenerationSpec::Zipfian { min, max, alpha } => {
                ColumnGenerator::Zipfian(ZipfianGenerator::new(min.clone(), max.clone(), *alpha))
            }
        }
    }
}

/// Method to use to generate column information.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnGenerator {
    /// Repeatedly returns a single constant value.
    Constant(ConstantGenerator),
    /// Returns a unique value. For integer types this is a
    /// 0-indexed incrementing value.
    Unique(UniqueGenerator),
    /// Returns a randomly generated value between a min and
    /// max value.
    Uniform(UniformGenerator),
    /// Returns a random value.
    Random(RandomGenerator),
    /// Returns a random string from a regex
    RandomString(RandomStringGenerator),
    /// Returns a value generated from a zipfian distribution.
    Zipfian(ZipfianGenerator),
    /// Generate a unique value for every row from a non unique generator
    NonRepeating(NonRepeatingGenerator),
}

impl ColumnGenerator {
    pub fn gen(&mut self) -> DataType {
        match self {
            ColumnGenerator::Constant(g) => g.gen(),
            ColumnGenerator::Unique(g) => g.gen(),
            ColumnGenerator::Uniform(g) => g.gen(),
            ColumnGenerator::Random(g) => g.gen(),
            ColumnGenerator::RandomString(g) => g.gen(),
            ColumnGenerator::Zipfian(g) => g.gen(),
            ColumnGenerator::NonRepeating(g) => g.gen(),
        }
    }
}

impl ColumnGenerator {
    fn into_unique(self) -> Self {
        match self {
            ColumnGenerator::Constant(_) => panic!("Can't make unique over Constant"),
            u @ ColumnGenerator::Unique(_) | u @ ColumnGenerator::NonRepeating(_) => u, /* nothing to do */
            u @ ColumnGenerator::Uniform(_)
            | u @ ColumnGenerator::Zipfian(_)
            | u @ ColumnGenerator::Random(_)
            | u @ ColumnGenerator::RandomString(_) => {
                ColumnGenerator::NonRepeating(NonRepeatingGenerator {
                    generator: Box::new(u),
                    generated: growable_bloom_filter::GrowableBloom::new(0.01, 1_000_000),
                })
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct ConstantGenerator {
    value: DataType,
}

#[derive(Debug, Clone)]
pub struct RandomStringGenerator {
    regex: String,
    inner: rand_regex::Regex,
}

impl Eq for RandomStringGenerator {}

impl PartialEq for RandomStringGenerator {
    fn eq(&self, other: &Self) -> bool {
        self.regex == other.regex
    }
}

impl<'a, S: AsRef<str>> From<S> for RandomStringGenerator {
    fn from(s: S) -> Self {
        let s = s.as_ref();
        Self {
            regex: s.to_string(),
            inner: rand_regex::Regex::compile(s, 256).unwrap(),
        }
    }
}

impl RandomStringGenerator {
    fn gen(&self) -> DataType {
        let val: String = rand::thread_rng().sample(&self.inner);
        val.into()
    }
}

impl From<SqlType> for ConstantGenerator {
    fn from(t: SqlType) -> Self {
        Self {
            value: value_of_type(&t),
        }
    }
}

impl From<DataType> for ConstantGenerator {
    fn from(value: DataType) -> Self {
        Self { value }
    }
}

impl ConstantGenerator {
    fn gen(&self) -> DataType {
        self.value.clone()
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct UniqueGenerator {
    /// The number of values we have generated in this generator so far.
    generated: u32,
    /// The current index to use to generate the random value. Incremented
    /// every batch_size.
    index: u32,
    /// The number of values to generate before incrementing `index`.
    batch_size: u32,
    sql_type: SqlType,
}

impl UniqueGenerator {
    fn new(sql_type: SqlType, index: u32, batch_size: u32) -> Self {
        Self {
            generated: 0,
            index,
            batch_size,
            sql_type,
        }
    }
}

impl From<SqlType> for UniqueGenerator {
    fn from(t: SqlType) -> Self {
        UniqueGenerator::new(t, 0, 1)
    }
}

impl UniqueGenerator {
    fn gen(&mut self) -> DataType {
        let val = unique_value_of_type(&self.sql_type, self.index);
        self.generated += 1;
        if self.generated % self.batch_size == 0 {
            self.index += 1;
        }
        val
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct UniformGenerator {
    min: DataType,
    max: DataType,
    /// Whether we should replace values within the uniform distribution.
    with_replacement: bool,
    /// The number of values to generate before resetting the
    /// distribution. Only relevant if `with_replacement` is true.
    batch_size: Option<u32>,

    /// Values we have already pulled from the uniform distribution
    /// if we are not replacing values.
    pulled: HashSet<DataType>,
}

impl UniformGenerator {
    fn gen(&mut self) -> DataType {
        if self.with_replacement {
            uniform_random_value(&self.min, &self.max)
        } else {
            let mut val = uniform_random_value(&self.min, &self.max);
            let mut iters = 0;
            while self.pulled.contains(&val) {
                val = uniform_random_value(&self.min, &self.max);
                iters += 1;

                assert!(
                    iters <= 100000,
                    "Too many iterations when trying to generate a single random value"
                );
            }
            self.pulled.insert(val.clone());

            // If this is the last value in a batch, reset the values we have
            // seen to start a new batch.
            if let Some(batch) = self.batch_size {
                if self.pulled.len() as u32 == batch {
                    self.pulled = HashSet::new();
                }
            }

            val
        }
    }
}

#[derive(Debug, Clone)]
pub struct ZipfianGenerator {
    min: DataType,
    max: DataType,
    alpha: f64,
    dist: ZipfDistribution,
}

impl ZipfianGenerator {
    fn new(min: DataType, max: DataType, alpha: f64) -> Self {
        let num_elements: u64 = match (&min, &max) {
            (DataType::Int(i), DataType::Int(j)) => (j - i) as u64,
            (DataType::UnsignedInt(i), DataType::UnsignedInt(j)) => (j - i) as u64,
            (_, _) => unimplemented!("DataTypes unsupported for discrete zipfian value generation"),
        };

        Self {
            min,
            max,
            alpha,
            dist: zipf::ZipfDistribution::new(num_elements as usize, alpha).unwrap(),
        }
    }

    fn gen(&mut self) -> DataType {
        let mut rng = rand::thread_rng();
        let offset = self.dist.sample(&mut rng);

        match self.min {
            DataType::Int(i) => DataType::Int(i + offset as i64),
            DataType::UnsignedInt(i) => DataType::UnsignedInt(i + offset as u64),
            _ => unimplemented!("DataType unsupported for discrete zipfian value generation."),
        }
    }
}

impl PartialEq for ZipfianGenerator {
    fn eq(&self, other: &Self) -> bool {
        self.min == other.min && self.max == other.max && self.alpha == other.alpha
    }
}

impl Eq for ZipfianGenerator {}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct RandomGenerator {
    sql_type: SqlType,
}

impl From<SqlType> for RandomGenerator {
    fn from(sql_type: SqlType) -> Self {
        Self { sql_type }
    }
}

impl RandomGenerator {
    pub fn gen(&self) -> DataType {
        random_value_of_type(&self.sql_type)
    }
}

#[derive(Debug, Clone)]
pub struct NonRepeatingGenerator {
    generator: Box<ColumnGenerator>,
    generated: growable_bloom_filter::GrowableBloom,
}

impl Eq for NonRepeatingGenerator {}

impl PartialEq for NonRepeatingGenerator {
    fn eq(&self, other: &Self) -> bool {
        self.generator == other.generator
    }
}

impl NonRepeatingGenerator {
    fn gen(&mut self) -> DataType {
        let mut reps = 0;
        loop {
            let d = match &mut *self.generator {
                ColumnGenerator::Uniform(u) => u.gen(),
                ColumnGenerator::Zipfian(z) => z.gen(),
                ColumnGenerator::Random(r) => r.gen(),
                ColumnGenerator::RandomString(r) => r.gen(),
                ColumnGenerator::Unique(_) => panic!("Non repeating over Unique"),
                ColumnGenerator::Constant(_) => panic!("Non repeating over Constant"),
                ColumnGenerator::NonRepeating(_) => panic!("Nested NonRepeating"),
            };

            if self.generated.insert(d.clone()) {
                return d;
            }

            reps += 1;
            if reps == 100 {
                println!(
                    "Having a hard time generating a unique value, try a wider range {:?}",
                    self.generator
                )
            }
        }
    }
}

#[derive(Debug)]
pub struct ColumnDataGeneration {
    pub generator: ColumnGenerator,
    /// Values per column that should be present in that column at least some of the time.
    ///
    /// This is used to ensure that queries that filter on constant values get at least some
    /// results
    expected_values: HashSet<DataType>,
}

/// Column data type and data generation information.
#[derive(Debug, Clone)]
pub struct ColumnSpec {
    pub sql_type: SqlType,
    pub gen_spec: Arc<Mutex<ColumnDataGeneration>>,
}

#[derive(Debug, Clone)]
pub struct TableSpec {
    pub name: TableName,
    pub columns: HashMap<ColumnName, ColumnSpec>,
    column_name_counter: u32,

    /// Name of the primary key column for the table, if any
    pub primary_key: Option<ColumnName>,
}

impl From<CreateTableStatement> for TableSpec {
    fn from(stmt: CreateTableStatement) -> Self {
        let primary_key: Option<ColumnName> =
            find_primary_keys(&stmt).map(|cspec| cspec.column.clone().into());

        let mut spec = TableSpec {
            name: stmt.table.name.into(),
            columns: stmt
                .fields
                .iter()
                .map(|field| {
                    let sql_type = field.sql_type.clone();
                    let generator = if let Some(d) =
                        field.has_default().and_then(|l| DataType::try_from(l).ok())
                    {
                        // Prefer the specified default value for a field
                        ColumnGenerator::Constant(d.coerce_to(&sql_type).unwrap().into())
                    } else {
                        // Otherwise default to generating fields with a constant value.
                        ColumnGenerator::Constant(sql_type.clone().into())
                    };

                    (
                        field.column.name.clone().into(),
                        ColumnSpec {
                            sql_type,
                            gen_spec: Arc::new(Mutex::new(ColumnDataGeneration {
                                generator,
                                expected_values: HashSet::new(),
                            })),
                        },
                    )
                })
                .collect(),
            column_name_counter: 0,
            primary_key,
        };

        for col in stmt
            .keys
            .into_iter()
            .flatten()
            .flat_map(|k| match k {
                    TableKey::PrimaryKey{columns: ks, .. }
                    | TableKey::UniqueKey { columns: ks, .. }
                      // HACK(grfn): To get foreign keys filled, we just mark them as unique, which
                      // given that we (currently) generate the same number of rows for each table
                      // means we're coincidentally guaranteed to get values matching the other side
                      // of the fk. This isn't super robust (unsurprisingly) and should probably be
                      // replaced with something smarter in the future.
                    | TableKey::ForeignKey { columns: ks, .. } => ks,
                    _ => vec![],
                })
            .map(|c| ColumnName::from(c.name))
        {
            // Unwrap: Unique key columns come from the CreateTableStatement we just
            // generated the TableSpec from. They should be valid columns.
            let col_spec = spec.columns.get_mut(&col).unwrap();
            col_spec.gen_spec.lock().generator =
                ColumnGenerator::Unique(col_spec.sql_type.clone().into());
        }

        // Apply annotations in the end
        for field in stmt.fields.iter() {
            if let Some(d) = field
                .comment
                .as_deref()
                .and_then(|s| s.parse::<DistributionAnnotation>().ok())
            {
                let col_spec = spec
                    .columns
                    .get_mut(&ColumnName::from(field.column.name.as_str()))
                    .unwrap();

                let generator = d.spec.generator_for_col(field.sql_type.clone());
                col_spec.gen_spec.lock().generator = if d.unique {
                    generator.into_unique()
                } else {
                    generator
                }
            }
        }

        spec
    }
}

impl From<TableSpec> for CreateTableStatement {
    fn from(spec: TableSpec) -> Self {
        CreateTableStatement {
            table: spec.name.into(),
            fields: spec
                .columns
                .into_iter()
                .map(|(col_name, col_type)| ColumnSpecification {
                    column: col_name.into(),
                    sql_type: col_type.sql_type,
                    constraints: vec![],
                    comment: None,
                })
                .collect(),
            keys: spec.primary_key.map(|cn| {
                vec![TableKey::PrimaryKey {
                    name: None,
                    columns: vec![cn.into()],
                }]
            }),
            if_not_exists: false,
            options: vec![],
        }
    }
}

impl TableSpec {
    pub fn new(name: TableName) -> Self {
        Self {
            name,
            columns: Default::default(),
            column_name_counter: 0,
            primary_key: None,
        }
    }

    /// Generate a new, unique column in this table (of an unspecified type) and return its name
    pub fn fresh_column(&mut self) -> ColumnName {
        self.fresh_column_with_type(SqlType::Int(None))
    }

    /// Generate a new, unique column in this table with the specified type and return its name.
    pub fn fresh_column_with_type(&mut self, col_type: SqlType) -> ColumnName {
        self.column_name_counter += 1;
        let column_name = ColumnName(format!("column_{}", self.column_name_counter));
        self.columns.insert(
            column_name.clone(),
            ColumnSpec {
                sql_type: col_type.clone(),
                gen_spec: Arc::new(Mutex::new(ColumnDataGeneration {
                    generator: ColumnGenerator::Constant(col_type.into()),
                    expected_values: HashSet::new(),
                })),
            },
        );
        column_name
    }

    /// Returns the name of *some* column in this table, potentially generating a new column if
    /// necessary
    pub fn some_column_name(&mut self) -> ColumnName {
        self.columns
            .keys()
            .next()
            .cloned()
            .unwrap_or_else(|| self.fresh_column())
    }

    /// Returns the name of *some* column in this table with the given type, potentially generating
    /// a new column if necessary
    pub fn some_column_with_type(&mut self, col_type: SqlType) -> ColumnName {
        self.columns
            .iter()
            .find_map(|(n, t)| {
                if t.sql_type == col_type {
                    Some(n)
                } else {
                    None
                }
            })
            .cloned()
            .unwrap_or_else(|| self.fresh_column_with_type(col_type))
    }

    /// Returns the name of *some* column in this table with the given type but different than the
    /// one specified, potentially generating a new column if necessary
    pub fn some_column_with_type_different_than(
        &mut self,
        col_type: SqlType,
        name: &ColumnName,
    ) -> ColumnName {
        self.columns
            .iter()
            .find_map(|(n, t)| {
                if t.sql_type == col_type && n != name {
                    Some(n)
                } else {
                    None
                }
            })
            .cloned()
            .unwrap_or_else(|| self.fresh_column_with_type(col_type))
    }

    /// Specifies that the column given by `column_name` should be a primary key value
    /// and generate unique column data.
    pub fn set_primary_key_column(&mut self, column_name: &ColumnName) {
        assert!(self.columns.contains_key(column_name));
        let col_spec = self.columns.get_mut(column_name).unwrap();
        col_spec.gen_spec.lock().generator =
            ColumnGenerator::Unique(col_spec.sql_type.clone().into());
    }

    /// Record that the column given by `column_name` should contain `value` at least some of the
    /// time.
    ///
    /// This can be used, for example, to ensure that queries that filter comparing against a
    /// constant value return at least some results
    pub fn expect_value(&mut self, column_name: ColumnName, value: DataType) {
        assert!(self.columns.contains_key(&column_name));
        self.columns
            .get_mut(&column_name)
            .unwrap()
            .gen_spec
            .lock()
            .expected_values
            .insert(value);
    }

    /// Overrides the existing `gen_spec` for a column with `spec`.
    pub fn set_column_generator_spec(
        &mut self,
        column_name: ColumnName,
        spec: ColumnGenerationSpec,
    ) {
        assert!(self.columns.contains_key(&column_name));
        let col_spec = self.columns.get_mut(&column_name).unwrap();
        self.columns
            .get_mut(&column_name)
            .unwrap()
            .gen_spec
            .lock()
            .generator = spec.generator_for_col(col_spec.sql_type.clone());
    }

    /// Overrides the existing `gen_spec` for a set of columns..
    pub fn set_column_generator_specs(&mut self, specs: &[(ColumnName, ColumnGenerationSpec)]) {
        for s in specs {
            self.set_column_generator_spec(s.0.clone(), s.1.clone());
        }
    }

    fn generate_row(&mut self, index: usize, random: bool) -> HashMap<ColumnName, DataType> {
        self.columns
            .iter_mut()
            .map(
                |(
                    col_name,
                    ColumnSpec {
                        sql_type: col_type,
                        gen_spec: col_spec,
                    },
                )| {
                    let mut spec = col_spec.lock();
                    let ColumnDataGeneration {
                        generator,
                        expected_values,
                    } = spec.deref_mut();
                    let value = match generator {
                        // Allow using the `index` for key columns which are specified
                        // as Unique.
                        ColumnGenerator::Unique(u) => u.gen(),
                        _ if index % 2 == 0 && !expected_values.is_empty() => expected_values
                            .iter()
                            .nth(index / 2 % expected_values.len())
                            .unwrap()
                            .clone(),
                        _ if random => random_value_of_type(col_type),
                        ColumnGenerator::Constant(c) => c.gen(),
                        ColumnGenerator::Uniform(u) => u.gen(),
                        ColumnGenerator::Random(r) => r.gen(),
                        ColumnGenerator::RandomString(r) => r.gen(),
                        ColumnGenerator::Zipfian(z) => z.gen(),
                        ColumnGenerator::NonRepeating(r) => r.gen(),
                    };

                    (col_name.clone(), value)
                },
            )
            .collect()
    }

    /// Generate `num_rows` rows of data for this table. If `random` is true, columns
    /// that are not unique and do not need to yield expected values, have their
    /// DataGenerationSpec overriden with DataGenerationSpec::Random.
    pub fn generate_data(
        &mut self,
        num_rows: usize,
        random: bool,
    ) -> Vec<HashMap<ColumnName, DataType>> {
        self.generate_data_from_index(num_rows, 0, random)
    }

    /// Generate `num_rows` rows of data for this table starting with the index:
    /// `index`. If `random` is true, columns that are not unique and do not
    /// need to yield expected values, have their DataGenerationSpec overriden
    /// with DataGenerationSpec::Random.
    pub fn generate_data_from_index(
        &mut self,
        num_rows: usize,
        index: usize,
        random: bool,
    ) -> Vec<HashMap<ColumnName, DataType>> {
        (index..index + num_rows)
            .map(|n| self.generate_row(n, random))
            .collect()
    }

    /// Ensure this table has a primary key column, and return its name
    pub fn primary_key(&mut self) -> &ColumnName {
        if self.primary_key.is_none() {
            let col = self.fresh_column_with_type(SqlType::Int(None));
            self.set_primary_key_column(&col);
            self.primary_key = Some(col)
        }

        // unwrap: we just set it to Some
        self.primary_key.as_ref().unwrap()
    }
}

#[derive(Debug, Default)]
pub struct GeneratorState {
    tables: HashMap<TableName, TableSpec>,
    table_name_counter: u32,
}

impl GeneratorState {
    /// Create a new, unique, empty table, and return a mutable reference to that table
    pub fn fresh_table_mut(&mut self) -> &mut TableSpec {
        self.table_name_counter += 1;
        let table_name = TableName(format!("table_{}", self.table_name_counter));
        self.tables
            .entry(table_name)
            .or_insert_with_key(|tn| TableSpec::new(tn.clone()))
    }

    /// Returns a reference to the table with the given name, if it exists
    pub fn table<'a, TN>(&'a self, name: &TN) -> Option<&'a TableSpec>
    where
        TableName: Borrow<TN>,
        TN: Eq + Hash,
    {
        self.tables.get(name)
    }

    /// Returns a mutable reference to the table with the given name, if it exists
    pub fn table_mut<'a, TN>(&'a mut self, name: &TN) -> Option<&'a mut TableSpec>
    where
        TableName: Borrow<TN>,
        TN: Eq + Hash,
    {
        self.tables.get_mut(name)
    }

    /// Returns an iterator over all the names of tables created for queries by this generator state
    pub fn table_names(&self) -> impl Iterator<Item = &TableName> {
        self.tables.keys()
    }

    /// Return a mutable reference to *some* table in the schema - the implication being that the
    /// caller doesn't care which table
    pub fn some_table_mut(&mut self) -> &mut TableSpec {
        if self.tables.is_empty() {
            self.fresh_table_mut()
        } else {
            self.tables.values_mut().next().unwrap()
        }
    }

    pub fn new_query(&mut self) -> QueryState<'_> {
        QueryState::new(self)
    }

    /// Generate a new query using the given [`QuerySeed`]
    pub fn generate_query(&mut self, seed: QuerySeed) -> Query {
        let mut state = self.new_query();
        let query = seed.generate(&mut state);

        Query::new(state, query)
    }

    /// Return an iterator over `CreateTableStatement`s for all the tables in the schema
    pub fn into_ddl(self) -> impl Iterator<Item = CreateTableStatement> {
        self.tables.into_iter().map(|(_, tbl)| tbl.into())
    }

    /// Return an iterator over clones of `CreateTableStatement`s for all the tables in the schema
    pub fn ddl(&self) -> impl Iterator<Item = CreateTableStatement> + '_ {
        self.tables.iter().map(|(_, tbl)| tbl.clone().into())
    }

    /// Generate `num_rows` rows of data for the table given by `table_name`.
    /// If `random` is passed on column data will be random in length for
    /// variable length data, and value for fixed-lenght data.
    ///
    /// # Panics
    ///
    /// Panics if `table_name` is not a known table
    pub fn generate_data_for_table(
        &mut self,
        table_name: &TableName,
        num_rows: usize,
        random: bool,
    ) -> Vec<HashMap<ColumnName, DataType>> {
        self.tables
            .get_mut(table_name)
            .unwrap()
            .generate_data(num_rows, random)
    }

    /// Get a reference to the generator state's tables.
    pub fn tables(&self) -> &HashMap<TableName, TableSpec> {
        &self.tables
    }

    /// Get a mutable reference to the generator state's tables.
    pub fn tables_mut(&mut self) -> &mut HashMap<TableName, TableSpec> {
        &mut self.tables
    }
}

impl From<Vec<CreateTableStatement>> for GeneratorState {
    fn from(stmts: Vec<CreateTableStatement>) -> Self {
        GeneratorState {
            tables: stmts
                .into_iter()
                .map(|stmt| (stmt.table.name.clone().into(), stmt.into()))
                .collect(),
            ..Default::default()
        }
    }
}

pub struct QueryParameter {
    table_name: TableName,
    column_name: ColumnName,
    /// Index of this parameter in the list of parameters with the same table and column name, if
    /// any. This value is used when generating values for query parameters to generate multiple
    /// values when the same column appears in multiple parameters
    index: Option<u32>,
    generator: Arc<Mutex<ColumnGenerator>>,
}

pub struct QueryState<'a> {
    gen: &'a mut GeneratorState,
    tables: HashSet<TableName>,
    parameters: Vec<QueryParameter>,
    unique_parameters: HashMap<TableName, Vec<(ColumnName, DataType)>>,
    alias_counter: u32,
    datatype_counter: u8,
}

impl<'a> QueryState<'a> {
    pub fn new(gen: &'a mut GeneratorState) -> Self {
        Self {
            gen,
            tables: HashSet::new(),
            unique_parameters: HashMap::new(),
            parameters: Vec::new(),
            alias_counter: 0,
            datatype_counter: 0,
        }
    }

    /// Generate a new, unique column alias for the query
    pub fn fresh_alias(&mut self) -> String {
        self.alias_counter += 1;
        format!("alias_{}", self.alias_counter)
    }

    /// Return a mutable reference to *some* table in the schema - the implication being that the
    /// caller doesn't care which table
    pub fn some_table_mut(&mut self) -> &mut TableSpec {
        if let Some(table) = self.tables.iter().next() {
            self.gen.table_mut(table).unwrap()
        } else {
            let table = self.gen.some_table_mut();
            self.tables.insert(table.name.clone());
            table
        }
    }

    /// Returns a mutable reference to some table referenced in the given query
    pub fn some_table_in_query_mut<'b>(&'b mut self, query: &SelectStatement) -> &'b mut TableSpec {
        match query
            .tables
            .iter()
            .chain(query.join.iter().filter_map(|jc| match &jc.right {
                JoinRightSide::Table(tbl) => Some(tbl),
                _ => None,
            }))
            .next()
        {
            Some(tbl) => self.gen.table_mut(&tbl.name).unwrap(),
            None => self.some_table_mut(),
        }
    }

    /// Create a new, unique, empty table, and return a mutable reference to that table
    pub fn fresh_table_mut(&mut self) -> &mut TableSpec {
        let table = self.gen.fresh_table_mut();
        self.tables.insert(table.name.clone());
        table
    }

    /// Generate `rows_per_table` rows of data for all the tables referenced in the query for this
    /// QueryState.
    ///
    /// If `make_unique` is true and `make_unique_key` was previously called, the returned rows
    /// are modified to match the key returned by `make_unique_key`.
    pub fn generate_data(
        &mut self,
        rows_per_table: usize,
        make_unique: bool,
        random: bool,
    ) -> HashMap<TableName, Vec<HashMap<ColumnName, DataType>>> {
        let table_names = self.tables.clone();
        table_names
            .iter()
            .map(|table_name| {
                let mut rows = self
                    .gen
                    .generate_data_for_table(table_name, rows_per_table, random);
                if make_unique {
                    if let Some(column_data) = self.unique_parameters.get(table_name) {
                        for row in &mut rows {
                            for (column, data) in column_data {
                                row.insert(column.clone(), data.clone());
                            }
                        }
                    }
                }
                (table_name.clone(), rows)
            })
            .collect()
    }

    /// Record a new (positional) parameter for the query, comparing against the given column of the
    /// given table
    pub fn add_parameter(&mut self, table_name: TableName, column_name: ColumnName) {
        let col_type = self.gen.table(&table_name).unwrap().columns[&column_name]
            .sql_type
            .clone();
        self.parameters.push(QueryParameter {
            table_name,
            column_name,
            index: None,
            generator: Arc::new(Mutex::new(ColumnGenerator::Constant(col_type.into()))),
        })
    }

    /// Record a new (positional) parameter for the query, comparing against the given column
    /// of the given table, and with the given value recorded for the key.
    ///
    /// It is the responsibility of the caller to ensure, by calling methods like
    /// [`TableSpec::set_column_generator_spec`], that the value given will match rows at least some
    /// of the time.
    pub fn add_parameter_with_value<V>(
        &mut self,
        table_name: TableName,
        column_name: ColumnName,
        value: V,
    ) where
        DataType: From<V>,
    {
        self.parameters.push(QueryParameter {
            table_name,
            column_name,
            index: None,
            generator: Arc::new(Mutex::new(ColumnGenerator::Constant(
                DataType::from(value).into(),
            ))),
        })
    }

    /// Record a new (positional) parameter for the query, comparing against the given column of the
    /// given table, and with the given *index*, used to distinguish between duplicate instances of
    /// the same parameter in the query.
    pub fn add_parameter_with_index(
        &mut self,
        table_name: TableName,
        column_name: ColumnName,
        index: u32,
    ) {
        let table = self.gen.table_mut(&table_name).unwrap();
        let sql_type = table.columns[&column_name].sql_type.clone();
        let val = unique_value_of_type(&sql_type, index);
        table.expect_value(column_name.clone(), val);

        self.parameters.push(QueryParameter {
            table_name,
            column_name,
            index: Some(index),
            generator: Arc::new(Mutex::new(ColumnGenerator::Unique(sql_type.into()))),
        });
    }

    /// Make a new, unique key for all the parameters in the query.
    ///
    /// To get data that matches this key, call `generate_data()` after calling this function.
    pub fn make_unique_key(&mut self) -> Vec<DataType> {
        let mut ret = Vec::with_capacity(self.parameters.len());
        for QueryParameter {
            table_name,
            column_name,
            ..
        } in self.parameters.iter()
        {
            let val = unique_value_of_type(
                &self.gen.tables[table_name].columns[column_name].sql_type,
                self.datatype_counter as u32,
            );
            self.unique_parameters
                .entry(table_name.clone())
                .or_insert_with(Vec::new)
                .push((column_name.clone(), val.clone()));
            self.datatype_counter += 1;
            ret.push(val);
        }
        ret
    }

    /// Returns a lookup key for the parameters in the query that will return results
    pub fn key(&self) -> Vec<DataType> {
        self.parameters
            .iter()
            .map(
                |QueryParameter {
                     table_name,
                     column_name,
                     index,
                     generator,
                 }| {
                    let sql_type = &self.gen.tables[table_name].columns[column_name].sql_type;
                    match index {
                        Some(idx) => unique_value_of_type(sql_type, *idx),
                        None => generator.lock().gen(),
                    }
                },
            )
            .collect()
    }
}

pub struct Query<'gen> {
    pub state: QueryState<'gen>,
    pub statement: SelectStatement,
}

impl<'gen> Query<'gen> {
    pub fn new(state: QueryState<'gen>, statement: SelectStatement) -> Self {
        Self { state, statement }
    }

    /// Converts the DDL for this query into a Noria recipe
    pub fn ddl_recipe(&self) -> String {
        self.state
            .tables
            .iter()
            .map(|table_name| {
                let stmt = CreateTableStatement::from(self.state.gen.tables[table_name].clone());
                format!("{};", stmt)
            })
            .join("\n")
    }

    /// Converts this query into a Noria recipe, including both the DDL and the query itself, using
    /// the given name for the query
    pub fn to_recipe(&self, query_name: &str) -> String {
        format!(
            "{}\nQUERY {}: {};",
            self.ddl_recipe(),
            query_name,
            self.statement
        )
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize, Arbitrary)]
pub enum AggregateType {
    Count {
        column_type: SqlType,
        distinct: bool,
        count_nulls: bool,
    },
    Sum {
        #[strategy(SqlType::arbitrary_numeric_type())]
        column_type: SqlType,
        distinct: bool,
    },
    Avg {
        #[strategy(SqlType::arbitrary_numeric_type())]
        column_type: SqlType,
        distinct: bool,
    },
    GroupConcat,
    Max {
        column_type: SqlType,
    },
    Min {
        column_type: SqlType,
    },
}

impl AggregateType {
    pub fn column_type(&self) -> SqlType {
        match self {
            AggregateType::Count { column_type, .. } => column_type.clone(),
            AggregateType::Sum { column_type, .. } => column_type.clone(),
            AggregateType::Avg { column_type, .. } => column_type.clone(),
            AggregateType::GroupConcat => SqlType::Text,
            AggregateType::Max { column_type } => column_type.clone(),
            AggregateType::Min { column_type } => column_type.clone(),
        }
    }
}

/// Parameters for generating an arbitrary FilterRhs
#[derive(Clone)]
pub struct FilterRhsArgs {
    column_type: SqlType,
}

impl Default for FilterRhsArgs {
    fn default() -> Self {
        Self {
            column_type: SqlType::Int(None),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize, Arbitrary)]
#[arbitrary(args = FilterRhsArgs)]
pub enum FilterRHS {
    Constant(#[strategy(Literal::arbitrary_with_type(&args.column_type))] Literal),
    Column,
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, EnumIter, Serialize, Deserialize, Arbitrary)]
pub enum LogicalOp {
    And,
    Or,
}

impl From<LogicalOp> for BinaryOperator {
    fn from(op: LogicalOp) -> Self {
        match op {
            LogicalOp::And => BinaryOperator::And,
            LogicalOp::Or => BinaryOperator::Or,
        }
    }
}

fn filter_op() -> impl Strategy<Value = BinaryOperator> {
    use BinaryOperator::*;

    proptest::sample::select(vec![
        Like,
        NotLike,
        ILike,
        NotILike,
        Equal,
        NotEqual,
        Greater,
        GreaterOrEqual,
        Less,
        LessOrEqual,
    ])
}

/// An individual filter operation
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize, Arbitrary)]
#[arbitrary(args = FilterRhsArgs)]
pub enum FilterOp {
    /// Compare a column with either another column, or a value
    Comparison {
        #[strategy(filter_op())]
        op: BinaryOperator,

        #[strategy(any_with::<FilterRHS>((*args).clone()))]
        rhs: FilterRHS,
    },

    /// A BETWEEN comparison on a column and two constant values
    Between { negated: bool },

    /// An IS NULL comparison on a column
    IsNull { negated: bool },
}

/// A full representation of a filter to be added to a query
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct Filter {
    /// How to add the filter to the WHERE clause of the query
    pub extend_where_with: LogicalOp,

    /// The actual filter operation to add
    pub operation: FilterOp,

    /// The type of the column that's being filtered on
    pub column_type: SqlType,
}

impl Arbitrary for Filter {
    type Parameters = ();

    type Strategy = BoxedStrategy<Filter>;

    fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
        (any::<SqlType>(), any::<LogicalOp>())
            .prop_flat_map(|(column_type, extend_where_with)| {
                any_with::<FilterOp>(FilterRhsArgs {
                    column_type: column_type.clone(),
                })
                .prop_map(move |operation| Self {
                    column_type: column_type.clone(),
                    operation,
                    extend_where_with,
                })
            })
            .boxed()
    }
}

impl Filter {
    fn all_with_operator(operator: BinaryOperator) -> impl Iterator<Item = Self> {
        ALL_FILTER_RHS
            .iter()
            .cloned()
            .cartesian_product(LogicalOp::iter())
            .map(move |(rhs, extend_where_with)| Self {
                operation: FilterOp::Comparison { op: operator, rhs },
                extend_where_with,
                column_type: SqlType::Int(None),
            })
    }
}

// The names of the built-in functions we can generate for use in a project expression
#[derive(Debug, Eq, PartialEq, Clone, Copy, EnumIter, Serialize, Deserialize, Arbitrary)]
pub enum BuiltinFunction {
    ConvertTZ,
    DayOfWeek,
    IfNull,
    Month,
    Timediff,
    Addtime,
    Round,
}

/// A representation for where in a query a subquery is located
///
/// When we support them, subqueries in `IN` clauses should go here as well
#[derive(Debug, Eq, PartialEq, Clone, Copy, Serialize, Deserialize, Arbitrary)]
pub enum SubqueryPosition {
    Cte(JoinOperator),
    Join(JoinOperator),
}

/// Parameters for generating an arbitrary [`QueryOperation`]
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct QueryOperationArgs {
    in_subquery: bool,
}

/// Operations that can be performed as part of a SQL query
///
/// Members of this enum represent some sense of an individual operation that can be performed on an
/// arbitrary SQL query. Each operation knows how to add itself to a given SQL query (via
/// [`add_to_query`](QueryOperation::add_to_query)) with the aid of a mutable reference to a
/// [`GeneratorState`].
///
/// Some operations are parametrized on fields that, due to having too large of a state space to
/// enumerate exhaustively, are hardcoded when query operations are built from a user-supplied
/// string on the command-line (via [`Operations`]), and can only be changed when generating queries
/// randomly via the proptest [`Arbitrary`] implementation. See [this design doc][0] for more
/// information
///
/// Note that not every operation that Noria supports is currently included in this enum - planned
/// for the future are:
///
/// - arithmetic projections
/// - union
/// - order by
/// - ilike
///
/// each of which should be relatively straightforward to add here.
///
/// [0]: https://docs.google.com/document/d/1rb-AU_PsH2Z40XFLjmLP7DcyeJzlwKI4Aa-GQgEoWKA
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize, Arbitrary)]
#[arbitrary(args = QueryOperationArgs)]
pub enum QueryOperation {
    ColumnAggregate(AggregateType),
    Filter(Filter),
    Distinct,
    Join(JoinOperator),
    ProjectLiteral,
    #[weight(if args.in_subquery { 0 } else { 1 })]
    SingleParameter,
    #[weight(if args.in_subquery { 0 } else { 1 })]
    MultipleParameters,
    #[weight(if args.in_subquery { 0 } else { 1 })]
    InParameter {
        num_values: u8,
    },
    #[weight(if args.in_subquery { 0 } else { 1 })]
    RangeParameter,
    #[weight(if args.in_subquery { 0 } else { 1 })]
    MultipleRangeParameters,
    ProjectBuiltinFunction(BuiltinFunction),
    TopK {
        order_type: OrderType,
        limit: u64,
    },
    #[weight(0)]
    Subquery(SubqueryPosition),
}

const ALL_FILTER_RHS: &[FilterRHS] = &[FilterRHS::Column, FilterRHS::Constant(Literal::Integer(1))];

const COMPARISON_OPS: &[BinaryOperator] = &[
    BinaryOperator::Equal,
    BinaryOperator::NotEqual,
    BinaryOperator::Greater,
    BinaryOperator::GreaterOrEqual,
    BinaryOperator::Less,
    BinaryOperator::LessOrEqual,
];

const JOIN_OPERATORS: &[JoinOperator] = &[
    JoinOperator::LeftJoin,
    JoinOperator::LeftOuterJoin,
    JoinOperator::InnerJoin,
];

const DEFAULT_LIMIT: u64 = 10;

const ALL_TOPK: &[QueryOperation] = &[
    QueryOperation::TopK {
        order_type: OrderType::OrderAscending,
        limit: DEFAULT_LIMIT,
    },
    QueryOperation::TopK {
        order_type: OrderType::OrderDescending,
        limit: DEFAULT_LIMIT,
    },
];

const ALL_AGGREGATE_TYPES: &[AggregateType] = &[
    AggregateType::Count {
        column_type: SqlType::Int(None),
        distinct: true,
        count_nulls: false,
    },
    AggregateType::Count {
        column_type: SqlType::Int(None),
        distinct: false,
        count_nulls: false,
    },
    AggregateType::Sum {
        column_type: SqlType::Int(None),
        distinct: true,
    },
    AggregateType::Sum {
        column_type: SqlType::Int(None),
        distinct: false,
    },
    AggregateType::Avg {
        column_type: SqlType::Int(None),
        distinct: true,
    },
    AggregateType::Avg {
        column_type: SqlType::Int(None),
        distinct: false,
    },
    AggregateType::GroupConcat,
    AggregateType::Max {
        column_type: SqlType::Int(None),
    },
    AggregateType::Min {
        column_type: SqlType::Int(None),
    },
];

const ALL_SUBQUERY_POSITIONS: &[SubqueryPosition] = &[
    SubqueryPosition::Join(JoinOperator::InnerJoin),
    SubqueryPosition::Cte(JoinOperator::InnerJoin),
];

lazy_static! {
    static ref ALL_COMPARISON_FILTER_OPS: Vec<FilterOp> = {
        COMPARISON_OPS
            .iter()
            .cartesian_product(ALL_FILTER_RHS.iter().cloned())
            .map(|(operator, rhs)| FilterOp::Comparison {
                    op: *operator,
                    rhs,
                },
            )
            .collect()
    };

    static ref ALL_FILTER_OPS: Vec<FilterOp> = {
        ALL_COMPARISON_FILTER_OPS
            .iter()
            .cloned()
            .chain(iter::once(FilterOp::Between { negated: true }))
            .chain(iter::once(FilterOp::Between { negated: false }))
            .chain(iter::once(FilterOp::IsNull { negated: true }))
            .chain(iter::once(FilterOp::IsNull { negated: false }))
            .collect()
    };

    static ref ALL_FILTERS: Vec<Filter> = {
        ALL_FILTER_OPS
            .iter()
            .cloned()
            .cartesian_product(LogicalOp::iter())
            .map(|(operation, extend_where_with)| Filter {
                extend_where_with,
                operation,
                column_type: SqlType::Int(None)
            })
            .collect()
    };

    /// A list of all possible [`QueryOperation`]s
    pub static ref ALL_OPERATIONS: Vec<QueryOperation> = {
        ALL_AGGREGATE_TYPES
            .iter()
            .cloned()
            .map(QueryOperation::ColumnAggregate)
            .chain(iter::once(QueryOperation::Distinct))
            .chain(JOIN_OPERATORS.iter().cloned().map(QueryOperation::Join))
            .chain(iter::once(QueryOperation::ProjectLiteral))
            .chain(iter::once(QueryOperation::SingleParameter))
            .chain(iter::once(QueryOperation::InParameter { num_values: 3 }))
            .chain(BuiltinFunction::iter().map(QueryOperation::ProjectBuiltinFunction))
            .chain(ALL_TOPK.iter().cloned())
            .chain(ALL_SUBQUERY_POSITIONS.iter().cloned().map(QueryOperation::Subquery))
            .collect()
    };
}

fn extend_where(query: &mut SelectStatement, op: LogicalOp, cond: Expression) {
    query.where_clause = Some(match query.where_clause.take() {
        Some(existing_cond) => Expression::BinaryOp {
            op: op.into(),
            lhs: Box::new(existing_cond),
            rhs: Box::new(cond),
        },
        None => cond,
    })
}

fn and_where(query: &mut SelectStatement, cond: Expression) {
    extend_where(query, LogicalOp::And, cond)
}

fn query_has_aggregate(query: &SelectStatement) -> bool {
    query.fields.iter().any(|fde| {
        matches!(
            fde,
            FieldDefinitionExpression::Expression { expr, .. } if contains_aggregate(expr),
        )
    })
}

fn column_in_query<'state>(state: &mut QueryState<'state>, query: &mut SelectStatement) -> Column {
    match query.tables.first() {
        Some(table) => {
            let table_name = table.name.clone();
            let column = state.gen.table_mut(&table_name).unwrap().some_column_name();
            Column {
                name: column.into(),
                table: Some(table_name),
            }
        }
        None => {
            let table = state.some_table_mut();
            query.tables.push(table.name.clone().into());
            let colname = table.some_column_name();
            Column {
                name: colname.into(),
                table: Some(table.name.clone().into()),
            }
        }
    }
}

impl QueryOperation {
    /// Returns true if this query operation is supported inside of subqueries. If this function
    /// returns false, `add_to_query` will not be called on this query operation when adding it to a
    /// subquery.
    fn supported_in_subqueries(&self) -> bool {
        // We don't currently support query parameters in subqueries
        !matches!(
            self,
            QueryOperation::MultipleParameters
                | QueryOperation::SingleParameter
                | QueryOperation::InParameter { .. }
                | QueryOperation::RangeParameter
                | QueryOperation::MultipleRangeParameters
        )
    }

    /// Add this query operation to `query`, recording information about new tables and columns in
    /// `state`.
    fn add_to_query<'state>(&self, state: &mut QueryState<'state>, query: &mut SelectStatement) {
        match self {
            QueryOperation::ColumnAggregate(agg) => {
                use AggregateType::*;

                let alias = state.fresh_alias();
                let tbl = state.some_table_in_query_mut(query);

                if query.tables.is_empty() {
                    query.tables.push(tbl.name.clone().into());
                }

                let col = tbl.fresh_column_with_type(agg.column_type());

                let expr = Box::new(Expression::Column(Column {
                    name: col.into(),
                    table: Some(tbl.name.clone().into()),
                }));

                let func = match *agg {
                    Count {
                        distinct,
                        count_nulls,
                        ..
                    } => FunctionExpression::Count {
                        expr,
                        distinct,
                        count_nulls,
                    },
                    Sum { distinct, .. } => FunctionExpression::Sum { expr, distinct },
                    Avg { distinct, .. } => FunctionExpression::Avg { expr, distinct },
                    GroupConcat => FunctionExpression::GroupConcat {
                        expr,
                        separator: ", ".to_owned(),
                    },
                    Max { .. } => FunctionExpression::Max(expr),
                    Min { .. } => FunctionExpression::Min(expr),
                };

                query.fields.push(FieldDefinitionExpression::Expression {
                    alias: Some(alias),
                    expr: Expression::Call(func),
                });
            }

            QueryOperation::Filter(filter) => {
                let alias = state.fresh_alias();
                let tbl = state.some_table_in_query_mut(query);
                let col = tbl.some_column_with_type(filter.column_type.clone());

                if query.tables.is_empty() {
                    query.tables.push(tbl.name.clone().into());
                }

                let col_expr = Expression::Column(Column {
                    table: Some(tbl.name.clone().into()),
                    ..col.clone().into()
                });

                query.fields.push(FieldDefinitionExpression::Expression {
                    expr: col_expr.clone(),
                    alias: Some(alias),
                });

                let cond = match &filter.operation {
                    FilterOp::Comparison { op, rhs } => {
                        let rhs = Box::new(match rhs {
                            FilterRHS::Constant(val) => {
                                tbl.expect_value(col, val.clone().try_into().unwrap());
                                Expression::Literal(val.clone())
                            }
                            FilterRHS::Column => {
                                let col = tbl.some_column_with_type_different_than(
                                    filter.column_type.clone(),
                                    &col,
                                );
                                Expression::Column(Column {
                                    table: Some(tbl.name.clone().into()),
                                    ..col.into()
                                })
                            }
                        });

                        Expression::BinaryOp {
                            op: *op,
                            lhs: Box::new(col_expr),
                            rhs,
                        }
                    }
                    FilterOp::Between { negated } => Expression::Between {
                        operand: Box::new(col_expr),
                        min: Box::new(Expression::Literal(Literal::Integer(1))),
                        max: Box::new(Expression::Literal(Literal::Integer(5))),
                        negated: *negated,
                    },
                    FilterOp::IsNull { negated } => {
                        tbl.expect_value(col, DataType::None);
                        Expression::BinaryOp {
                            lhs: Box::new(col_expr),
                            op: if *negated {
                                BinaryOperator::Is
                            } else {
                                BinaryOperator::IsNot
                            },
                            rhs: Box::new(Expression::Literal(Literal::Null)),
                        }
                    }
                };

                extend_where(query, filter.extend_where_with, cond);
            }

            QueryOperation::Distinct => {
                query.distinct = true;
                if let Some(order) = &query.order {
                    for (expr, _) in &order.order_by {
                        query.fields.push(FieldDefinitionExpression::Expression {
                            expr: expr.clone(),
                            alias: Some(state.fresh_alias()),
                        })
                    }
                }
            }

            QueryOperation::Join(operator) => {
                let left_table = state.some_table_in_query_mut(query);
                let left_table_name = left_table.name.clone();
                let left_join_key = left_table.some_column_with_type(SqlType::Int(None));
                let left_projected = left_table.fresh_column();

                if query.tables.is_empty() {
                    query.tables.push(left_table_name.clone().into());
                }

                let right_table = state.fresh_table_mut();
                let right_table_name = right_table.name.clone();
                let right_join_key = right_table.some_column_with_type(SqlType::Int(None));
                let right_projected = right_table.fresh_column();

                query.join.push(JoinClause {
                    operator: *operator,
                    right: JoinRightSide::Table(right_table.name.clone().into()),
                    constraint: JoinConstraint::On(Expression::BinaryOp {
                        op: BinaryOperator::Equal,
                        lhs: Box::new(Expression::Column(Column {
                            table: Some(left_table_name.clone().into()),
                            ..left_join_key.into()
                        })),
                        rhs: Box::new(Expression::Column(Column {
                            table: Some(right_table_name.clone().into()),
                            ..right_join_key.into()
                        })),
                    }),
                });

                query.fields.push(FieldDefinitionExpression::Expression {
                    expr: Expression::Column(Column {
                        table: Some(left_table_name.into()),
                        ..left_projected.into()
                    }),
                    alias: Some(state.fresh_alias()),
                });
                query.fields.push(FieldDefinitionExpression::Expression {
                    expr: Expression::Column(Column {
                        table: Some(right_table_name.into()),
                        ..right_projected.into()
                    }),
                    alias: Some(state.fresh_alias()),
                });
            }

            QueryOperation::ProjectLiteral => {
                let alias = state.fresh_alias();
                query.fields.push(FieldDefinitionExpression::Expression {
                    expr: Expression::Literal(Literal::Integer(1)),
                    alias: Some(alias),
                });
            }

            QueryOperation::SingleParameter => {
                let col = column_in_query(state, query);
                and_where(
                    query,
                    Expression::BinaryOp {
                        op: BinaryOperator::Equal,
                        lhs: Box::new(Expression::Column(col.clone())),
                        rhs: Box::new(Expression::Literal(Literal::Placeholder(
                            ItemPlaceholder::QuestionMark,
                        ))),
                    },
                );
                state.add_parameter(col.table.unwrap().into(), col.name.into());
            }

            QueryOperation::MultipleParameters => {
                QueryOperation::SingleParameter.add_to_query(state, query);
                QueryOperation::SingleParameter.add_to_query(state, query);
            }

            QueryOperation::RangeParameter => {
                let tbl = state.some_table_in_query_mut(query);
                let col = tbl.some_column_with_type(SqlType::Int(None));
                and_where(
                    query,
                    Expression::BinaryOp {
                        lhs: Box::new(Expression::Column(Column {
                            table: Some(tbl.name.clone().into()),
                            ..col.clone().into()
                        })),
                        op: BinaryOperator::Greater,
                        rhs: Box::new(Expression::Literal(Literal::Placeholder(
                            ItemPlaceholder::QuestionMark,
                        ))),
                    },
                );
                tbl.set_column_generator_spec(
                    col.clone(),
                    ColumnGenerationSpec::Uniform(1.into(), 20.into()),
                );
                let tbl_name = tbl.name.clone();
                state.add_parameter_with_value(tbl_name, col, 10i32);
            }

            QueryOperation::MultipleRangeParameters => {
                QueryOperation::RangeParameter.add_to_query(state, query);
                QueryOperation::RangeParameter.add_to_query(state, query);
            }

            QueryOperation::InParameter { num_values } => {
                let col = column_in_query(state, query);
                and_where(
                    query,
                    Expression::In {
                        lhs: Box::new(Expression::Column(col.clone())),
                        rhs: InValue::List(
                            (0..*num_values)
                                .map(|_| {
                                    Expression::Literal(Literal::Placeholder(
                                        ItemPlaceholder::QuestionMark,
                                    ))
                                })
                                .collect(),
                        ),
                        negated: false,
                    },
                );

                for idx in 0..*num_values {
                    state.add_parameter_with_index(
                        col.table.clone().unwrap().into(),
                        col.name.clone().into(),
                        idx as _,
                    )
                }
            }
            QueryOperation::ProjectBuiltinFunction(bif) => {
                macro_rules! add_builtin {
                    ($fname:ident($($arg:tt)*)) => {{
                        let table = state.some_table_in_query_mut(&query);

                        if query.tables.is_empty() {
                            query.tables.push(table.name.clone().into());
                        }

                        let mut arguments = Vec::new();
                        add_builtin!(@args_to_expr, table, arguments, $($arg)*);
                        let expr = Expression::Call(FunctionExpression::Call {
                            name: stringify!($fname).to_owned(),
                            arguments,
                        });
                        let alias = state.fresh_alias();
                        query.fields.push(FieldDefinitionExpression::Expression {
                            alias: Some(alias.clone()),
                            expr,
                        });
                    }};

                    (@args_to_expr, $table: ident, $out: ident, $(,)?) => {};

                    (@args_to_expr, $table: ident, $out:ident, $arg:literal, $($args: tt)*) => {{
                        $out.push(Expression::Literal($arg.into()));
                        add_builtin!(@args_to_expr, $table, $out, $($args)*);
                    }};
                    (@args_to_expr, $table: ident, $out:ident, $arg:literal) => {
                        add_builtin!(@args_to_expr, $table, $out, $arg,);
                    };

                    (@args_to_expr, $table: ident, $out:ident, $arg:expr, $($args: tt)*) => {{
                        $out.push(Expression::Column(
                            Column {
                                table: Some($table.name.clone().into()),
                                ..$table.some_column_with_type($arg).into()
                            }
                        ));
                        add_builtin!(@args_to_expr, $table, $out, $($args)*);
                    }};
                    (@args_to_expr, $table: ident, $out:ident, $arg:expr) => {{
                        add_builtin!(@args_to_expr, $table, $out, $arg,);
                    }};
                }

                match bif {
                    BuiltinFunction::ConvertTZ => {
                        add_builtin!(convert_tz(SqlType::Timestamp, "America/New_York", "UTC"))
                    }
                    BuiltinFunction::DayOfWeek => add_builtin!(dayofweek(SqlType::Date)),
                    BuiltinFunction::IfNull => add_builtin!(ifnull(SqlType::Text, SqlType::Text)),
                    BuiltinFunction::Month => add_builtin!(month(SqlType::Date)),
                    BuiltinFunction::Timediff => {
                        add_builtin!(timediff(SqlType::Time, SqlType::Time))
                    }
                    BuiltinFunction::Addtime => add_builtin!(addtime(SqlType::Time, SqlType::Time)),
                    BuiltinFunction::Round => add_builtin!(round(SqlType::Real)),
                }
            }
            QueryOperation::TopK { order_type, limit } => {
                let table = state.some_table_in_query_mut(query);

                if query.tables.is_empty() {
                    query.tables.push(table.name.clone().into());
                }

                let column_name = table.some_column_name();
                let column = Column {
                    table: Some(table.name.clone().into()),
                    ..column_name.into()
                };
                query.order = Some(OrderClause {
                    order_by: vec![(Expression::Column(column.clone()), Some(*order_type))],
                });

                query.limit = Some(LimitClause {
                    limit: Expression::Literal(Literal::Integer(*limit as _)),
                    offset: None,
                });

                if query.distinct {
                    query.fields.push(FieldDefinitionExpression::Expression {
                        expr: Expression::Column(column),
                        alias: Some(state.fresh_alias()),
                    })
                }
            }
            // Subqueries are turned into QuerySeed::subqueries as part of
            // GeneratorOps::into_query_seeds
            QueryOperation::Subquery(_) => {}
        }
    }

    /// Returns an iterator over all permuations of length 1..`max_depth` [`QueryOperation`]s.
    pub fn permute(max_depth: usize) -> impl Iterator<Item = Vec<&'static QueryOperation>> {
        (1..=max_depth).flat_map(|depth| ALL_OPERATIONS.iter().combinations(depth))
    }
}

/// Representation of a subset of query operations
///
/// Operations can be converted from a user-supplied string using [`FromStr::from_str`], which
/// supports the following speccifications:
///
/// | Specification                           | Meaning                           |
/// |-----------------------------------------|-----------------------------------|
/// | aggregates                              | All [`AggregateType`]s            |
/// | count                                   | COUNT aggregates                  |
/// | count_distinct                          | COUNT(DISTINCT) aggregates        |
/// | sum                                     | SUM aggregates                    |
/// | sum_distinct                            | SUM(DISTINCT) aggregates          |
/// | avg                                     | AVG aggregates                    |
/// | avg_distinct                            | AVG(DISTINCT) aggregates          |
/// | group_concat                            | GROUP_CONCAT aggregates           |
/// | max                                     | MAX aggregates                    |
/// | min                                     | MIN aggregates                    |
/// | filters                                 | All constant-valued [`Filter`]s   |
/// | equal_filters                           | Constant-valued `=` filters       |
/// | not_equal_filters                       | Constant-valued `!=` filters      |
/// | greater_filters                         | Constant-valued `>` filters       |
/// | greater_or_equal_filters                | Constant-valued `>=` filters      |
/// | less_filters                            | Constant-valued `<` filters       |
/// | less_or_equal_filters                   | Constant-valued `<=` filters      |
/// | between_filters                         | Constant-valued `BETWEEN` filters |
/// | is_null_filters                         | IS NULL and IS NOT NULL filters   |
/// | distinct                                | `SELECT DISTINCT`                 |
/// | joins                                   | Joins, with all [`JoinOperator`]s |
/// | inner_join                              | `INNER JOIN`s                     |
/// | left_join                               | `LEFT JOIN`s                      |
/// | single_parameter / single_param / param | A single query parameter          |
/// | range_param                             | A range query parameter           |
/// | multiple_parameters / params            | Multiple query parameters         |
/// | multiple_range_params                   | Multiple range query parameters   |
/// | in_parameter                            | IN with multiple query parameters |
/// | project_literal                         | A projected literal value         |
/// | project_builtin                         | Project a built-in function       |
/// | subqueries                              | All subqueries                    |
/// | cte                                     | CTEs (WITH statements)            |
/// | join_subquery                           | JOIN to a subquery directly       |
/// | topk                                    | ORDER BY combined with LIMIT      |
#[repr(transparent)]
#[derive(Debug, PartialEq, Eq, Clone, From, Into)]
pub struct Operations(pub Vec<QueryOperation>);

impl FromStr for Operations {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use QueryOperation::*;

        match s {
            "aggregates" => Ok(ALL_AGGREGATE_TYPES
                .iter()
                .cloned()
                .map(ColumnAggregate)
                .collect()),
            "count" => Ok(vec![ColumnAggregate(AggregateType::Count {
                column_type: SqlType::Int(None),
                distinct: false,
                count_nulls: false,
            })]
            .into()),
            "count_distinct" => Ok(vec![ColumnAggregate(AggregateType::Count {
                column_type: SqlType::Int(None),
                distinct: true,
                count_nulls: false,
            })]
            .into()),
            "sum" => Ok(vec![ColumnAggregate(AggregateType::Sum {
                column_type: SqlType::Int(None),
                distinct: false,
            })]
            .into()),
            "sum_distinct" => Ok(vec![ColumnAggregate(AggregateType::Sum {
                column_type: SqlType::Int(None),
                distinct: true,
            })]
            .into()),
            "avg" => Ok(vec![ColumnAggregate(AggregateType::Avg {
                column_type: SqlType::Int(None),
                distinct: false,
            })]
            .into()),
            "avg_distinct" => Ok(vec![ColumnAggregate(AggregateType::Avg {
                column_type: SqlType::Int(None),
                distinct: true,
            })]
            .into()),
            "group_concat" => Ok(vec![ColumnAggregate(AggregateType::GroupConcat)].into()),
            "max" => Ok(vec![ColumnAggregate(AggregateType::Max {
                column_type: SqlType::Int(None),
            })]
            .into()),
            "min" => Ok(vec![ColumnAggregate(AggregateType::Min {
                column_type: SqlType::Int(None),
            })]
            .into()),
            "filters" => Ok(ALL_FILTERS.iter().cloned().map(Filter).collect()),
            "equal_filters" => Ok(crate::Filter::all_with_operator(BinaryOperator::Equal)
                .map(Filter)
                .collect()),
            "not_equal_filters" => Ok(crate::Filter::all_with_operator(BinaryOperator::NotEqual)
                .map(Filter)
                .collect()),
            "greater_filters" => Ok(crate::Filter::all_with_operator(BinaryOperator::Greater)
                .map(Filter)
                .collect()),
            "greater_or_equal_filters" => Ok(crate::Filter::all_with_operator(
                BinaryOperator::GreaterOrEqual,
            )
            .map(Filter)
            .collect()),
            "less_filters" => Ok(crate::Filter::all_with_operator(BinaryOperator::Less)
                .map(Filter)
                .collect()),
            "less_or_equal_filters" => Ok(crate::Filter::all_with_operator(
                BinaryOperator::LessOrEqual,
            )
            .map(Filter)
            .collect()),
            "between_filters" => Ok(LogicalOp::iter()
                .cartesian_product(
                    iter::once(FilterOp::Between { negated: true })
                        .chain(iter::once(FilterOp::Between { negated: false })),
                )
                .map(|(extend_where_with, operation)| crate::Filter {
                    extend_where_with,
                    operation,

                    column_type: SqlType::Int(None),
                })
                .map(Filter)
                .collect()),
            "is_null_filters" => Ok(LogicalOp::iter()
                .cartesian_product(
                    iter::once(FilterOp::IsNull { negated: true })
                        .chain(iter::once(FilterOp::IsNull { negated: false })),
                )
                .map(|(extend_where_with, operation)| crate::Filter {
                    extend_where_with,
                    operation,
                    column_type: SqlType::Int(None),
                })
                .map(Filter)
                .collect()),
            "distinct" => Ok(vec![Distinct].into()),
            "joins" => Ok(JOIN_OPERATORS.iter().cloned().map(Join).collect()),
            "inner_join" => Ok(vec![Join(JoinOperator::InnerJoin)].into()),
            "left_join" => Ok(vec![Join(JoinOperator::LeftJoin)].into()),
            "single_parameter" | "single_param" | "param" => Ok(vec![SingleParameter].into()),
            "multiple_parameters" | "params" => Ok(vec![MultipleParameters].into()),
            "range_param" => Ok(vec![RangeParameter].into()),
            "multiple_range_params" => Ok(vec![MultipleRangeParameters].into()),
            "in_parameter" => Ok(vec![InParameter { num_values: 3 }].into()),
            "project_literal" => Ok(vec![ProjectLiteral].into()),
            "project_builtin" => Ok(BuiltinFunction::iter()
                .map(ProjectBuiltinFunction)
                .collect()),
            "subqueries" => Ok(ALL_SUBQUERY_POSITIONS
                .iter()
                .cloned()
                .map(Subquery)
                .collect()),
            "cte" => Ok(vec![Subquery(SubqueryPosition::Cte(JoinOperator::InnerJoin))].into()),
            "join_subquery" => {
                Ok(vec![Subquery(SubqueryPosition::Join(JoinOperator::InnerJoin))].into())
            }
            "topk" => Ok(ALL_TOPK.to_vec().into()),
            s => Err(anyhow!("unknown query operation: {}", s)),
        }
    }
}

impl FromIterator<QueryOperation> for Operations {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = QueryOperation>,
    {
        Self(iter.into_iter().collect())
    }
}

impl IntoIterator for Operations {
    type Item = QueryOperation;

    type IntoIter = <Vec<QueryOperation> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a Operations {
    type Item = &'a QueryOperation;

    type IntoIter = <&'a Vec<QueryOperation> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        (&self.0).iter()
    }
}

/// Representation of a list of subsets of query operations, as specified by the user on the command
/// line.
///
/// `OperationList` can be converted from a (user-supplied) string using [`FromStr::from_str`],
/// using a comma-separated list of [`Operations`]
#[repr(transparent)]
#[derive(Clone)]
pub struct OperationList(pub Vec<Operations>);

impl FromStr for OperationList {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(
            s.split(',')
                .map(Operations::from_str)
                .collect::<Result<Vec<_>, _>>()?,
        ))
    }
}

impl OperationList {
    /// Generate a set of permutations of all the sets of [`QueryOperation`]s represented by the
    /// [`Operations`] in this `OperationList`.
    pub fn permute(&self) -> impl Iterator<Item = Vec<QueryOperation>> + '_ {
        self.0
            .iter()
            .multi_cartesian_product()
            .map(|ops| ops.into_iter().cloned().collect())
    }
}

/// A specification for a subquery included in a query
#[derive(Debug, Clone, PartialEq, Eq, Arbitrary)]
pub struct Subquery {
    /// Where does the subquery appear in the query?
    position: SubqueryPosition,

    /// The specification for the query itself
    #[strategy(any_with::<QuerySeed>(QueryOperationArgs { in_subquery: true } ))]
    seed: QuerySeed,
}

impl Subquery {
    fn add_to_query<'state>(self, state: &mut QueryState<'state>, query: &mut SelectStatement) {
        let mut subquery = self.seed.generate(state);
        // just use the first selected column as the join key (maybe change this later)
        let right_join_col = match subquery.fields.first_mut() {
            Some(FieldDefinitionExpression::Expression {
                alias: Some(alias), ..
            }) => alias.clone(),
            Some(FieldDefinitionExpression::Expression {
                alias: alias @ None,
                ..
            }) => alias.insert(state.fresh_alias()).clone(),
            _ => panic!("Could not find a join key in subquery: {}", subquery),
        };

        let left_join_col = column_in_query(state, query);

        let subquery_name = state.fresh_alias();
        let (join_rhs, operator) = match self.position {
            SubqueryPosition::Cte(operator) => {
                query.ctes.push(CommonTableExpression {
                    name: subquery_name.clone(),
                    statement: subquery,
                });
                (
                    JoinRightSide::Table(Table {
                        name: subquery_name.clone(),
                        schema: None,
                        alias: None,
                    }),
                    operator,
                )
            }
            SubqueryPosition::Join(operator) => (
                JoinRightSide::NestedSelect(Box::new(subquery), Some(subquery_name.clone())),
                operator,
            ),
        };

        query.join.push(JoinClause {
            operator,
            right: join_rhs,
            constraint: JoinConstraint::On(Expression::BinaryOp {
                lhs: Box::new(Expression::Column(left_join_col)),
                op: BinaryOperator::Equal,
                rhs: Box::new(Expression::Column(Column {
                    name: right_join_col,
                    table: Some(subquery_name),
                })),
            }),
        })
    }
}

/// A specification for generating an individual query
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuerySeed {
    /// The set of operations to include in the query
    operations: Vec<QueryOperation>,

    /// A set of subqueries to include in the query
    subqueries: Vec<Subquery>,
}

impl Arbitrary for QuerySeed {
    type Parameters = QueryOperationArgs;

    type Strategy = BoxedStrategy<QuerySeed>;

    fn arbitrary_with(op_args: Self::Parameters) -> Self::Strategy {
        any_with::<Vec<QueryOperation>>((Default::default(), op_args))
            .prop_map(|operations| Self {
                operations,
                subqueries: vec![],
            })
            .prop_recursive(3, 5, 3, |inner| {
                (
                    proptest::collection::vec((any::<SubqueryPosition>(), inner), 0..3).prop_map(
                        |sqs| {
                            sqs.into_iter()
                                .map(|(position, seed)| Subquery { position, seed })
                                .collect()
                        },
                    ),
                    any::<Vec<QueryOperation>>(),
                )
                    .prop_map(|(subqueries, operations)| Self {
                        subqueries,
                        operations,
                    })
            })
            .boxed()
    }
}

impl QuerySeed {
    /// Construct a new QuerySeed with the given operations and subqueries
    pub fn new(operations: Vec<QueryOperation>, subqueries: Vec<Subquery>) -> Self {
        Self {
            operations,
            subqueries,
        }
    }

    fn generate(self, state: &mut QueryState) -> SelectStatement {
        let mut query = SelectStatement::default();

        for op in self.operations {
            op.add_to_query(state, &mut query);
        }

        for subquery in self.subqueries {
            subquery.add_to_query(state, &mut query);
        }

        if query.fields.is_empty() {
            let col = column_in_query(state, &mut query);
            query.fields.push(FieldDefinitionExpression::Expression {
                expr: Expression::Column(col.clone()),
                alias: Some(state.fresh_alias()),
            });

            if query.tables.is_empty() {
                query.tables.push(Table {
                    name: col.table.unwrap(),
                    alias: None,
                    schema: None,
                });
            }
        }

        if query_has_aggregate(&query) {
            let mut group_by = query.group_by.take().unwrap_or_default();
            // Fill the GROUP BY with all columns not mentioned in an aggregate
            let existing_group_by_cols: HashSet<_> = group_by.columns.iter().cloned().collect();
            for field in &query.fields {
                if let FieldDefinitionExpression::Expression { expr, .. } = field {
                    if !contains_aggregate(expr) {
                        for col in expr.referred_columns() {
                            if !existing_group_by_cols.contains(col) {
                                group_by.columns.push(col.clone());
                            }
                        }
                    }
                }
            }

            if let Some(order) = &query.order {
                for (expr, _) in &order.order_by {
                    let col = match expr {
                        Expression::Column(col) => col,
                        _ => unreachable!(
                            "We don't currently ever generate order clauses on expressions"
                        ),
                    };
                    if !existing_group_by_cols.contains(col) {
                        group_by.columns.push(col.clone());
                    }
                }
            }

            // TODO: once we support HAVING we'll need to check that here too
            if !group_by.columns.is_empty() {
                query.group_by = Some(group_by);
            }
        }

        query
    }
}

fn parse_num_operations<T>(s: &str) -> anyhow::Result<BoundPair<T>>
where
    T: FromStr + Clone,
    <T as FromStr>::Err: Send + Sync + Error + 'static,
{
    use Bound::*;

    let (lower_s, upper_s) = match s.split_once("..") {
        Some(lu) => lu,
        None => {
            let n = T::from_str(s)?;
            return Ok((Included(n.clone()), Included(n)));
        }
    };

    let lower = T::from_str(lower_s)?;

    if let Some(without_equals) = upper_s.strip_prefix('=') {
        Ok((Included(lower), Included(T::from_str(without_equals)?)))
    } else {
        Ok((Included(lower), Excluded(T::from_str(upper_s)?)))
    }
}

#[derive(Parser, Clone)]
pub struct GenerateOpts {
    /// Comma-separated list of query operations to generate top-level queries with
    ///
    /// If not specified, will permute the set of all possible query operations.
    #[clap(long)]
    pub operations: Option<OperationList>,

    /// Maximum recursion depth to use when generating subqueries
    #[clap(long, default_value = "2")]
    pub subquery_depth: usize,

    /// Range of operations to be used in a single query, represented as either a single number or
    /// a Rust-compatible range
    ///
    /// If not specified, queries will all contain a number of operations equal to the length of
    /// `operations`.
    #[clap(long, parse(try_from_str = parse_num_operations))]
    pub num_operations: Option<BoundPair<usize>>,
}

impl GenerateOpts {
    /// Construct an iterator of [`QuerySeed`]s from the options in self.
    ///
    /// This involves permuting [`Self::operations`] up to [`Self::num_operations`] times, and
    /// recursively generating subqueries up to a depth of [`Self::subquery_depth`]
    pub fn into_query_seeds(self) -> impl Iterator<Item = QuerySeed> {
        let operations: Vec<_> = match self.operations {
            Some(OperationList(ops)) => ops.into_iter().flat_map(|ops| ops.into_iter()).collect(),
            None => ALL_OPERATIONS.clone(),
        };

        let (subqueries, operations): (Vec<SubqueryPosition>, Vec<QueryOperation>) =
            operations.into_iter().partition_map(|op| {
                if let QueryOperation::Subquery(position) = op {
                    Either::Left(position)
                } else {
                    Either::Right(op)
                }
            });

        let num_operations = match self.num_operations {
            None => Either::Left(1..=operations.len()),
            Some(num_ops) => Either::Right(num_ops.into_iter().unwrap()),
        };

        let available_ops: Vec<_> = num_operations
            .flat_map(|depth| operations.clone().into_iter().combinations(depth))
            .collect();

        fn make_seeds(
            subquery_depth: usize,
            operations: Vec<QueryOperation>,
            subqueries: Vec<SubqueryPosition>,
            available_ops: Vec<Vec<QueryOperation>>,
        ) -> impl Iterator<Item = QuerySeed> {
            if subquery_depth == 0 || subqueries.is_empty() {
                Either::Left(iter::once(QuerySeed {
                    operations,
                    subqueries: vec![],
                }))
            } else {
                Either::Right(
                    subqueries
                        .iter()
                        .cloned()
                        .map(|position| {
                            available_ops
                                .clone()
                                .into_iter()
                                .map(|mut ops| {
                                    ops.retain(|op| op.supported_in_subqueries());
                                    ops
                                })
                                .flat_map(|operations| {
                                    make_seeds(
                                        subquery_depth - 1,
                                        operations,
                                        subqueries.clone(),
                                        available_ops.clone(),
                                    )
                                })
                                .map(|seed| Subquery { position, seed })
                                .collect::<Vec<_>>()
                        })
                        .multi_cartesian_product()
                        .map(move |subqueries| QuerySeed {
                            operations: operations.clone(),
                            subqueries,
                        }),
                )
            }
        }

        let subquery_depth = self.subquery_depth;
        available_ops.clone().into_iter().flat_map(move |ops| {
            make_seeds(
                subquery_depth,
                ops,
                subqueries.clone(),
                available_ops.clone(),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generate_query(operations: Vec<QueryOperation>) -> SelectStatement {
        let mut gen = GeneratorState::default();
        let seed = QuerySeed {
            operations,
            subqueries: vec![],
        };
        gen.generate_query(seed).statement
    }

    #[test]
    fn parse_operation_list() {
        let src = "aggregates,joins";
        let OperationList(res) = OperationList::from_str(src).unwrap();
        assert_eq!(
            res,
            vec![
                Operations(vec![
                    QueryOperation::ColumnAggregate(AggregateType::Count {
                        column_type: SqlType::Int(None),
                        distinct: true,
                        count_nulls: false,
                    }),
                    QueryOperation::ColumnAggregate(AggregateType::Count {
                        column_type: SqlType::Int(None),
                        distinct: false,
                        count_nulls: false,
                    }),
                    QueryOperation::ColumnAggregate(AggregateType::Sum {
                        column_type: SqlType::Int(None),
                        distinct: true,
                    }),
                    QueryOperation::ColumnAggregate(AggregateType::Sum {
                        column_type: SqlType::Int(None),
                        distinct: false,
                    }),
                    QueryOperation::ColumnAggregate(AggregateType::Avg {
                        column_type: SqlType::Int(None),
                        distinct: true,
                    }),
                    QueryOperation::ColumnAggregate(AggregateType::Avg {
                        column_type: SqlType::Int(None),
                        distinct: false,
                    }),
                    QueryOperation::ColumnAggregate(AggregateType::GroupConcat),
                    QueryOperation::ColumnAggregate(AggregateType::Max {
                        column_type: SqlType::Int(None)
                    }),
                    QueryOperation::ColumnAggregate(AggregateType::Min {
                        column_type: SqlType::Int(None)
                    }),
                ]),
                Operations(vec![
                    QueryOperation::Join(JoinOperator::LeftJoin),
                    QueryOperation::Join(JoinOperator::LeftOuterJoin),
                    QueryOperation::Join(JoinOperator::InnerJoin),
                ])
            ]
        );
    }

    #[test]
    fn single_join() {
        let query = generate_query(vec![QueryOperation::Join(JoinOperator::LeftJoin)]);
        eprintln!("query: {}", query);
        assert_eq!(query.tables.len(), 1);
        assert_eq!(query.join.len(), 1);
        let join = query.join.first().unwrap();
        match &join.constraint {
            JoinConstraint::On(Expression::BinaryOp { op, lhs, rhs }) => {
                assert_eq!(op, &BinaryOperator::Equal);
                match (lhs.as_ref(), rhs.as_ref()) {
                    (Expression::Column(left_field), Expression::Column(right_field)) => {
                        assert_eq!(
                            left_field.table.as_ref(),
                            Some(&query.tables.first().unwrap().name)
                        );
                        assert_eq!(
                            right_field.table.as_ref(),
                            Some(match &join.right {
                                JoinRightSide::Table(table) => &table.name,
                                _ => unreachable!(),
                            })
                        );
                    }
                    _ => unreachable!(),
                }
            }
            constraint => unreachable!("Unexpected constraint: {:?}", constraint),
        }
    }

    mod parse_num_operations {
        use super::*;

        #[test]
        fn number() {
            assert_eq!(
                parse_num_operations::<usize>("13").unwrap(),
                (Bound::Included(13), Bound::Included(13))
            );
        }

        #[test]
        fn exclusive() {
            assert_eq!(
                parse_num_operations::<usize>("0..9").unwrap(),
                (Bound::Included(0), Bound::Excluded(9))
            )
        }

        #[test]
        fn inclusive() {
            assert_eq!(
                parse_num_operations::<usize>("0..=123").unwrap(),
                (Bound::Included(0), Bound::Included(123))
            )
        }
    }

    #[test]
    fn in_params() {
        let mut gen = GeneratorState::default();
        let seed = QuerySeed {
            operations: vec![QueryOperation::InParameter { num_values: 3 }],
            subqueries: vec![],
        };
        let query = gen.generate_query(seed);
        eprintln!("query: {}", query.statement);
        match query.statement.where_clause {
            Some(Expression::In {
                lhs: _,
                rhs: InValue::List(exprs),
                negated: false,
            }) => {
                assert_eq!(exprs.len(), 3);
                assert!(exprs.iter().all(|expr| *expr
                    == Expression::Literal(Literal::Placeholder(ItemPlaceholder::QuestionMark))));
            }
            _ => unreachable!(),
        }

        let key = query.state.key();
        assert_eq!(key.len(), 3);
    }
}
