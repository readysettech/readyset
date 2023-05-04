use std::cmp::min;
use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use bit_vec::BitVec;
use chrono::{Duration, FixedOffset, NaiveDate, NaiveTime, TimeZone};
use eui48::{MacAddress, MacAddressFormat};
use nom_sql::SqlType;
use rand::distributions::Standard;
use rand::prelude::Distribution;
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng, RngCore};
use readyset_data::{DfType, DfValue, Dialect};
use rust_decimal::Decimal;
use zipf::ZipfDistribution;

mod distribution_annotation;

pub use crate::distribution_annotation::DistributionAnnotation;

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
    Uniform(DfValue, DfValue),
    /// Non-repeating Uniform, an optional batch size can be specified to
    /// reset the distribution after n rows are generated.
    ///
    /// As this repeatedly pulls from a uniform distribution until we
    /// receive a value we have not yet seen in a batch, the batch
    /// size should be much smaller than the size of the distribution.
    UniformWithoutReplacement {
        min: DfValue,
        max: DfValue,
        batch_size: Option<u32>,
    },
    /// Generates a random value for the row.
    Random,
    /// Generate a random string from a regex
    RandomString(String),
    /// Generates an integer in the specified range. Cannot be used for
    /// non discrete integer DfValues.
    Zipfian {
        min: DfValue,
        max: DfValue,
        alpha: f64,
    },
    /// Always generate the same value
    Constant(DfValue),
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
            ColumnGenerationSpec::Constant(val) => {
                let col_type =
                    DfType::from_sql_type(&col_type, Dialect::DEFAULT_MYSQL, |_| None).unwrap();
                let val = val.coerce_to(&col_type, &DfType::Unknown).unwrap();
                ColumnGenerator::Constant(val.into())
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
    pub fn gen(&mut self) -> DfValue {
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
    pub fn into_unique(self) -> Self {
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
    value: DfValue,
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

impl<S: AsRef<str>> From<S> for RandomStringGenerator {
    fn from(s: S) -> Self {
        let s = s.as_ref();
        Self {
            regex: s.to_string(),
            inner: rand_regex::Regex::compile(s, 256).unwrap(),
        }
    }
}

impl RandomStringGenerator {
    pub fn gen(&self) -> DfValue {
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

impl From<DfValue> for ConstantGenerator {
    fn from(value: DfValue) -> Self {
        Self { value }
    }
}

impl ConstantGenerator {
    pub fn gen(&self) -> DfValue {
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
    pub fn gen(&mut self) -> DfValue {
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
    min: DfValue,
    max: DfValue,
    /// Whether we should replace values within the uniform distribution.
    with_replacement: bool,
    /// The number of values to generate before resetting the
    /// distribution. Only relevant if `with_replacement` is true.
    batch_size: Option<u32>,

    /// Values we have already pulled from the uniform distribution
    /// if we are not replacing values.
    pulled: HashSet<DfValue>,
}

impl UniformGenerator {
    pub fn gen(&mut self) -> DfValue {
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
    min: DfValue,
    max: DfValue,
    alpha: f64,
    dist: ZipfDistribution,
    mapping: Vec<DfValue>,
}

impl ZipfianGenerator {
    fn new(min: DfValue, max: DfValue, alpha: f64) -> Self {
        let (num_elements, mapping): (u64, Vec<DfValue>) = match (&min, &max) {
            (DfValue::Int(i), DfValue::Int(j)) => {
                let mut mapping: Vec<_> = (*i..*j).map(DfValue::Int).collect();
                mapping.shuffle(&mut rand::thread_rng());
                ((j - i) as u64, mapping)
            }
            (DfValue::UnsignedInt(i), DfValue::UnsignedInt(j)) => {
                let mut mapping: Vec<_> = (*i..*j).map(DfValue::UnsignedInt).collect();
                mapping.shuffle(&mut rand::thread_rng());
                ((j - i), mapping)
            }
            (_, _) => unimplemented!("DfValues unsupported for discrete zipfian value generation"),
        };

        Self {
            min,
            max,
            alpha,
            dist: ZipfDistribution::new(num_elements as usize, alpha).unwrap(),
            mapping,
        }
    }

    pub fn gen(&mut self) -> DfValue {
        let mut rng = rand::thread_rng();
        let offset = self.dist.sample(&mut rng);
        self.mapping.get(offset).unwrap().clone()
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
    pub fn gen(&self) -> DfValue {
        random_value_of_type(&self.sql_type, thread_rng())
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
    pub fn gen(&mut self) -> DfValue {
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

/// Generate a constant value with the given [`SqlType`]
///
/// The following SqlTypes do not have a representation as a [`DfValue`] and will panic if passed:
///
/// - [`SqlType::Date`]
/// - [`SqlType::Enum`]
/// - [`SqlType::Bool`]
pub fn value_of_type(typ: &SqlType) -> DfValue {
    match typ {
        SqlType::Char(_)
        | SqlType::VarChar(_)
        | SqlType::Blob
        | SqlType::LongBlob
        | SqlType::MediumBlob
        | SqlType::TinyBlob
        | SqlType::TinyText
        | SqlType::MediumText
        | SqlType::LongText
        | SqlType::Text
        | SqlType::Binary(_)
        | SqlType::VarBinary(_)
        | SqlType::Citext => "a".into(),
        SqlType::QuotedChar => 1i8.into(),
        SqlType::ByteArray => {
            // Zero is an interesting value, because it can only occur for
            // byte arrays, since character strings don't allow zero
            // octets.
            DfValue::ByteArray(Arc::new(vec![0u8]))
        }
        SqlType::Int(_) | SqlType::Int4 => 1i32.into(),
        SqlType::BigInt(_) | SqlType::Int8 => 1i64.into(),
        SqlType::UnsignedInt(_) | SqlType::Serial => 1u32.into(),
        SqlType::UnsignedBigInt(_) | SqlType::BigSerial => 1u64.into(),
        SqlType::TinyInt(_) => 1i8.into(),
        SqlType::UnsignedTinyInt(_) => 1u8.into(),
        SqlType::SmallInt(_) | SqlType::Int2 => 1i16.into(),
        SqlType::UnsignedSmallInt(_) => 1u16.into(),
        SqlType::Float | SqlType::Double => 1.5f64.try_into().unwrap(),
        SqlType::Real => 1.5f32.try_into().unwrap(),
        SqlType::Decimal(prec, scale) => {
            Decimal::new(if *prec == 1 { 1 } else { 15 }, *scale as _).into()
        }
        SqlType::Numeric(None) => DfValue::from(Decimal::new(15, 1)),
        SqlType::Numeric(Some((prec, scale))) => DfValue::from(Decimal::new(
            if *prec == 1 { 1 } else { 15 },
            (*scale).unwrap_or(1) as _,
        )),
        SqlType::DateTime(_) | SqlType::Timestamp => {
            NaiveDate::from_ymd(2020, 1, 1).and_hms(12, 30, 45).into()
        }
        SqlType::TimestampTz => DfValue::from(
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
        SqlType::Inet => "::beef".into(),
        SqlType::Uuid => "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11".into(),
        SqlType::Bit(size_opt) => {
            DfValue::from(BitVec::with_capacity(size_opt.unwrap_or(1) as usize))
        }
        SqlType::VarBit(_) => DfValue::from(BitVec::new()),
        SqlType::Array(_) => unimplemented!(),
        SqlType::Other(_) => unimplemented!(),
    }
}

/// Generate a random value with the given [`SqlType`]. The length of the value
/// is pulled from a uniform distribution over the set of possible ranges.
///
/// The following SqlTypes do not have a representation as a [`DfValue`] and will panic if passed:
///
/// - [`SqlType::Date`]
/// - [`SqlType::Enum`]
/// - [`SqlType::Bool`]
pub fn random_value_of_type<R>(typ: &SqlType, mut rng: R) -> DfValue
where
    R: RngCore,
{
    match typ {
        SqlType::Char(Some(x)) | SqlType::VarChar(Some(x)) => {
            let length: usize = rng.gen_range(1..=*x).into();
            "a".repeat(length).into()
        }
        SqlType::QuotedChar => rng.gen::<i8>().into(),
        SqlType::TinyBlob | SqlType::TinyText => {
            // 2^8 bytes
            let length: usize = rng.gen_range(1..256);
            "a".repeat(length).into()
        }
        SqlType::Blob
        | SqlType::Text
        | SqlType::Citext
        | SqlType::VarChar(None)
        | SqlType::Binary(None) => {
            // 2^16 bytes
            let length: usize = rng.gen_range(1..65536);
            "a".repeat(length).into()
        }
        SqlType::Char(None) => "a".into(),
        SqlType::MediumBlob | SqlType::MediumText => {
            // 2^24 bytes
            // Currently capped at 65536 as these are generated in memory.
            let length: usize = rng.gen_range(1..65536);
            "a".repeat(length).into()
        }
        SqlType::LongBlob | SqlType::LongText => {
            // 2^32 bytes
            // Currently capped at 65536 as these are generated in memory.
            let length: usize = rng.gen_range(1..65536);
            "a".repeat(length).into()
        }
        SqlType::Binary(Some(x)) | SqlType::VarBinary(x) => {
            // Convert to bytes and generate string data to match.
            let length: usize = rng.gen_range(1..*x / 8).into();
            "a".repeat(length).into()
        }
        SqlType::ByteArray => {
            let length = rng.gen_range(1..10);
            let mut array = Vec::new();
            for _ in 0..length {
                array.push(rng.gen::<u8>());
            }
            DfValue::ByteArray(Arc::new(array))
        }
        SqlType::Int(_) | SqlType::Int4 => rng.gen::<i32>().into(),
        SqlType::BigInt(_) | SqlType::Int8 => rng.gen::<i64>().into(),
        SqlType::UnsignedInt(_) => rng.gen::<u32>().into(),
        SqlType::UnsignedBigInt(_) => rng.gen::<u32>().into(),
        SqlType::TinyInt(_) => rng.gen::<i8>().into(),
        SqlType::UnsignedTinyInt(_) => rng.gen::<u8>().into(),
        SqlType::SmallInt(_) | SqlType::Int2 => rng.gen::<i16>().into(),
        SqlType::UnsignedSmallInt(_) => rng.gen::<u16>().into(),
        SqlType::Float | SqlType::Double => 1.5f64.try_into().unwrap(),
        SqlType::Real => 1.5f32.try_into().unwrap(),
        SqlType::Decimal(prec, scale) => {
            Decimal::new(if *prec == 1 { 1 } else { 15 }, *scale as _).into()
        }
        SqlType::Numeric(None) => DfValue::from(Decimal::new(15, 1)),
        SqlType::Numeric(Some((prec, scale))) => DfValue::from(Decimal::new(
            if *prec == 1 { 1 } else { 15 },
            (*scale).unwrap_or(1) as _,
        )),
        SqlType::DateTime(_) | SqlType::Timestamp => {
            // Generate a random month and day within the same year.
            NaiveDate::from_ymd(2020, rng.gen_range(1..12), rng.gen_range(1..28))
                .and_hms(12, 30, 45)
                .into()
        }
        SqlType::TimestampTz => DfValue::from(
            FixedOffset::west(18_000)
                .ymd(2020, rng.gen_range(1..12), rng.gen_range(1..28))
                .and_hms(12, 30, 45),
        ),
        SqlType::Time => NaiveTime::from_hms(12, 30, 45).into(),
        SqlType::Date => {
            NaiveDate::from_ymd(2020, rng.gen_range(1..12), rng.gen_range(1..28)).into()
        }
        SqlType::Bool => DfValue::from(rng.gen_bool(0.5)),
        SqlType::Enum(_) => unimplemented!(),
        SqlType::Json | SqlType::Jsonb => DfValue::from(format!(
            "{{\"k\":\"{}\"}}",
            "a".repeat(rng.gen_range(1..255))
        )),
        SqlType::MacAddr => {
            let mut bytes = [0_u8; 6];
            rng.fill(&mut bytes);
            // We know the length and format of the bytes, so this should always be parsable as a
            // `MacAddress`.
            #[allow(clippy::unwrap_used)]
            DfValue::from(
                MacAddress::from_bytes(&bytes[..])
                    .unwrap()
                    .to_string(MacAddressFormat::HexString),
            )
        }
        SqlType::Inet => DfValue::from(
            IpAddr::V4(Ipv4Addr::new(rng.gen(), rng.gen(), rng.gen(), rng.gen())).to_string(),
        ),
        SqlType::Uuid => {
            let mut bytes = [0_u8, 16];
            rng.fill(&mut bytes);
            // We know the length and format of the bytes, so this should always be parsable as a
            // `UUID`.
            #[allow(clippy::unwrap_used)]
            DfValue::from(uuid::Uuid::from_slice(&bytes[..]).unwrap().to_string())
        }
        SqlType::Bit(size_opt) => DfValue::from(BitVec::from_iter(
            rng.sample_iter(Standard)
                .take(size_opt.unwrap_or(1) as usize)
                .collect::<Vec<bool>>(),
        )),
        SqlType::VarBit(max_size) => {
            let size = rng.gen_range(0..max_size.unwrap_or(u16::MAX));
            DfValue::from(BitVec::from_iter(
                rng.sample_iter(Standard)
                    .take(size as usize)
                    .collect::<Vec<bool>>(),
            ))
        }
        SqlType::Serial => (rng.gen::<u32>() + 1).into(),
        SqlType::BigSerial => (rng.gen::<u64>() + 1).into(),
        SqlType::Array(_) => unimplemented!(),
        SqlType::Other(_) => unimplemented!(),
    }
}

/// Generate a random value from a uniform distribution with the given integer
/// [`SqlType`] for a given range of values.If the range of `min` and `max`
/// exceeds the storage of the type, this truncates to fit.
fn uniform_random_value(min: &DfValue, max: &DfValue) -> DfValue {
    let mut rng = rand::thread_rng();
    match (min, max) {
        (DfValue::Int(i), DfValue::Int(j)) => rng.gen_range(*i..*j).into(),
        (DfValue::UnsignedInt(i), DfValue::UnsignedInt(j)) => rng.gen_range(*i..*j).into(),
        (_, _) => unimplemented!("DfValues unsupported for random uniform value generation"),
    }
}

/// Generate a unique value with the given [`SqlType`] from a monotonically increasing counter,
/// `idx`.
///
/// This is an injective function (from `(idx, typ)` to the resultant [`DfValue`]).
///
/// The following SqlTypes do not have a representation as a [`DfValue`] and will panic if passed:
///
/// - [`SqlType::Date`]
/// - [`SqlType::Enum`]
/// - [`SqlType::Bool`]
pub fn unique_value_of_type(typ: &SqlType, idx: u32) -> DfValue {
    let clamp_digits = |prec: u32| {
        10u64
            .checked_pow(prec)
            .map(|digits| ((idx + 1) as u64 % digits) as i64)
            .unwrap_or(i64::MAX)
    };

    match typ {
        // FIXME: Take into account length parameters.
        SqlType::VarChar(None)
        | SqlType::Blob
        | SqlType::LongBlob
        | SqlType::MediumBlob
        | SqlType::TinyBlob
        | SqlType::TinyText
        | SqlType::MediumText
        | SqlType::LongText
        | SqlType::Text
        | SqlType::Citext
        | SqlType::Binary(_)
        | SqlType::VarBinary(_) => idx.to_string().into(),
        SqlType::VarChar(Some(len)) | SqlType::Char(Some(len)) => {
            let s = idx.to_string();
            (&s[..min(s.len(), *len as usize)]).into()
        }
        SqlType::Char(None) => (idx % 10).to_string().into(),
        SqlType::QuotedChar => (idx as i8).into(),
        SqlType::Int(_) | SqlType::Int4 => (idx as i32).into(),
        SqlType::BigInt(_) | SqlType::Int8 => (idx as i64).into(),
        SqlType::UnsignedInt(_) => idx.into(),
        SqlType::UnsignedBigInt(_) => (idx as u64).into(),
        SqlType::TinyInt(_) => (idx as i8).into(),
        SqlType::UnsignedTinyInt(_) => (idx).into(),
        SqlType::SmallInt(_) | SqlType::Int2 => (idx as i16).into(),
        SqlType::UnsignedSmallInt(_) => (idx as u16).into(),
        SqlType::Float | SqlType::Double => (1.5 + idx as f64).try_into().unwrap(),
        SqlType::Real => (1.5 + idx as f32).try_into().unwrap(),
        SqlType::Decimal(prec, scale) => Decimal::new(clamp_digits(*prec as _), *scale as _).into(),
        SqlType::Numeric(prec_scale) => match prec_scale {
            Some((prec, None)) => Decimal::new(clamp_digits(*prec as _), 1),
            Some((prec, Some(scale))) => Decimal::new(clamp_digits(*prec as _), *scale as _),
            None => Decimal::new((15 + idx) as i64, 2),
        }
        .into(),
        SqlType::DateTime(_) | SqlType::Timestamp => {
            (NaiveDate::from_ymd(2020, 1, 1).and_hms(0, 0, 0) + Duration::minutes(idx as _)).into()
        }
        SqlType::TimestampTz => DfValue::from(
            FixedOffset::west(18_000)
                .ymd(2020, 1, 1)
                .and_hms(12, idx as _, 30),
        ),
        SqlType::Date => {
            DfValue::from(NaiveDate::from_ymd(1000, 1, 1) + Duration::days(idx.into()))
        }
        SqlType::Enum(_) => unimplemented!(),
        SqlType::Bool => unimplemented!(),
        SqlType::ByteArray => unimplemented!(),
        SqlType::Time => (NaiveTime::from_hms(0, 0, 0) + Duration::seconds(idx as _)).into(),
        SqlType::Json | SqlType::Jsonb => DfValue::from(format!("{{\"k\": {}}}", idx)),
        SqlType::MacAddr => {
            let b1: u8 = ((idx >> 24) & 0xff) as u8;
            let b2: u8 = ((idx >> 16) & 0xff) as u8;
            let b3: u8 = ((idx >> 8) & 0xff) as u8;
            let b4: u8 = (idx & 0xff) as u8;
            let bytes = [b1, b2, b3, b4, u8::MIN, u8::MAX];
            // We know the length and format of the bytes, so this should always be parsable as a
            // `MacAddress`.
            #[allow(clippy::unwrap_used)]
            DfValue::from(
                MacAddress::from_bytes(&bytes[..])
                    .unwrap()
                    .to_string(MacAddressFormat::HexString),
            )
        }
        SqlType::Inet => {
            let b1: u8 = ((idx >> 24) & 0xff) as u8;
            let b2: u8 = ((idx >> 16) & 0xff) as u8;
            let b3: u8 = ((idx >> 8) & 0xff) as u8;
            let b4: u8 = (idx & 0xff) as u8;
            DfValue::from(IpAddr::V4(Ipv4Addr::new(b1, b2, b3, b4)).to_string())
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
            DfValue::from(uuid::Uuid::from_slice(&bytes[..]).unwrap().to_string())
        }
        SqlType::Bit(_) | SqlType::VarBit(_) => {
            let mut bytes = [u8::MAX; 4];
            bytes[0] = ((idx >> 24) & 0xff) as u8;
            bytes[1] = ((idx >> 16) & 0xff) as u8;
            bytes[2] = ((idx >> 8) & 0xff) as u8;
            bytes[3] = (idx & 0xff) as u8;
            DfValue::from(BitVec::from_bytes(&bytes[..]))
        }
        SqlType::Serial => (idx + 1).into(),
        SqlType::BigSerial => ((idx + 1) as u64).into(),
        SqlType::Array(_) => unimplemented!(),
        SqlType::Other(_) => unimplemented!(),
    }
}
