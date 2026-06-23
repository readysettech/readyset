use std::cmp::min;
use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use bit_vec::BitVec;
use chrono::{Duration, FixedOffset, NaiveDate, NaiveTime, TimeZone};
use eui48::{MacAddress, MacAddressFormat};
use rand::distr::uniform::SampleRange as _;
use rand::distr::{StandardUniform, Uniform};
use rand::prelude::Distribution;
use rand::seq::SliceRandom;
use rand::{Rng, RngExt};
use rand_distr::Zipf;
use readyset_data::Array;
use readyset_data::{encoding::Encoding, DfType, DfValue, Dialect};
use readyset_decimal::Decimal;
use readyset_spatial::{make_postgis_point_bytes, make_postgis_polygon_bytes};
use readyset_sql::ast::SqlType;

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
    /// Generate random characters of a specified max length and a specified charset
    RandomChar {
        min_length: usize,
        max_length: usize,
        charset: String,
    },
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
    pub fn generator_for_col(&self, col_type: SqlType, rng: &mut dyn Rng) -> ColumnGenerator {
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
            ColumnGenerationSpec::RandomChar {
                min_length,
                max_length,
                charset,
            } => ColumnGenerator::RandomChars(RandomCharsGenerator::new(
                *min_length,
                *max_length,
                charset,
            )),
            ColumnGenerationSpec::Zipfian { min, max, alpha } => ColumnGenerator::Zipfian(
                ZipfianGenerator::new(min.clone(), max.clone(), *alpha, rng),
            ),
            ColumnGenerationSpec::Constant(val) => {
                let col_type =
                    DfType::from_sql_type(&col_type, Dialect::DEFAULT_MYSQL, |_| None, None)
                        .unwrap();
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
    /// Returns random characters that are valid in a given charset
    RandomChars(RandomCharsGenerator),
    /// Returns a value generated from a zipfian distribution.
    Zipfian(ZipfianGenerator),
    /// Generate a unique value for every row from a non unique generator
    NonRepeating(NonRepeatingGenerator),
}

impl ColumnGenerator {
    pub fn gen<R: Rng>(&mut self, rng: &mut R) -> DfValue {
        match self {
            ColumnGenerator::Constant(g) => g.gen(),
            ColumnGenerator::Unique(g) => g.gen(),
            ColumnGenerator::Uniform(g) => g.gen(rng),
            ColumnGenerator::Random(g) => g.gen(rng),
            ColumnGenerator::RandomString(g) => g.gen(rng),
            ColumnGenerator::RandomChars(g) => g.gen(rng),
            ColumnGenerator::Zipfian(g) => g.gen(rng),
            ColumnGenerator::NonRepeating(g) => g.gen(rng),
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
            | u @ ColumnGenerator::RandomString(_)
            | u @ ColumnGenerator::RandomChars(_) => {
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
    pub fn gen<R: Rng>(&self, rng: &mut R) -> DfValue {
        let val: String = rng.sample(&self.inner);
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
        if self.generated.is_multiple_of(self.batch_size) {
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
    pub fn gen<R: Rng>(&mut self, rng: &mut R) -> DfValue {
        if self.with_replacement {
            uniform_random_value(&self.min, &self.max, rng)
        } else {
            let mut val = uniform_random_value(&self.min, &self.max, rng);
            let mut iters = 0;
            while self.pulled.contains(&val) {
                val = uniform_random_value(&self.min, &self.max, rng);
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
    dist: Zipf<f64>,
    mapping: Vec<DfValue>,
}

impl ZipfianGenerator {
    fn new(min: DfValue, max: DfValue, alpha: f64, rng: &mut dyn Rng) -> Self {
        let (num_elements, mut mapping): (u64, Vec<DfValue>) = match (&min, &max) {
            (DfValue::Int(i), DfValue::Int(j)) => {
                // Half-open `[i, j)`. When `i == j` the iterator is empty
                // and `Zipf::new(0, _)` errors out — the old code unwrapped
                // and panicked. Treat the empty range as a single-element
                // distribution containing `min`, so callers that
                // accidentally configure `min == max` get a deterministic
                // constant rather than a process abort.
                let mapping: Vec<_> = (*i..*j).map(DfValue::Int).collect();
                let n = (j - i).max(1) as u64;
                (n, mapping)
            }
            (DfValue::UnsignedInt(i), DfValue::UnsignedInt(j)) => {
                let mapping: Vec<_> = (*i..*j).map(DfValue::UnsignedInt).collect();
                let n = (j - i).max(1);
                (n, mapping)
            }
            (_, _) => unimplemented!("DfValues unsupported for discrete zipfian value generation"),
        };
        if mapping.is_empty() {
            mapping.push(min.clone());
        } else {
            mapping.shuffle(rng);
        }

        Self {
            min,
            max,
            alpha,
            // `Zipf::new` requires `n >= 1`; we just clamped to ensure that.
            // Use `expect` with context so a future invariant break surfaces
            // a clear message instead of a bare unwrap panic.
            dist: Zipf::new(num_elements as _, alpha).expect("Zipf::new requires n >= 1"),
            mapping,
        }
    }

    pub fn gen<R: Rng>(&mut self, rng: &mut R) -> DfValue {
        // `Zipf::sample` returns f64 in `[1, n]` inclusive — at the upper
        // bound `offset.round() as usize == n` indexes one past the end.
        // Clamp to `[0, len-1]` so the upper bound is well-defined instead
        // of panicking on `mapping.get(n).unwrap()`.
        let offset = self.dist.sample(rng);
        let idx = (offset.round() as usize)
            .saturating_sub(1)
            .min(self.mapping.len().saturating_sub(1));
        self.mapping[idx].clone()
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
    pub fn gen<R: Rng>(&self, rng: &mut R) -> DfValue {
        random_value_of_type(&self.sql_type, rng)
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
    pub fn gen<R: Rng>(&mut self, rng: &mut R) -> DfValue {
        let mut reps = 0;
        loop {
            let d = match &mut *self.generator {
                ColumnGenerator::Uniform(u) => u.gen(rng),
                ColumnGenerator::Zipfian(z) => z.gen(rng),
                ColumnGenerator::Random(r) => r.gen(rng),
                ColumnGenerator::RandomString(r) => r.gen(rng),
                ColumnGenerator::RandomChars(r) => r.gen(rng),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RandomCharsGenerator {
    min_length: usize,
    max_length: usize,
    low: u8,
    high: u8,
    encoding: Encoding,
}

impl RandomCharsGenerator {
    pub fn new(min_length: usize, max_length: usize, charset_name: &str) -> Self {
        let (low, high, encoding) = match charset_name {
            "ascii" => (0, 127, Encoding::Utf8),
            "utf8" => (0, 255, Encoding::Utf8),
            "latin1" => (0, 255, Encoding::Latin1),
            "binary" => (0, 255, Encoding::Binary),
            _ => panic!("Invalid charset"),
        };
        Self {
            min_length,
            max_length,
            low,
            high,
            encoding,
        }
    }

    pub fn gen<R: Rng>(&self, rng: &mut R) -> DfValue {
        let len = (self.min_length..self.max_length)
            .sample_single(rng)
            .unwrap();
        let sampler = Uniform::new_inclusive(self.low, self.high).unwrap();
        let bytes: Vec<u8> = (0..len).map(|_| sampler.sample(rng)).collect();

        // XXX: Hack alert! This goes through [`benchmarks::utils::generate::load_table_part`] as a
        // prepared statement parameter, which means it will be interpreted according to the client
        // connection, which will be utf8mb4. So until or unless we reorganize that part of the code
        // to accept arbitrary bytes with (for example in MySQL) either a `_binary` introducer or
        // using `UNHEX(...)` in the INSERT statement, we will translate this to the equivalent
        // UTF-8 so that the database re-converts it to the intended character set for storage.
        if let Encoding::Binary = &self.encoding {
            bytes.into()
        } else {
            self.encoding.decode(&bytes).unwrap().into()
        }
    }
}

/// Generate a constant value with the given [`SqlType`]
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
        SqlType::Int(_) | SqlType::MediumInt(_) | SqlType::Int4 | SqlType::Serial => 1i32.into(),
        SqlType::BigInt(_)
        | SqlType::Int8
        | SqlType::BigSerial
        | SqlType::Signed
        | SqlType::SignedInteger => 1i64.into(),
        SqlType::BigIntUnsigned(_) | SqlType::Unsigned | SqlType::UnsignedInteger => 1u64.into(),
        SqlType::MediumIntUnsigned(_) | SqlType::IntUnsigned(_) => 1u32.into(),
        SqlType::TinyInt(_) => 1i8.into(),
        SqlType::TinyIntUnsigned(_) => 1u8.into(),
        SqlType::SmallInt(_) | SqlType::Int2 => 1i16.into(),
        SqlType::SmallIntUnsigned(_) => 1u16.into(),
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
        SqlType::DateTime(_) | SqlType::Timestamp => NaiveDate::from_ymd_opt(2020, 1, 1)
            .unwrap()
            .and_hms_opt(12, 30, 45)
            .into(),
        SqlType::TimestampTz => DfValue::from(
            FixedOffset::west_opt(18_000)
                .unwrap()
                .with_ymd_and_hms(2020, 1, 1, 12, 30, 45)
                .single(),
        ),
        SqlType::Time => NaiveTime::from_hms_opt(12, 30, 45).into(),
        SqlType::Date => NaiveDate::from_ymd_opt(2020, 1, 1).into(),
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
        SqlType::Interval { .. } => unimplemented!(),
        SqlType::Array(t) => {
            let arr: Vec<_> = (0..3).map(|_| value_of_type(t)).collect();
            DfValue::Array(Arc::new(Array::from(arr)))
        }
        SqlType::Other(_) => unimplemented!(),
        SqlType::Point => unimplemented!(),
        SqlType::PostgisPoint => {
            DfValue::ByteArray(Arc::new(make_postgis_point_bytes(1.0, 2.0, None, true)))
        }
        SqlType::PostgisPolygon => {
            // A simple closed triangle, little-endian, no SRID.
            let ring = [(0.0, 0.0), (1.0, 0.0), (0.0, 1.0), (0.0, 0.0)];
            DfValue::ByteArray(Arc::new(make_postgis_polygon_bytes(
                Some(&ring),
                None,
                None,
                true,
            )))
        }
        SqlType::Tsvector => DfValue::None,
    }
}

/// Generate a random value with the given [`SqlType`]. The length of the value
/// is pulled from a uniform distribution over the set of possible ranges.
pub fn random_value_of_type<R>(typ: &SqlType, rng: &mut R) -> DfValue
where
    R: Rng + RngExt + ?Sized,
{
    match typ {
        SqlType::Char(Some(x)) | SqlType::VarChar(Some(x)) => {
            let length: usize = rng.random_range(1..=*x).into();
            "a".repeat(length).into()
        }
        SqlType::QuotedChar => rng.random::<i8>().into(),
        SqlType::Char(None) => "a".into(),
        SqlType::TinyBlob | SqlType::TinyText => {
            // 2^8 bytes
            let length: usize = rng.random_range(1..256);
            "a".repeat(length).into()
        }
        SqlType::Blob | SqlType::Text | SqlType::Citext | SqlType::VarChar(None) => {
            // 2^16 bytes
            let length: usize = rng.random_range(1..65536);
            "a".repeat(length).into()
        }
        SqlType::MediumBlob | SqlType::MediumText => {
            // 2^24 bytes
            // Currently capped at 65536 as these are generated in memory.
            let length: usize = rng.random_range(1..65536);
            "a".repeat(length).into()
        }
        SqlType::LongBlob | SqlType::LongText => {
            // 2^32 bytes
            // Currently capped at 65536 as these are generated in memory.
            let length: usize = rng.random_range(1..65536);
            "a".repeat(length).into()
        }
        SqlType::Binary(None) => {
            // 1 byte
            b"b".to_vec().into()
        }
        SqlType::Binary(Some(x)) | SqlType::VarBinary(x) => {
            let length: usize = rng.random_range(1..=*x).into();
            b"b".repeat(length).to_vec().into()
        }
        SqlType::ByteArray => {
            let length = rng.random_range(1..10);
            let mut array = Vec::new();
            for _ in 0..length {
                array.push(rng.random::<u8>());
            }
            DfValue::ByteArray(Arc::new(array))
        }
        SqlType::Int(_) | SqlType::Int4 => rng.random::<i32>().into(),
        SqlType::BigInt(_) | SqlType::Int8 | SqlType::Signed | SqlType::SignedInteger => {
            rng.random::<i64>().into()
        }
        SqlType::BigIntUnsigned(_) | SqlType::Unsigned | SqlType::UnsignedInteger => {
            rng.random::<u64>().into()
        }
        SqlType::IntUnsigned(_) => rng.random::<u32>().into(),
        SqlType::TinyInt(_) => rng.random::<i8>().into(),
        SqlType::TinyIntUnsigned(_) => rng.random::<u8>().into(),
        SqlType::SmallInt(_) | SqlType::Int2 => rng.random::<i16>().into(),
        SqlType::SmallIntUnsigned(_) => rng.random::<u16>().into(),
        SqlType::MediumInt(_) => rng.random_range((-1i32 << 23)..(1i32 << 23)).into(),
        SqlType::MediumIntUnsigned(_) => rng.random_range(0..(1u32 << 24)).into(),
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
            NaiveDate::from_ymd_opt(2020, rng.random_range(1..12), rng.random_range(1..28))
                .unwrap()
                .and_hms_opt(12, 30, 45)
                .into()
        }
        SqlType::TimestampTz => DfValue::from(
            FixedOffset::west_opt(18_000)
                .unwrap()
                .with_ymd_and_hms(
                    2020,
                    rng.random_range(1..12),
                    rng.random_range(1..28),
                    12,
                    30,
                    45,
                )
                .single(),
        ),
        SqlType::Time => NaiveTime::from_hms_opt(12, 30, 45).into(),
        SqlType::Date => {
            NaiveDate::from_ymd_opt(2020, rng.random_range(1..12), rng.random_range(1..28)).into()
        }
        SqlType::Bool => DfValue::from(rng.random_bool(0.5)),
        SqlType::Enum(_) => unimplemented!(),
        SqlType::Json | SqlType::Jsonb => DfValue::from(format!(
            "{{\"k\":\"{}\"}}",
            "a".repeat(rng.random_range(1..255))
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
            IpAddr::V4(Ipv4Addr::new(
                rng.random(),
                rng.random(),
                rng.random(),
                rng.random(),
            ))
            .to_string(),
        ),
        SqlType::Uuid => {
            let mut bytes = [0_u8; 16];
            rng.fill(&mut bytes);
            DfValue::from(uuid::Uuid::from_bytes(bytes).to_string())
        }
        SqlType::Bit(size_opt) => DfValue::from(BitVec::from_iter(
            rng.sample_iter(StandardUniform)
                .take(size_opt.unwrap_or(1) as usize)
                .collect::<Vec<bool>>(),
        )),
        SqlType::VarBit(max_size) => {
            let size = rng.random_range(0..max_size.unwrap_or(u16::MAX));
            DfValue::from(BitVec::from_iter(
                rng.sample_iter(StandardUniform)
                    .take(size as usize)
                    .collect::<Vec<bool>>(),
            ))
        }
        SqlType::Serial => ((rng.random::<u32>() + 1) as i32).into(),
        SqlType::BigSerial => ((rng.random::<u64>() + 1) as i64).into(),
        SqlType::Interval { .. } => unimplemented!(),
        SqlType::Array(t) => {
            let length: usize = rng.random_range(1..5);
            let arr: Vec<_> = (0..length).map(|_| random_value_of_type(t, rng)).collect();
            DfValue::Array(Arc::new(Array::from(arr)))
        }
        SqlType::Other(_) => unimplemented!(),
        SqlType::Point => unimplemented!(),
        SqlType::PostgisPoint => {
            let x = rng.random::<f64>() * 360.0 - 180.0;
            let y = rng.random::<f64>() * 180.0 - 90.0;
            DfValue::ByteArray(Arc::new(make_postgis_point_bytes(x, y, None, true)))
        }
        SqlType::PostgisPolygon => {
            let x = rng.random::<f64>() * 360.0 - 180.0;
            let y = rng.random::<f64>() * 180.0 - 90.0;
            let ring = [(x, y), (x + 1.0, y), (x, y + 1.0), (x, y)];
            DfValue::ByteArray(Arc::new(make_postgis_polygon_bytes(
                Some(&ring),
                None,
                None,
                true,
            )))
        }
        SqlType::Tsvector => DfValue::None,
    }
}

/// Generate a random value from a uniform distribution with the given integer
/// [`SqlType`] for a given range of values.If the range of `min` and `max`
/// exceeds the storage of the type, this truncates to fit.
fn uniform_random_value<R: Rng>(min: &DfValue, max: &DfValue, rng: &mut R) -> DfValue {
    match (min, max) {
        (DfValue::Int(i), DfValue::Int(j)) => rng.random_range(*i..*j).into(),
        (DfValue::UnsignedInt(i), DfValue::UnsignedInt(j)) => rng.random_range(*i..*j).into(),
        (_, _) => unimplemented!("DfValues unsupported for random uniform value generation"),
    }
}

/// Generate a unique value with the given [`SqlType`] from a monotonically increasing counter,
/// `idx`.
///
/// This is a bijective function (from `(idx, typ)` to the resultant [`DfValue`]).
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
        | SqlType::ByteArray => idx.to_string().into(),
        SqlType::Binary(None) => idx.to_string().as_bytes()[..1].to_vec().into(),
        SqlType::Binary(Some(len)) | SqlType::VarBinary(len) => {
            let s = idx.to_string();
            let b = s.as_bytes();
            b[..min(b.len(), *len as usize)].to_vec().into()
        }
        SqlType::VarChar(Some(len)) | SqlType::Char(Some(len)) => {
            let s = idx.to_string();
            (&s[..min(s.len(), *len as usize)]).into()
        }
        SqlType::Char(None) => (idx % 10).to_string().into(),
        SqlType::QuotedChar => (idx as i8).into(),
        SqlType::Int(_) | SqlType::Int4 => (idx as i32).into(),
        SqlType::BigInt(_) | SqlType::Int8 | SqlType::Signed | SqlType::SignedInteger => {
            (idx as i64).into()
        }
        SqlType::IntUnsigned(_) => idx.into(),
        SqlType::BigIntUnsigned(_) | SqlType::Unsigned | SqlType::UnsignedInteger => {
            (idx as u64).into()
        }
        SqlType::TinyInt(_) => {
            assert!(idx <= i8::MAX as u32, "generated too many TinyInts");
            (idx as i8).into()
        }
        SqlType::TinyIntUnsigned(_) => {
            assert!(idx <= u8::MAX as u32, "generated too many TinyIntUnsigneds");
            (idx as u8).into()
        }
        SqlType::SmallInt(_) | SqlType::Int2 => {
            assert!(idx <= i16::MAX as u32, "generated too many SmallInts");
            (idx as i16).into()
        }
        SqlType::SmallIntUnsigned(_) => {
            assert!(
                idx <= u16::MAX as u32,
                "generated too many SmallIntUnsigneds"
            );
            (idx as u16).into()
        }
        SqlType::MediumInt(_) => {
            assert!(idx < (1u32 << 23), "generated too many MediumInts");
            (idx as i32).into()
        }
        SqlType::MediumIntUnsigned(_) => {
            assert!(idx < (1u32 << 24), "generated too many MediumIntUnsigneds");
            idx.into()
        }
        SqlType::Float | SqlType::Double => (1.5 + idx as f64).try_into().unwrap(),
        SqlType::Real => (1.5 + idx as f32).try_into().unwrap(),
        SqlType::Decimal(prec, scale) => {
            Decimal::new(clamp_digits(*prec as _) as _, *scale as _).into()
        }
        SqlType::Numeric(prec_scale) => match prec_scale {
            Some((prec, None)) => Decimal::new(clamp_digits(*prec as _) as _, 1),
            Some((prec, Some(scale))) => Decimal::new(clamp_digits(*prec as _) as _, *scale as _),
            None => Decimal::new((15 + idx) as _, 2),
        }
        .into(),
        SqlType::DateTime(_) | SqlType::Timestamp => (NaiveDate::from_ymd_opt(2020, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            + Duration::minutes(idx as _))
        .into(),
        SqlType::TimestampTz => DfValue::from(
            FixedOffset::west_opt(18_000)
                .unwrap()
                .with_ymd_and_hms(2020, 1, 1, 12, 0, 0)
                .single()
                .expect("should have a value")
                + Duration::minutes(idx as _),
        ),
        SqlType::Date => {
            DfValue::from(NaiveDate::from_ymd_opt(1000, 1, 1).unwrap() + Duration::days(idx.into()))
        }
        SqlType::Enum(_) => unimplemented!(),
        SqlType::Bool => unimplemented!(),
        SqlType::Time => {
            (NaiveTime::from_hms_opt(0, 0, 0).unwrap() + Duration::seconds(idx as _)).into()
        }
        SqlType::Json | SqlType::Jsonb => DfValue::from(format!("{{\"k\": {idx}}}")),
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
        SqlType::Serial => ((idx + 1) as i32).into(),
        SqlType::BigSerial => ((idx + 1) as i64).into(),
        SqlType::Interval { .. } => unimplemented!(),
        SqlType::Array(t) => {
            let arr: Vec<_> = (0..4).map(|_| unique_value_of_type(t, idx)).collect();
            DfValue::Array(Arc::new(Array::from(arr)))
        }
        SqlType::Other(_) => unimplemented!(),
        SqlType::Point => unimplemented!(),
        SqlType::PostgisPoint => {
            let x = idx as f64;
            let y = (idx as f64) + 0.5;
            DfValue::ByteArray(Arc::new(make_postgis_point_bytes(x, y, None, true)))
        }
        SqlType::PostgisPolygon => {
            let x = idx as f64;
            let y = (idx as f64) + 0.5;
            let ring = [(x, y), (x + 1.0, y), (x, y + 1.0), (x, y)];
            DfValue::ByteArray(Arc::new(make_postgis_polygon_bytes(
                Some(&ring),
                None,
                None,
                true,
            )))
        }
        SqlType::Tsvector => DfValue::None,
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::SmallRng;
    use rand::SeedableRng;
    use readyset_data::DfValue;
    use readyset_sql::ast::SqlType;

    use super::*;

    #[test]
    fn zipfian_generator_handles_min_equals_max() {
        // `Zipf::new(0, _)` errors; the old code unwrapped and panicked.
        // Now `min == max` should be accepted and produce a deterministic
        // constant value instead of crashing the run.
        let mut rng = SmallRng::seed_from_u64(42);
        let mut g = ZipfianGenerator::new(DfValue::Int(7), DfValue::Int(7), 1.0, &mut rng);
        // Repeated calls must not panic; the value is whatever fallback
        // we agreed on (currently `min`).
        for _ in 0..100 {
            let _ = g.gen(&mut rng);
        }
    }

    #[test]
    fn zipfian_generator_does_not_panic_at_upper_bound() {
        // `Zipf::sample` returns f64 in `[1, n]` inclusive; rounding the
        // upper bound used to index `mapping.get(n)` and panic.
        // Run many samples across multiple seeds to flush out the boundary.
        for seed in 0u64..32 {
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut g = ZipfianGenerator::new(DfValue::Int(0), DfValue::Int(4), 5.0, &mut rng);
            for _ in 0..500 {
                let _ = g.gen(&mut rng);
            }
        }
    }

    /// Two generators created from the same spec with the same seed must
    /// produce identical sequences when given identical RNG streams.
    #[test]
    fn column_generator_is_deterministic_with_same_seed() {
        let specs: Vec<(ColumnGenerationSpec, SqlType)> = vec![
            (ColumnGenerationSpec::Random, SqlType::Int(None)),
            (
                ColumnGenerationSpec::Uniform(DfValue::Int(1), DfValue::Int(1000)),
                SqlType::Int(None),
            ),
            (
                ColumnGenerationSpec::Zipfian {
                    min: DfValue::Int(1),
                    max: DfValue::Int(100),
                    alpha: 1.0,
                },
                SqlType::Int(None),
            ),
            (
                ColumnGenerationSpec::RandomString("[a-z]{5,10}".to_string()),
                SqlType::VarChar(Some(255)),
            ),
        ];

        for (spec, sql_type) in &specs {
            let mut rng1 = SmallRng::seed_from_u64(42);
            let mut rng2 = SmallRng::seed_from_u64(42);

            let mut gen1 = spec.generator_for_col(sql_type.clone(), &mut rng1);
            let mut gen2 = spec.generator_for_col(sql_type.clone(), &mut rng2);

            let vals1: Vec<_> = (0..10).map(|_| gen1.gen(&mut rng1)).collect();
            let vals2: Vec<_> = (0..10).map(|_| gen2.gen(&mut rng2)).collect();

            assert_eq!(vals1, vals2, "non-deterministic for spec: {spec:?}");
        }
    }

    /// Parse generated PostGIS bytes back through the canonical readyset-spatial
    /// parser, asserting they form a well-formed geometry of the expected kind.
    #[track_caller]
    fn assert_postgis_round_trips(val: &DfValue, expected_prefix: &str) {
        let DfValue::ByteArray(bytes) = val else {
            panic!("expected ByteArray, got {val:?}");
        };
        let text = readyset_spatial::try_get_postgis_spatial_text(bytes, false)
            .expect("generated PostGIS bytes must parse");
        assert!(
            text.starts_with(expected_prefix),
            "expected {expected_prefix} geometry, got {text}"
        );
    }

    #[test]
    fn value_of_type_postgis_point_round_trips() {
        assert_postgis_round_trips(&value_of_type(&SqlType::PostgisPoint), "POINT");
    }

    #[test]
    fn value_of_type_postgis_polygon_round_trips() {
        assert_postgis_round_trips(&value_of_type(&SqlType::PostgisPolygon), "POLYGON");
    }

    #[test]
    fn random_value_of_type_postgis_round_trips() {
        let mut rng = SmallRng::seed_from_u64(42);
        assert_postgis_round_trips(
            &random_value_of_type(&SqlType::PostgisPoint, &mut rng),
            "POINT",
        );
        assert_postgis_round_trips(
            &random_value_of_type(&SqlType::PostgisPolygon, &mut rng),
            "POLYGON",
        );
    }

    #[test]
    fn unique_value_of_type_postgis_polygon_distinct_and_round_trips() {
        let v0 = unique_value_of_type(&SqlType::PostgisPolygon, 0);
        let v1 = unique_value_of_type(&SqlType::PostgisPolygon, 1);
        assert_postgis_round_trips(&v0, "POLYGON");
        assert_postgis_round_trips(&v1, "POLYGON");
        assert_ne!(v0, v1, "unique generator must return distinct values");
    }
}
