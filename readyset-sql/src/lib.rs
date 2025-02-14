/// Errors that can occur when converting a sqlparser rs AST to a Readyset AST
#[derive(Debug, thiserror::Error)]
pub enum AstConversionError {
    /// A conversion failed because an internal invariant was broken, the Readyset AST didn't look
    /// how we expected, or an auxiliary conversion (such as number parsing) failed
    #[error("Conversion unexpectedly failed: {0}")]
    Failed(String),
    /// A conversion was not supported because Readyset cannot process this statement
    #[error("Conversion not supported: {0}")]
    Unsupported(String),
    /// A conversion was not completed because it is pending implementation
    #[error("Conversion not yet implemented: {0}")]
    NotYetImplemented(String),
    /// A conversion is not needed because Readyset does not need to process this kind of statement
    #[error("Conversion skipped: {0}")]
    Skipped(String),
}

macro_rules! ast_conversion_err {
    ($kind:ident, $e:expr) => {
        AstConversionError::$kind(format!(
            "at {}:{}:{}: {}",
            std::file!(),
            std::line!(),
            std::column!(),
            $e
        ))
    };
}

macro_rules! failed_err {
    ($($format_args:tt)*) => {
        ast_conversion_err!(Failed, format!($($format_args)*))
    };
}

macro_rules! failed {
    ($($format_args:tt)*) => {
        Err(failed_err!($($format_args)*))
    };
}

macro_rules! unsupported_err {
    ($($format_args:tt)*) => {
        ast_conversion_err!(Unsupported, format!($($format_args)*))
    };
}

macro_rules! unsupported {
    ($($format_args:tt)*) => {
        Err(unsupported_err!($($format_args)*))
    };
}

macro_rules! not_yet_implemented_err {
    ($($format_args:tt)*) => {
        ast_conversion_err!(NotYetImplemented, format!($($format_args)*))
    };
}

macro_rules! not_yet_implemented {
    ($($format_args:tt)*) => {
        Err(not_yet_implemented_err!($($format_args)*))
    };
}

macro_rules! skipped_err {
    ($($format_args:tt)*) => {
        ast_conversion_err!(Skipped, format!($($format_args)*))
    };
}

macro_rules! skipped {
    ($($format_args:tt)*) => {
        Err(skipped_err!($($format_args)*))
    };
}

pub mod analysis;
pub mod ast;
pub mod dialect;
pub mod dialect_display;

pub use dialect::Dialect;
pub use dialect_display::DialectDisplay;

pub trait TryFromDialect<T>: Sized {
    fn try_from_dialect(value: T, dialect: Dialect) -> Result<Self, AstConversionError>;
}

pub trait TryIntoDialect<T>: Sized {
    fn try_into_dialect(self, dialect: Dialect) -> Result<T, AstConversionError>;
}

impl<T, U> TryIntoDialect<U> for T
where
    U: TryFromDialect<T>,
{
    #[inline]
    fn try_into_dialect(self, dialect: Dialect) -> Result<U, AstConversionError> {
        U::try_from_dialect(self, dialect)
    }
}

pub trait FromDialect<T> {
    fn from_dialect(value: T, dialect: Dialect) -> Self;
}

pub trait IntoDialect<T>: Sized {
    #[must_use]
    fn into_dialect(self, dialect: Dialect) -> T;
}

impl<T, U> IntoDialect<U> for T
where
    U: FromDialect<T>,
{
    #[inline]
    fn into_dialect(self, dialect: Dialect) -> U {
        U::from_dialect(self, dialect)
    }
}
