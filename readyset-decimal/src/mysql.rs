use mysql_common::Value;

use crate::Decimal;

impl From<&Decimal> for Value {
    fn from(value: &Decimal) -> Self {
        value.to_string().into()
    }
}
