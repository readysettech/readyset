use crate::myc;
use std::collections::HashMap;
use crate::{StatementData, Value};

/// A `ParamParser` decodes query parameters included in a client's `EXECUTE` command given
/// type information for the expected parameters.
///
/// Users should invoke [`iter`](struct.ParamParser.html#method.iter) method to iterate over the
/// provided parameters.
pub struct ParamParser<'a> {
    pub(crate) params: u16,
    pub(crate) bytes: &'a [u8],
    pub(crate) long_data: &'a HashMap<u16, Vec<u8>>,
    pub(crate) bound_types: &'a mut Vec<(myc::constants::ColumnType, bool)>,
}

impl<'a> ParamParser<'a> {
    pub(crate) fn new(input: &'a [u8], stmt: &'a mut StatementData) -> Self {
        ParamParser {
            params: stmt.params,
            bytes: input,
            long_data: &stmt.long_data,
            bound_types: &mut stmt.bound_types,
        }
    }
}

impl<'a> IntoIterator for ParamParser<'a> {
    type IntoIter = Params<'a>;
    type Item = ParamValue<'a>;
    fn into_iter(self) -> Params<'a> {
        Params {
            params: self.params,
            input: self.bytes,
            nullmap: None,
            col: 0,
            long_data: self.long_data,
            bound_types: self.bound_types,
        }
    }
}

/// An iterator over parameters provided by a client in an `EXECUTE` command.
pub struct Params<'a> {
    params: u16,
    input: &'a [u8],
    nullmap: Option<&'a [u8]>,
    col: u16,
    long_data: &'a HashMap<u16, Vec<u8>>,
    bound_types: &'a mut Vec<(myc::constants::ColumnType, bool)>,
}

/// A single parameter value provided by a client when issuing an `EXECUTE` command.
pub struct ParamValue<'a> {
    /// The value provided for this parameter.
    pub value: Value<'a>,
    /// The column type assigned to this parameter.
    pub coltype: myc::constants::ColumnType,
}

impl<'a> Iterator for Params<'a> {
    type Item = ParamValue<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.nullmap.is_none() {
            let nullmap_len = (self.params as usize + 7) / 8;
            let (nullmap, rest) = self.input.split_at(nullmap_len);
            self.nullmap = Some(nullmap);
            self.input = rest;

            if !rest.is_empty() && rest[0] != 0x00 {
                let (typmap, rest) = rest[1..].split_at(2 * self.params as usize);
                self.bound_types.clear();
                for i in 0..self.params as usize {
                    self.bound_types.push((
                        myc::constants::ColumnType::from(typmap[2 * i as usize]),
                        (typmap[2 * i as usize + 1] & 128) != 0,
                    ));
                }
                self.input = rest;
            }
        }

        if self.col >= self.params {
            return None;
        }
        let pt = &self.bound_types[self.col as usize];

        // https://web.archive.org/web/20170404144156/https://dev.mysql.com/doc/internals/en/null-bitmap.html
        // NULL-bitmap-byte = ((field-pos + offset) / 8)
        // NULL-bitmap-bit  = ((field-pos + offset) % 8)
        if let Some(nullmap) = self.nullmap {
            let byte = self.col as usize / 8;
            if byte >= nullmap.len() {
                return None;
            }
            if (nullmap[byte] & 1u8 << (self.col % 8)) != 0 {
                self.col += 1;
                return Some(ParamValue {
                    value: Value::null(),
                    coltype: pt.0,
                });
            }
        } else {
            unreachable!();
        }

        let v = if let Some(data) = self.long_data.get(&self.col) {
            Value::bytes(&data[..])
        } else {
            Value::parse_from(&mut self.input, pt.0, pt.1).unwrap()
        };
        self.col += 1;
        Some(ParamValue {
            value: v,
            coltype: pt.0,
        })
    }
}
