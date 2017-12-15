use std::borrow::Borrow;
use {Column, Value};
use myc;

/// A `ParamParser` decodes query parameters included in a client's `EXECUTE` command given
/// type information for the expected parameters.
///
/// Users should invoke [`iter`](struct.ParamParser.html#method.iter) method to iterate over the
/// provided parameters.
pub struct ParamParser<'a>(pub(crate) &'a [u8]);

impl<'a> ParamParser<'a> {
    /// Produces an iterator over the parameters supplied by the client given the expected types of
    /// those parameters.
    ///
    /// This method may be called multiple times to generate multiple independent iterators, but
    /// beware that this will also parse the underlying client byte-stream again.
    pub fn iter<I, E>(&self, types: I) -> Params<'a, <I as IntoIterator>::IntoIter>
    where
        I: IntoIterator<Item = E>,
        E: Borrow<Column>,
    {
        Params {
            input: self.0,
            nullmap: None,
            typmap: None,
            types: types.into_iter(),
            col: 0,
        }
    }
}

/// An iterator over parameters provided by a client in an `EXECUTE` command.
pub struct Params<'a, I> {
    input: &'a [u8],
    nullmap: Option<&'a [u8]>,
    typmap: Option<&'a [u8]>,
    types: I,
    col: usize,
}

impl<'a, I, E> Iterator for Params<'a, I>
where
    I: Iterator<Item = E> + ExactSizeIterator,
    E: Borrow<Column>,
{
    type Item = Value<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.nullmap.is_none() {
            let nullmap_len = (self.types.len() + 7) / 8;
            let (nullmap, rest) = self.input.split_at(nullmap_len);
            self.nullmap = Some(nullmap);
            self.input = rest;

            if !rest.is_empty() && rest[0] != 0x00 {
                let (typmap, rest) = rest[1..].split_at(2 * self.types.len());
                self.typmap = Some(typmap);
                self.input = rest;
            }
        }

        let pt = self.types.next()?;

        // https://web.archive.org/web/20170404144156/https://dev.mysql.com/doc/internals/en/null-bitmap.html
        // NULL-bitmap-byte = ((field-pos + offset) / 8)
        // NULL-bitmap-bit  = ((field-pos + offset) % 8)
        if let Some(ref nullmap) = self.nullmap {
            let byte = self.col / 8;
            if byte >= nullmap.len() {
                return None;
            }
            if (nullmap[byte] & 1u8 << (self.col % 8)) != 0 {
                self.col += 1;
                return Some(Value::null());
            }
        } else {
            unreachable!();
        }

        let pt = self.typmap
            .map(|typmap| {
                (
                    myc::constants::ColumnType::from(typmap[2 * self.col]),
                    (typmap[2 * self.col + 1] & 128) != 0,
                )
            })
            .unwrap_or_else(move || {
                let pt = pt.borrow();
                (
                    pt.coltype,
                    pt.colflags
                        .contains(myc::constants::ColumnFlags::UNSIGNED_FLAG),
                )
            });

        self.col += 1;
        Some(Value::parse_from(&mut self.input, pt.0, pt.1).unwrap())
    }
}
