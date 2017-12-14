use nom::IResult;
use Column;
use myc;

use std::borrow::Borrow;
pub fn parse<'a, C>(mut input: &'a [u8], ptypes: &[C]) -> IResult<&'a [u8], Vec<myc::value::Value>>
where
    C: Borrow<Column>,
{
    if ptypes.is_empty() {
        assert!(input.is_empty());
        return Ok((input, Vec::new()));
    }

    let nullmap_len = (ptypes.len() + 7) / 8;
    let nullmap = &input[..nullmap_len];

    let ptypes2 = if input[nullmap_len] != 0x00 {
        input = &input[(nullmap_len + 1)..];
        let ps: Vec<_> = (0..ptypes.len())
            .map(|i| {
                (
                    myc::constants::ColumnType::from(input[2 * i]),
                    (input[2 * i + 1] & 128) != 0,
                )
            })
            .collect();
        input = &input[(2 * ptypes.len())..];
        Some(ps)
    } else {
        None
    };

    let ps = ptypes
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let c = c.borrow();

            // https://web.archive.org/web/20170404144156/https://dev.mysql.com/doc/internals/en/null-bitmap.html
            // NULL-bitmap-byte = ((field-pos + offset) / 8)
            // NULL-bitmap-bit  = ((field-pos + offset) % 8)
            if (nullmap[i / 8] & 1u8 << (i % 8)) != 0 {
                return myc::value::Value::NULL;
            }

            let (ct, unsigned) = ptypes2.as_ref().map(|p| p[i]).unwrap_or_else(move || {
                (
                    c.coltype,
                    c.colflags
                        .contains(myc::constants::ColumnFlags::UNSIGNED_FLAG),
                )
            });
            myc::value::read_bin_value(&mut input, ct, unsigned).unwrap()
        })
        .collect();
    Ok((input, ps))
}
