use bit_vec::BitVec;
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
    let nullmap = BitVec::from_bytes(&input[..nullmap_len]);

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
            if nullmap[i] {
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
