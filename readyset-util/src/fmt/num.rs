//! Utilities for formatting numbers.

use bytes::{BufMut, BytesMut};

struct Digits;

impl Digits {
    /// A lookup table that is used to efficiently convert integers between 0 and 99 to their
    /// padded, two-digit string representations.
    const DIGIT_TABLE: [&'static [u8; 2]; 100] = [
        b"00", b"01", b"02", b"03", b"04", b"05", b"06", b"07", b"08", b"09", b"10", b"11", b"12",
        b"13", b"14", b"15", b"16", b"17", b"18", b"19", b"20", b"21", b"22", b"23", b"24", b"25",
        b"26", b"27", b"28", b"29", b"30", b"31", b"32", b"33", b"34", b"35", b"36", b"37", b"38",
        b"39", b"40", b"41", b"42", b"43", b"44", b"45", b"46", b"47", b"48", b"49", b"50", b"51",
        b"52", b"53", b"54", b"55", b"56", b"57", b"58", b"59", b"60", b"61", b"62", b"63", b"64",
        b"65", b"66", b"67", b"68", b"69", b"70", b"71", b"72", b"73", b"74", b"75", b"76", b"77",
        b"78", b"79", b"80", b"81", b"82", b"83", b"84", b"85", b"86", b"87", b"88", b"89", b"90",
        b"91", b"92", b"93", b"94", b"95", b"96", b"97", b"98", b"99",
    ];

    fn get(n: u32) -> &'static [u8; 2] {
        unsafe { Self::DIGIT_TABLE.get_unchecked(n as usize) }
    }
}

trait PutU32Unchecked {
    fn put_one_digit(&mut self, n: u32, pos: usize);
    fn put_two_digits(&mut self, n: u32, pos: usize);
}

impl PutU32Unchecked for &mut [u8] {
    fn put_one_digit(&mut self, n: u32, pos: usize) {
        let slice = unsafe { self.get_unchecked_mut(pos) };
        *slice = b'0' + n as u8;
    }

    fn put_two_digits(&mut self, n: u32, pos: usize) {
        let slice = unsafe { self.get_unchecked_mut(pos..(pos + 2)) };
        slice[0..2].copy_from_slice(Digits::get(n));
    }
}

/// This function encodes the given `value` as a string into the given [`BytesMut`], padding it
/// with zeroes such that the encoded string never has fewer than `width` digits. This function
/// out-performs Rust's native formatter and should be used to format unsigned integers in
/// situations where performance is particularly important.
// Inspired by this Postgres function:
// https://github.com/postgres/postgres/blob/c6cf6d353c2865d82356ac86358622a101fde8ca/src/backend/utils/adt/numutils.c#L1270
pub fn write_padded_u32(mut value: u32, width: u32, dst: &mut BytesMut) {
    if value < 100 && width == 2 {
        dst.put_slice(Digits::get(value));
    } else {
        let num_digits = value.checked_ilog10().unwrap_or(0) + 1;
        let padding = width.saturating_sub(num_digits);

        for _ in 0..padding {
            dst.put_u8(b'0');
        }

        if value == 0 {
            dst.put_u8(b'0');
        } else {
            dst.reserve(num_digits as usize);
            let a = dst.len();

            for _ in 0..num_digits {
                dst.put_u8(0);
            }

            let mut buf = dst.as_mut();
            let mut i = 0;

            while value >= 10000 {
                let c = value - 10000 * (value / 10000);
                let c0 = c % 100;
                let c1 = c / 100;
                let pos = a + num_digits as usize - i;

                value /= 10000;

                buf.put_two_digits(c0, pos - 2);
                buf.put_two_digits(c1, pos - 4);
                i += 4;
            }

            if value >= 100 {
                let c = value % 100;
                let pos = a + num_digits as usize - i;

                value /= 100;

                buf.put_two_digits(c, pos - 2);
                i += 2;
            }

            if value >= 10 {
                let pos = a + num_digits as usize - i;

                buf.put_two_digits(value, pos - 2);
            } else {
                buf.put_one_digit(value, a);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;

    use bytes::BytesMut;
    use proptest::prelude::*;

    #[test]
    fn test_write_padded_u32_basic() {
        let mut buf = BytesMut::new();
        super::write_padded_u32(1, 1, &mut buf);
        assert_eq!(b"1".as_slice(), buf);
    }

    #[test]
    fn test_write_padded_u32_basic_with_padding() {
        let mut buf = BytesMut::new();
        super::write_padded_u32(1, 2, &mut buf);
        assert_eq!(b"01".as_slice(), buf);
    }

    #[test]
    fn test_write_padded_u32_multi_digit() {
        let mut buf = BytesMut::new();
        super::write_padded_u32(10000, 5, &mut buf);
        assert_eq!(b"10000".as_slice(), buf);
    }

    #[test]
    fn test_write_padded_u32_multi_digit_with_padding() {
        let mut buf = BytesMut::new();
        super::write_padded_u32(10000, 6, &mut buf);
        assert_eq!(b"010000".as_slice(), buf);
    }

    #[test]
    fn test_write_padded_u32_many_pad_digits() {
        let mut buf = BytesMut::new();
        super::write_padded_u32(1, 10, &mut buf);
        assert_eq!(b"0000000001".as_slice(), buf);
    }

    #[test]
    fn test_write_padded_u32_large_number() {
        let mut buf = BytesMut::new();
        // 4_294_967_295 is u32::MAX
        super::write_padded_u32(4_294_967_295, 14, &mut buf);
        assert_eq!(b"00004294967295".as_slice(), buf);
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 10_000,
            ..ProptestConfig::default()
        })]

        #[test]
        fn proptest_write_padded_u32(i in u32::MIN..=u32::MAX, min_width in 0..16u32) {
            let mut actual = BytesMut::new();
            super::write_padded_u32(i, min_width, &mut actual);

            let mut expected = BytesMut::new();
            write!(expected, "{:01$}", i, min_width as usize).unwrap();

            assert_eq!(actual, expected);
        }
    }
}
