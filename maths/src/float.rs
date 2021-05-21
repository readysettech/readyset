/// Float holds utility functions that are re-used for various floating point conversions.

/// Decodes a floating point number into a mantissa-exponent-sign triplet. This preserves all of
/// the original data in a lossless way.
/// Ref: https://github.com/rust-lang/rust/blob/5c674a11471ec0569f616854d715941757a48a0a/src/libcore/num/f64.rs#L203-L216.
pub fn decode_f64(f: f64) -> (u64, i16, i8) {
    let bits: u64 = f.to_bits();
    let sign: i8 = if bits >> 63 == 0 { 1 } else { -1 };
    let mut exponent: i16 = ((bits >> 52) & 0x7ff) as i16;
    let mantissa = if exponent == 0 {
        (bits & 0xfffffffffffff) << 1
    } else {
        (bits & 0xfffffffffffff) | 0x10000000000000
    };

    exponent -= 1023 + 52;
    (mantissa, exponent, sign)
}

/// Encodes a mantissa, exponent and sign representation of a floating point number into an f64.
pub fn encode_f64(mantissa: u64, exponent: i16, sign: i8) -> f64 {
    (mantissa as f64) * (sign as f64) * 2.0_f64.powf(exponent as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lossless_decode_encode() {
        let want = 5.0_f64;
        let (mantissa, exponent, sign) = decode_f64(want);
        let got = encode_f64(mantissa, exponent, sign);
        assert_eq!(got, want);
    }

    #[test]
    fn zero_converts_correctly() {
        let want = 0.0_f64;
        let (mantissa, exponent, sign) = decode_f64(want);
        let got = encode_f64(mantissa, exponent, sign);
        assert_eq!(got, want);
    }

    #[test]
    fn neg_converts_correctly() {
        let want = -1.0_f64;
        let (mantissa, exponent, sign) = decode_f64(want);
        let got = encode_f64(mantissa, exponent, sign);
        assert_eq!(got, want);
    }

    #[test]
    fn f64_max_converts_correctly() {
        let want = f64::MAX;
        let (mantissa, exponent, sign) = decode_f64(want);
        let got = encode_f64(mantissa, exponent, sign);
        assert_eq!(got, want);
    }

    #[test]
    fn f64_min_converts_correctly() {
        let want = f64::MIN;
        let (mantissa, exponent, sign) = decode_f64(want);
        let got = encode_f64(mantissa, exponent, sign);
        assert_eq!(got, want);
    }

    #[test]
    fn f64_min_pos_converts_correctly() {
        let want = f64::MIN_POSITIVE;
        let (mantissa, exponent, sign) = decode_f64(want);
        let got = encode_f64(mantissa, exponent, sign);
        assert_eq!(got, want);
    }
}
