/// Int holds utility functions that are re-used for various integer related math needs.

/// Round the given integer provided negative precision. No-op if precision is positive (decimal
/// rounding)
pub fn integer_rnd(val: i128, prec: i32) -> i128 {
    if prec > 0 {
        // No-op case.
        return val;
    }
    ((val as f64 / 10.0_f64.powf(-(prec as f64))).round() * 10.0_f64.powf(-(prec as f64))) as i128
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_rnd_skips_pos_prec() {
        let want = 53;
        let got = integer_rnd(want, 20);
        assert_eq!(got, want);
    }

    #[test]
    fn integer_rnd_works() {
        let want = 1000;
        let got = integer_rnd(888, -3);
        assert_eq!(got, want);
    }
}
