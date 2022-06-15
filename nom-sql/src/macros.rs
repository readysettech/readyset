#[cfg(test)]
macro_rules! test_parse {
    ($parser: expr, $src: expr) => {{
        let res = $parser($src);
        assert!(
            res.is_ok(),
            "res.err = {}",
            match res.err().unwrap() {
                nom::Err::Incomplete(n) => format!("Incomplete({:?})", n),
                nom::Err::Failure(nom::error::Error { input, code })
                | nom::Err::Error(nom::error::Error { input, code }) => {
                    format!("{:?}: at {}", code, String::from_utf8_lossy(input))
                }
            }
        );
        let (rem, res) = res.unwrap();
        assert!(
            rem.is_empty(),
            "non-empty remainder: \"{}\", parsed: {:?}",
            String::from_utf8_lossy(rem),
            res
        );
        res
    }};
}
