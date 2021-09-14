#[cfg(test)]
macro_rules! test_parse {
    ($parser: expr, $src: expr) => {{
        let res = $parser($src);
        assert!(
            res.is_ok(),
            "res.err = {}",
            match res.err().unwrap() {
                nom::Err::Incomplete(_) => "Incomplete".to_owned(),
                nom::Err::Failure((i, k)) | nom::Err::Error((i, k)) => {
                    format!("{:?}: at {}", k, String::from_utf8_lossy(i))
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
