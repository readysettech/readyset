#[cfg(test)]
macro_rules! test_parse {
    ($parser: expr, $src: expr) => {{
        let res = $parser($src);
        assert!(res.is_ok(), "res.err = {}", {
            let res = res.err().unwrap();
            let code = res.kind;
            let input = res.input;
            format!("{:?}: at {}", code, String::from_utf8_lossy(*input))
        });
        let (rem, res) = res.unwrap();
        assert!(
            rem.is_empty(),
            "non-empty remainder: \"{}\", parsed: {:?}",
            String::from_utf8_lossy(*rem),
            res
        );
        res
    }};
}
