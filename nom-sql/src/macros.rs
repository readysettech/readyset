#[cfg(test)]
macro_rules! test_parse {
    ($parser: expr, $src: expr) => {{
        let res = crate::to_nom_result($parser(nom_locate::LocatedSpan::new($src)));
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

#[cfg(test)]
macro_rules! test_format_parse_round_trip {
    ($($name:ident($parser: expr, $type: ty, $dialect: expr);)+) => {
        $(test_format_parse_round_trip!(@impl, $name, $parser, $type, $dialect);)+
    };

    (@impl, $name: ident, $parser: expr, $type: ty, $dialect: expr) => {
        #[test_strategy::proptest]
        fn $name(s: $type) {
            let formatted = s.display($dialect).to_string();
            let round_trip = $parser($dialect)(LocatedSpan::new(formatted.as_bytes()));

            if round_trip.is_err() {
                println!("{}", formatted);
                println!("{:?}", &s);
            }
            let (_, limit) = round_trip.unwrap();
            if limit != s {
                println!("{}", formatted);
            }
            assert_eq!(limit, s);
        }
    };
}
