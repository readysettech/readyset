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
/// Generates a round-trip test that displays and then parses a provided type with a provided
/// parser.
///
/// Optionally, a prop_assume!() can be injected by providing a closure fn over the type that
/// returns a boolean
///
/// For example, to limit the `Variable` generation to only those with SqlIdentifiers that are
/// parseable (so keywords will be rejected):
/// ```
/// rt_variable(variable, Variable, Dialect::PostgreSQL, {
///     |s: &Variable| {
///         let name = s.name.to_string();
///         Dialect::PostgreSQL
///             .identifier()
///             .map(|ident| ident.to_ascii_lowercase())
///             .parse(LocatedSpan::new(name.as_bytes()))
///             .is_ok()
///     }
/// });
/// ```
macro_rules! test_format_parse_round_trip {
    ($($name:ident($parser: expr, $type: ty, $dialect: expr $(, $prop_assume_fn: block)?);)+) => {
        $(test_format_parse_round_trip!(@impl, $name, $parser, $type, $dialect $(, $prop_assume_fn)?);)+
    };

    (@impl, $name: ident, $parser: expr, $type: ty, $dialect: expr $(, $prop_assume_fn: block)? ) => {
        #[test_strategy::proptest]
        fn $name(s: $type) {
            $(proptest::prop_assume!($prop_assume_fn(&s));)?

            let displayed = s.display($dialect).to_string();
            // Do one extra format->parse trip to deal with things that are ambiguous at the
            // Display level.
            // For example, a Numeric(1) could be displayed as `1` and then parsed as an Integer
            let parsed = $parser($dialect)(LocatedSpan::new(displayed.as_bytes()));
            if parsed.is_err() {
                println!("{}", displayed);
                println!("{:?}", &s);
            }
            let (_, parsed) = parsed.unwrap();
            let displayed = parsed.display($dialect).to_string();
            let round_trip = $parser($dialect)(LocatedSpan::new(displayed.as_bytes()));
            if round_trip.is_err() {
                println!("{}", displayed);
                println!("{:?}", &s);
            }

            let (_, round_trip) = round_trip.unwrap();
            if round_trip != parsed {
                println!("{}", displayed);
            }
            assert_eq!(round_trip, parsed);
        }
    };
}
