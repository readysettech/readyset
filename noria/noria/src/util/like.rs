//! Implementation of the semantics of SQL's LIKE operator
//!
//! Within LIKE patterns:
//!
//! * `%` represents any string, of any length
//! * `_` represents any single character
//! * `\%` represents a literal `%` character
//! * `\_` represents a literal `_` character

use lazy_static::lazy_static;
use regex::{Error, Regex};

/// Case-sensitivity mode for a [`LikePattern`]
#[derive(Debug, Eq, PartialEq)]
pub enum CaseSensitivityMode {
    /// Match case-sentitively
    CaseSensitive,

    /// Match case-insentitively
    CaseInsensitive,
}
use crate::{ReadySetError, ReadySetResult};
pub use CaseSensitivityMode::*;

impl Default for CaseSensitivityMode {
    fn default() -> Self {
        CaseSensitive
    }
}

struct LikeTokenReplacer;
impl regex::Replacer for LikeTokenReplacer {
    fn replace_append(&mut self, caps: &regex::Captures<'_>, dst: &mut String) {
        // According to the docs from `regex::Captures`, the first group always
        // exists and it corresponds to the entire match. So, it's allowed to
        // get the 0th position through index slicing.
        #[allow(clippy::indexing_slicing)]
        match &caps[0] {
            "%" => dst.push_str(".*"),
            "_" => dst.push('.'),
            r"\%" => dst.push('%'),
            r"\_" => dst.push('_'),
            s
            @
            ("{" | "}" | "." | "*" | "+" | "?" | "|" | "(" | ")" | "[" | "]" | "$" | "^"
            | r"\") => {
                dst.push('\\');
                dst.push_str(s);
            }
            s => dst.push_str(s),
        }
    }
}

fn like_to_regex(like_pattern: &str, mode: CaseSensitivityMode) -> ReadySetResult<Regex> {
    lazy_static! {
        static ref TOKEN: Regex = {
            // We have a hardocded string so we know this will not fail to create a valid
            // regex.
            #[allow(clippy::unwrap_used)]
            Regex::new(r"(\\?[%_])|[{}.*+?|()\[\]\\$^]").unwrap()
        };
    }
    let mut re = if mode == CaseInsensitive {
        "(?i)^".to_string()
    } else {
        "^".to_string()
    };
    re.push_str(&TOKEN.replace_all(like_pattern, LikeTokenReplacer));
    re.push('$');
    Regex::new(&re).map_err(|e| {
        ReadySetError::BadRequest(match e {
            Error::Syntax(s) => format!("syntax error: '{}'", s),
            Error::CompiledTooBig(l) => format!("size limit exceeded. Limit: {}", l),
            _ => "could not create regex".to_owned(),
        })
    })
}

/// Representation for a LIKE or ILIKE pattern
pub struct LikePattern {
    regex: Regex,
}

impl LikePattern {
    /// Construct a new LIKE pattern from the given string and [`CaseSensitivityMode`].
    ///
    /// This will do some work, so should be done ideally at most once per pattern.
    pub fn new(pat: &str, case_sensitivity_mode: CaseSensitivityMode) -> ReadySetResult<Self> {
        Ok(Self {
            regex: like_to_regex(pat, case_sensitivity_mode)?,
        })
    }

    /// Returns true if this LikePattern matches the given string.
    pub fn matches(&self, s: &str) -> bool {
        self.regex.is_match(s)
    }
}

// /// Converts to a [`CaseSensitive`] pattern
// impl From<&str> for LikePattern {
//     fn from(s: &str) -> Self {
//         Self::new(s, CaseSensitive)
//     }
// }
//
// /// Converts to a [`CaseSensitive`] pattern
// impl From<String> for LikePattern {
//     fn from(s: String) -> Self {
//         Self::new(&s, CaseSensitive)
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use test_strategy::proptest;

    #[test]
    fn like_matching() {
        assert!(LikePattern::new("foo%", CaseSensitive)
            .unwrap()
            .matches("foobar"));
        assert!(!LikePattern::new("foo%", CaseSensitive)
            .unwrap()
            .matches("oofoobar"));
        assert!(LikePattern::new("%foo%", CaseSensitive)
            .unwrap()
            .matches("oofoobar"));
        assert!(LikePattern::new("%foo___", CaseSensitive)
            .unwrap()
            .matches("oofoobar"));
        assert!(!LikePattern::new("%foo___", CaseSensitive)
            .unwrap()
            .matches("oofoobarr"));
        assert!(!LikePattern::new("%foo%", CaseSensitive)
            .unwrap()
            .matches("ooFOOoo"));
    }

    #[test]
    fn ilike_matching() {
        assert!(LikePattern::new("%foo%", CaseInsensitive)
            .unwrap()
            .matches("oofoobar"));
        assert!(LikePattern::new("%foo%", CaseInsensitive)
            .unwrap()
            .matches("ooFOObar"));
    }

    #[test]
    fn ilike_matching_unicode() {
        assert!(LikePattern::new("σ%Σ%σ%", CaseInsensitive)
            .unwrap()
            .matches("Σomebody σet up uσ the bomb"));
    }

    #[test]
    fn escapes() {
        assert!(LikePattern::new(r"\%", CaseSensitive).unwrap().matches("%"));
        assert!(!LikePattern::new(r"\%", CaseSensitive)
            .unwrap()
            .matches(r"\foo"));
        assert!(LikePattern::new(r"\_", CaseSensitive).unwrap().matches("_"));
        assert!(!LikePattern::new(r"\_", CaseSensitive)
            .unwrap()
            .matches(r"\a"));
    }

    #[proptest]
    fn pattern_matches_itself(pat: String) {
        lazy_static! {
            static ref ESCAPER: Regex = Regex::new(r"(\\)+(?P<tok>[%_])").unwrap();
        }
        let pat = ESCAPER.replace_all(&pat, "$tok");
        let pattern = LikePattern::new(&pat, CaseSensitive).unwrap();
        assert!(pattern.matches(&pat));
    }
}
