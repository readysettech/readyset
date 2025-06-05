//! Implementation of the semantics of SQL's LIKE operator
//!
//! Within LIKE patterns:
//!
//! * `%` represents any string, of any length
//! * `_` represents any single character
//! * `\%` represents a literal `%` character
//! * `\_` represents a literal `_` character

use lazy_static::lazy_static;
use regex::Regex;

/// Case-sensitivity mode for a [`LikePattern`]
#[derive(Debug, Eq, PartialEq, Default)]
pub enum CaseSensitivityMode {
    /// Match case-sentitively
    #[default]
    CaseSensitive,

    /// Match case-insentitively
    CaseInsensitive,
}
pub use CaseSensitivityMode::*;

struct LikeTokenReplacer;
impl regex::Replacer for LikeTokenReplacer {
    fn replace_append(&mut self, caps: &regex::Captures<'_>, dst: &mut String) {
        // According to the docs from `regex::Replacer::replace_append`, the current match to be
        // replaced always exists in `caps[0]`.
        match &caps[0] {
            // Handle unescaped LIKE pattern characters
            "%" => dst.push_str(".*"),
            "_" => dst.push('.'),
            // Handle regex metacharacters
            s @ ("{" | "}" | "." | "*" | "+" | "?" | "|" | "(" | ")" | "[" | "]" | "$" | "^") => {
                dst.push('\\');
                dst.push_str(s);
            }
            // Handle escaped characters
            s if s.starts_with('\\') => {
                let c = s.chars().nth(1).unwrap(); // Guaranteed by `TOKEN` regex: `\\.`
                match c {
                    '\\' => dst.push_str(r"\\"),
                    // Re-handle regex metacharacters
                    '{' | '}' | '.' | '*' | '+' | '?' | '|' | '(' | ')' | '[' | ']' | '$' | '^' => {
                        dst.push('\\');
                        dst.push(c);
                    }
                    // For any other escaped character (including _ and %), treat it literally
                    _ => {
                        dst.push(c);
                    }
                }
            }
            s => dst.push_str(s),
        }
    }
}

fn like_to_regex(like_pattern: &str, mode: CaseSensitivityMode) -> Regex {
    lazy_static! {
        static ref TOKEN: Regex = {
            #[allow(clippy::unwrap_used)]
            // Regex is hardcoded. This pattern matches:
            // 1. Special LIKE characters (% or _)
            // 2. Special regex metacharacters that need escaping
            // 3. An escaped character (\\.)
            Regex::new(r"[%_]|[{}.*+?|()\[\]$^]|\\.").unwrap()
        };
    }
    let mut re = if mode == CaseInsensitive {
        "(?i)^".to_string()
    } else {
        "^".to_string()
    };
    re.push_str(&TOKEN.replace_all(like_pattern, LikeTokenReplacer));
    re.push('$');
    #[allow(clippy::expect_used)]
    // We escape all regex characters that could cause regex construction to fail, so there's no way
    // this expect can actually get hit (and there's a property test asserting that)
    Regex::new(&re).expect("Like pattern compiled to invalid regex")
}

/// Representation for a LIKE or ILIKE pattern
pub struct LikePattern {
    regex: Regex,
}

impl LikePattern {
    /// Construct a new LIKE pattern from the given string and [`CaseSensitivityMode`].
    ///
    /// This will do some work, so should be done ideally at most once per pattern.
    pub fn new(pat: &str, case_sensitivity_mode: CaseSensitivityMode) -> Self {
        Self {
            regex: like_to_regex(pat, case_sensitivity_mode),
        }
    }

    /// Returns true if this LikePattern matches the given string.
    pub fn matches(&self, s: &str) -> bool {
        self.regex.is_match(s)
    }
}

/// Converts to a [`CaseSensitive`] pattern
impl From<&str> for LikePattern {
    fn from(s: &str) -> Self {
        Self::new(s, CaseSensitive)
    }
}

/// Converts to a [`CaseSensitive`] pattern
impl From<String> for LikePattern {
    fn from(s: String) -> Self {
        Self::new(&s, CaseSensitive)
    }
}

#[cfg(test)]
mod tests {
    use test_strategy::proptest;
    use test_utils::tags;

    use super::*;

    #[test]
    fn like_matching() {
        assert!(LikePattern::new("foo%", CaseSensitive).matches("foobar"));
        assert!(!LikePattern::new("foo%", CaseSensitive).matches("oofoobar"));
        assert!(LikePattern::new("%foo%", CaseSensitive).matches("oofoobar"));
        assert!(LikePattern::new("%foo___", CaseSensitive).matches("oofoobar"));
        assert!(!LikePattern::new("%foo___", CaseSensitive).matches("oofoobarr"));
        assert!(!LikePattern::new("%foo%", CaseSensitive).matches("ooFOOoo"));
    }

    #[test]
    fn ilike_matching() {
        assert!(LikePattern::new("%foo%", CaseInsensitive).matches("oofoobar"));
        assert!(LikePattern::new("%foo%", CaseInsensitive).matches("ooFOObar"));
    }

    #[test]
    fn ilike_matching_unicode() {
        assert!(LikePattern::new("σ%Σ%σ%", CaseInsensitive).matches("Σomebody σet up uσ the bomb"));
    }

    #[test]
    fn escapes() {
        assert!(LikePattern::new(r"foo\bar", CaseSensitive).matches(r"foobar"));
        assert!(LikePattern::new(r"foo\\bar", CaseSensitive).matches(r"foo\bar"));
        assert!(LikePattern::new(r"\%", CaseSensitive).matches("%"));
        assert!(!LikePattern::new(r"\%", CaseSensitive).matches(r"\foo"));
        assert!(LikePattern::new(r"\_", CaseSensitive).matches("_"));
        assert!(!LikePattern::new(r"\_", CaseSensitive).matches(r"\a"));
        assert!(LikePattern::new(r"\\", CaseSensitive).matches(r"\"));
        assert!(LikePattern::new(r"\a", CaseSensitive).matches("a"));
        assert!(LikePattern::new(r"\\%", CaseSensitive).matches(r"\foo"));
        assert!(LikePattern::new(r"\\\%", CaseSensitive).matches(r"\%"));
        assert!(!LikePattern::new(r"\a", CaseSensitive).matches("b"));
        for c in r"{}.*+?|()[]$^".chars() {
            assert!(LikePattern::new(&c.to_string(), CaseSensitive).matches(&c.to_string()));
        }
        for c in r"%_\{}.*+?|()[]$^".chars() {
            assert!(LikePattern::new(&format!(r"\{c}"), CaseSensitive).matches(&c.to_string()));
        }
        assert!(LikePattern::new(r"\\", CaseSensitive).matches(r"\"));
    }

    #[tags(no_retry)]
    #[proptest]
    fn pattern_matches_itself(pat: String) {
        lazy_static! {
            static ref SPECIAL_CHARS: Regex = Regex::new(r"[%_\\]").unwrap();
        }

        let escaped_pat = SPECIAL_CHARS.replace_all(&pat, r"\$0");
        let pattern = LikePattern::new(&escaped_pat, CaseSensitive);
        assert!(pattern.matches(&pat));
    }
}
