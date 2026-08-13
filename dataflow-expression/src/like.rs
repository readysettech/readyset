//! Implementation of the semantics of SQL's LIKE operator
//!
//! Within LIKE patterns:
//!
//! * `%` represents any string, of any length
//! * `_` represents any single character
//! * `\%` represents a literal `%` character
//! * `\_` represents a literal `_` character
//!
//! Literal characters are compared according to a [`Collation`], so LIKE is case- or
//! accent-insensitive exactly when the collation is.

use std::cmp::Ordering;
use std::mem;

use readyset_data::Collation;
use smallvec::SmallVec;

use Token::{Any, One, Run};

#[derive(Debug, PartialEq, Eq)]
enum Token {
    /// A run of literal characters, which matches a slice of the input with the same number of
    /// characters that compares equal under the pattern's collation. Runs are never empty, so
    /// `chars` is at least 1.
    Run { text: String, chars: usize },
    /// `_`, which matches exactly one character.
    One,
    /// `%`, which matches any number of characters.
    Any,
}

fn tokenize(pat: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut run = String::new();
    let flush = |run: &mut String, tokens: &mut Vec<Token>| {
        if !run.is_empty() {
            let chars = run.chars().count();
            tokens.push(Run {
                text: mem::take(run),
                chars,
            });
        }
    };

    let mut chars = pat.chars();
    while let Some(c) = chars.next() {
        match c {
            '%' => {
                flush(&mut run, &mut tokens);
                if !matches!(tokens.last(), Some(Any)) {
                    tokens.push(Any);
                }
            }
            '_' => {
                flush(&mut run, &mut tokens);
                tokens.push(One);
            }
            '\\' => run.push(chars.next().unwrap_or('\\')),
            c => run.push(c),
        }
    }
    flush(&mut run, &mut tokens);
    tokens
}

/// A matcher for a tokenized pattern. The common kinds of pattern are recognized and matched
/// directly. Everything else is matched token by token with backtracking.
#[derive(Debug, PartialEq, Eq)]
enum Matcher {
    /// `%`: matches everything.
    Anything,
    /// A pattern of only literal characters: the whole input must compare equal to `text`.
    Exact { text: String, chars: usize },
    /// `text%`: the first `chars` characters of the input must compare equal to `text`.
    Prefix { text: String, chars: usize },
    /// `%text`: the last `chars` characters of the input must compare equal to `text`.
    Suffix { text: String, chars: usize },
    /// `%text%`: some `chars`-character window of the input must compare equal to `text`.
    Contains { text: String, chars: usize },
    /// Any other pattern, matched token by token with backtracking.
    General(Box<[Token]>),
}

impl Matcher {
    fn new(mut tokens: Vec<Token>) -> Self {
        let known = match tokens.as_mut_slice() {
            [Any] => Some(Self::Anything),
            [Run { text, chars }] => Some(Self::Exact {
                text: mem::take(text),
                chars: *chars,
            }),
            [Run { text, chars }, Any] => Some(Self::Prefix {
                text: mem::take(text),
                chars: *chars,
            }),
            [Any, Run { text, chars }] => Some(Self::Suffix {
                text: mem::take(text),
                chars: *chars,
            }),
            [Any, Run { text, chars }, Any] => Some(Self::Contains {
                text: mem::take(text),
                chars: *chars,
            }),
            _ => None,
        };
        known.unwrap_or_else(|| Self::General(tokens.into_boxed_slice()))
    }
}

/// Splits `s` after its first `chars` characters, or returns None if `s` has fewer characters.
fn split_at_chars(s: &str, chars: usize) -> Option<(&str, &str)> {
    if chars == 0 {
        return Some(("", s));
    }
    let (i, c) = s.char_indices().nth(chars - 1)?;
    Some(s.split_at(i + c.len_utf8()))
}

/// Representation for a LIKE or ILIKE pattern
pub struct LikePattern {
    matcher: Matcher,
    collation: Collation,
}

impl LikePattern {
    /// Construct a new LIKE pattern from the given string and [`Collation`].
    ///
    /// This will do some work, so should be done ideally at most once per pattern.
    pub fn new(pat: &str, collation: Collation) -> Self {
        Self {
            matcher: Matcher::new(tokenize(pat)),
            collation,
        }
    }

    /// Returns true if this LikePattern matches the given string.
    pub fn matches(&self, s: &str) -> bool {
        let eq = |text: &str, slice: &str| self.collation.compare(text, slice) == Ordering::Equal;
        match &self.matcher {
            Matcher::Anything => true,
            Matcher::Exact { text, chars } => match split_at_chars(s, *chars) {
                Some((head, "")) => eq(text, head),
                _ => false,
            },
            Matcher::Prefix { text, chars } => {
                split_at_chars(s, *chars).is_some_and(|(head, _)| eq(text, head))
            }
            Matcher::Suffix { text, chars } => s
                .char_indices()
                .rev()
                .nth(*chars - 1)
                .is_some_and(|(start, _)| eq(text, &s[start..])),
            Matcher::Contains { text, chars } => {
                // Every `chars`-character window of `s`: `starts` yields the windows' first byte
                // offsets and `ends` the offsets one past their last characters.
                let starts = s.char_indices().map(|(i, _)| i);
                let ends = s
                    .char_indices()
                    .map(|(i, _)| i)
                    .chain(std::iter::once(s.len()))
                    .skip(*chars);
                starts
                    .zip(ends)
                    .any(|(start, end)| eq(text, &s[start..end]))
            }
            Matcher::General(tokens) => self.matches_general(tokens, s),
        }
    }

    fn matches_general(&self, tokens: &[Token], s: &str) -> bool {
        // Character boundaries of `s`, including the end, so boundaries[i]..boundaries[j] is
        // the byte range covering characters i..j.
        let boundaries: SmallVec<[usize; 64]> = s
            .char_indices()
            .map(|(i, _)| i)
            .chain(std::iter::once(s.len()))
            .collect();
        let nchars = boundaries.len() - 1;

        let consume = |token: &Token, pos: usize| -> Option<usize> {
            match token {
                Run { text, chars } => {
                    let end = pos.checked_add(*chars).filter(|&end| end <= nchars)?;
                    let slice = &s[boundaries[pos]..boundaries[end]];
                    (self.collation.compare(text, slice) == Ordering::Equal).then_some(end)
                }
                One => (pos < nchars).then_some(pos + 1),
                Any => unreachable!("Any is handled by the caller"),
            }
        };

        // The standard wildcard-matching algorithm: match tokens greedily, and on a mismatch
        // backtrack to the most recent `%`, retrying with it consuming one more character.
        let mut token_idx = 0;
        let mut pos = 0;
        let mut backtrack: Option<(usize, usize)> = None;

        loop {
            match tokens.get(token_idx) {
                Some(Any) => {
                    backtrack = Some((token_idx, pos));
                    token_idx += 1;
                }
                Some(token) => match consume(token, pos) {
                    Some(next) => {
                        token_idx += 1;
                        pos = next;
                    }
                    None => match backtrack {
                        Some((any_idx, any_pos)) if any_pos < nchars => {
                            backtrack = Some((any_idx, any_pos + 1));
                            token_idx = any_idx + 1;
                            pos = any_pos + 1;
                        }
                        _ => return false,
                    },
                },
                None => {
                    if pos == nchars {
                        return true;
                    }
                    // Only a trailing `%` can absorb the leftover characters.
                    match backtrack {
                        Some((any_idx, _)) if any_idx == tokens.len() - 1 => return true,
                        Some((any_idx, any_pos)) if any_pos < nchars => {
                            backtrack = Some((any_idx, any_pos + 1));
                            token_idx = any_idx + 1;
                            pos = any_pos + 1;
                        }
                        _ => return false,
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use readyset_data::Collation::*;
    use test_strategy::proptest;
    use test_utils::tags;

    use super::*;

    #[test]
    fn like_matching() {
        assert!(LikePattern::new("foo%", Utf8).matches("foobar"));
        assert!(!LikePattern::new("foo%", Utf8).matches("oofoobar"));
        assert!(LikePattern::new("%foo%", Utf8).matches("oofoobar"));
        assert!(LikePattern::new("%foo___", Utf8).matches("oofoobar"));
        assert!(!LikePattern::new("%foo___", Utf8).matches("oofoobarr"));
        assert!(!LikePattern::new("%foo%", Utf8).matches("ooFOOoo"));
    }

    #[test]
    fn ilike_matching() {
        assert!(LikePattern::new("%foo%", Utf8Ci).matches("oofoobar"));
        assert!(LikePattern::new("%foo%", Utf8Ci).matches("ooFOObar"));
    }

    #[test]
    fn ilike_matching_unicode() {
        assert!(LikePattern::new("σ%Σ%σ%", Utf8Ci).matches("Σomebody σet up uσ the bomb"));
    }

    #[test]
    fn ai_ci_matching() {
        let pat = LikePattern::new("résum%", Utf8AiCi);
        assert!(pat.matches("resume"));
        assert!(pat.matches("Resume"));
        assert!(pat.matches("résumé"));
        assert!(pat.matches("RÉSUMÉ"));
        assert!(!pat.matches("zzz"));
        assert!(LikePattern::new("RESUM%", Utf8AiCi).matches("résumé"));
        assert!(LikePattern::new("r_sum_", Utf8AiCi).matches("résumé"));
        // LIKE consumes one input character per pattern character, so a decomposed pattern
        // does not match precomposed input even though the collation compares the strings
        // equal. MySQL LIKE behaves the same way.
        assert!(!LikePattern::new("re\u{301}sume\u{301}", Utf8AiCi).matches("résumé"));
    }

    #[test]
    fn latin1_swedish_ci_matching() {
        assert!(LikePattern::new("résum%", Latin1SwedishCi).matches("RESUME"));
        assert!(LikePattern::new("ü", Latin1SwedishCi).matches("y"));
        assert!(LikePattern::new("Å", Latin1SwedishCi).matches("å"));
        assert!(!LikePattern::new("ö", Latin1SwedishCi).matches("o"));
    }

    #[test]
    fn escapes() {
        assert!(LikePattern::new(r"foo\bar", Utf8).matches(r"foobar"));
        assert!(LikePattern::new(r"foo\\bar", Utf8).matches(r"foo\bar"));
        assert!(LikePattern::new(r"\%", Utf8).matches("%"));
        assert!(!LikePattern::new(r"\%", Utf8).matches(r"\foo"));
        assert!(LikePattern::new(r"\_", Utf8).matches("_"));
        assert!(!LikePattern::new(r"\_", Utf8).matches(r"\a"));
        assert!(LikePattern::new(r"\\", Utf8).matches(r"\"));
        assert!(LikePattern::new(r"\a", Utf8).matches("a"));
        assert!(LikePattern::new(r"\\%", Utf8).matches(r"\foo"));
        assert!(LikePattern::new(r"\\\%", Utf8).matches(r"\%"));
        assert!(!LikePattern::new(r"\a", Utf8).matches("b"));
        for c in r"{}.*+?|()[]$^".chars() {
            assert!(LikePattern::new(&c.to_string(), Utf8).matches(&c.to_string()));
        }
        for c in r"%_\{}.*+?|()[]$^".chars() {
            assert!(LikePattern::new(&format!(r"\{c}"), Utf8).matches(&c.to_string()));
        }
        assert!(LikePattern::new(r"\\", Utf8).matches(r"\"));
    }

    fn run(text: &str) -> Token {
        Token::Run {
            text: text.into(),
            chars: text.chars().count(),
        }
    }

    #[test]
    fn tokenizer() {
        assert_eq!(*tokenize(""), []);
        assert_eq!(*tokenize("abc"), [run("abc")]);
        assert_eq!(
            *tokenize("a%b_c"),
            [run("a"), Token::Any, run("b"), Token::One, run("c")]
        );
        assert_eq!(*tokenize("%%a%%"), [Token::Any, run("a"), Token::Any]);
        assert_eq!(*tokenize("__"), [Token::One, Token::One]);
        assert_eq!(*tokenize(r"a\%b"), [run("a%b")]);
        assert_eq!(*tokenize(r"\_"), [run("_")]);
        assert_eq!(*tokenize(r"\\"), [run(r"\")]);
        assert_eq!(*tokenize(r"\a"), [run("a")]);
        // A trailing lone backslash is a literal backslash.
        assert_eq!(*tokenize("a\\"), [run("a\\")]);
        // Char counts are in characters, not bytes.
        assert_eq!(*tokenize("é béchamel"), [run("é béchamel")]);
        assert!(matches!(
            &tokenize("é%")[..],
            [Token::Run { chars: 1, .. }, Token::Any]
        ));
    }

    #[test]
    fn classification() {
        let exact = Matcher::Exact {
            text: "abc".into(),
            chars: 3,
        };
        let prefix = Matcher::Prefix {
            text: "abc".into(),
            chars: 3,
        };
        let suffix = Matcher::Suffix {
            text: "abc".into(),
            chars: 3,
        };
        let contains = Matcher::Contains {
            text: "abc".into(),
            chars: 3,
        };
        assert_eq!(Matcher::new(tokenize("%")), Matcher::Anything);
        assert_eq!(Matcher::new(tokenize("abc")), exact);
        assert_eq!(Matcher::new(tokenize("abc%")), prefix);
        assert_eq!(Matcher::new(tokenize("%abc")), suffix);
        assert_eq!(Matcher::new(tokenize("%abc%")), contains);
        assert_eq!(
            Matcher::new(tokenize("a%b")),
            Matcher::General([run("a"), Any, run("b")].into())
        );
        assert_eq!(
            Matcher::new(tokenize("_abc")),
            Matcher::General([One, run("abc")].into())
        );
        assert_eq!(Matcher::new(tokenize("")), Matcher::General([].into()));
    }

    fn pattern(matcher: Matcher) -> LikePattern {
        LikePattern {
            matcher,
            collation: Utf8,
        }
    }

    #[test]
    fn anything_matching() {
        let pat = pattern(Matcher::Anything);
        assert!(pat.matches(""));
        assert!(pat.matches("anything"));
    }

    #[test]
    fn exact_matching() {
        let pat = pattern(Matcher::Exact {
            text: "foo".into(),
            chars: 3,
        });
        assert!(pat.matches("foo"));
        assert!(!pat.matches("fo"));
        assert!(!pat.matches("foob"));
        assert!(!pat.matches(""));
    }

    #[test]
    fn prefix_matching() {
        let pat = pattern(Matcher::Prefix {
            text: "foo".into(),
            chars: 3,
        });
        assert!(pat.matches("foo"));
        assert!(pat.matches("foobar"));
        assert!(!pat.matches("fo"));
        assert!(!pat.matches("xfoo"));
    }

    #[test]
    fn suffix_matching() {
        let pat = pattern(Matcher::Suffix {
            text: "foo".into(),
            chars: 3,
        });
        assert!(pat.matches("foo"));
        assert!(pat.matches("barfoo"));
        assert!(!pat.matches("oo"));
        assert!(!pat.matches("foox"));
        assert!(pattern(Matcher::Suffix {
            text: "fé".into(),
            chars: 2,
        })
        .matches("café"));
    }

    #[test]
    fn contains_matching() {
        let pat = pattern(Matcher::Contains {
            text: "foo".into(),
            chars: 3,
        });
        assert!(pat.matches("foo"));
        assert!(pat.matches("xxfooyy"));
        assert!(!pat.matches("fo"));
        assert!(!pat.matches("fxoxo"));
    }

    #[tags(no_retry)]
    #[proptest]
    fn pattern_matches_itself(pat: String) {
        let escaped_pat: String = pat
            .chars()
            .flat_map(|c| {
                matches!(c, '%' | '_' | '\\')
                    .then_some('\\')
                    .into_iter()
                    .chain(std::iter::once(c))
            })
            .collect();
        let pattern = LikePattern::new(&escaped_pat, Utf8);
        assert!(pattern.matches(&pat));
    }
}
