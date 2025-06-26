use std::cmp::Ordering;
use std::collections::HashMap;

use log_once::error_once;

lazy_static::lazy_static! {
    static ref WEIGHTS: HashMap<char, u8> = {
        super::mysql_latin1_swedish_ci_weights::WEIGHTS.iter().cloned().collect()
    };
}

pub(crate) fn compare(a: &str, b: &str) -> Ordering {
    let mut a = a.chars();
    let mut b = b.chars();

    loop {
        let (a, b) = match (a.next(), b.next()) {
            (Some(a), Some(b)) => (a, b),
            (Some(a), None) => (a, ' '),
            (None, Some(b)) => (' ', b),
            (None, None) => return Ordering::Equal,
        };

        let (a, b) = match (WEIGHTS.get(&a), WEIGHTS.get(&b)) {
            (Some(a), Some(b)) => (a, b),
            (_, _) => {
                let (a, b) = (a as u32, b as u32);
                error_once!("non-latin1 codepoint in latin1 comparison: {a:X}, {b:X}");
                return a.cmp(&b); // wrong, but what else can we do?
            }
        };

        let res = a.cmp(b);
        if res != Ordering::Equal {
            return res;
        }
    }
}

pub(crate) fn key(s: &str, k: &mut Vec<u8>) {
    let s = s.trim_end_matches(' ');
    for c in s.chars() {
        let Some(c) = WEIGHTS.get(&c) else {
            let c = c as u32;
            error_once!("non-latin1 codepoint in latin1 key: {c:X}");
            k.push(0); // wrong, but what else can we do?
            continue;
        };
        k.push(*c);
    }
}
