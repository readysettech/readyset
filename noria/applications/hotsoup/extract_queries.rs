use std::path::{Path, PathBuf};

use tracing::{info, trace};

fn traverse(path: &Path) -> Vec<PathBuf> {
    use std::fs;

    let mut files = Vec::new();
    for entry in fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() {
            if let Some(e) = path.extension() {
                if e.to_str().unwrap() == "php" || e.to_str().unwrap() == "inc" {
                    files.push(path.clone())
                }
            }
        } else if path.is_dir() {
            files.extend(traverse(path.as_path()));
        }
    }
    files
}

fn process_file(fp: &Path, git_rev: &str) -> Vec<(String, String)> {
    use std::collections::hash_map::DefaultHasher;
    use std::fs::File;
    use std::hash::{Hash, Hasher};
    use std::io::Read;

    use regex::Regex;

    let re = "\"((?is)select [^;]* from [^;]*?(?-is))\"(?:, \".*\"\\)| \\.|;|\\s*\\)|,)";
    let query_regex = Regex::new(re).unwrap();

    let mut f = File::open(fp).unwrap();
    let mut s = String::new();

    f.read_to_string(&mut s).unwrap();

    let mut queries = Vec::new();
    let mut h = DefaultHasher::new();
    for cap in query_regex.captures_iter(&s) {
        let qstr = &cap[1];
        qstr.hash(&mut h);
        let qid = h.finish();
        queries.push((format!("{}_{:x}", git_rev, qid), String::from(qstr)));
    }
    queries
}

#[allow(clippy::trivial_regex)]
fn reformat(queries: Vec<(String, String)>) -> Vec<(String, String)> {
    use regex::Regex;

    let incomplete = Regex::new("=$").unwrap();
    let linebreaks_tabs = Regex::new("\t|\n").unwrap();
    let php_str_concat = Regex::new("\"[:space:]*\\.[:space:]*\"").unwrap();
    let php_str_concat_inset = Regex::new("\"[:space:]*\\.(?P<cc>.*)\\.[:space:]*\"").unwrap();
    let php_vars = Regex::new("\\$[a-zA-Z0-9->_]+").unwrap();
    let braces_question_mark = Regex::new("(\\{\\?\\})|'\\?'").unwrap();
    let question_mark_a = Regex::new("[:space:]*\\?[A|a]").unwrap();
    let unclosed_quote = Regex::new("='\\z").unwrap();

    queries
        .into_iter()
        .filter(|&(_, ref q)| !q.contains("Matches"))
        .map(|(qn, q)| (qn, php_str_concat_inset.replace_all(&q, "$cc").to_string()))
        .map(|(qn, q)| (qn, php_str_concat.replace_all(&q, "").to_string()))
        .map(|(qn, q)| (qn, php_vars.replace_all(&q, "?").to_string()))
        .map(|(qn, q)| (qn, linebreaks_tabs.replace_all(&q, " ").to_string()))
        .map(|(qn, q)| (qn, incomplete.replace_all(&q, "=?").to_string()))
        .map(|(qn, q)| (qn, braces_question_mark.replace_all(&q, "?").to_string()))
        .map(|(qn, q)| (qn, question_mark_a.replace_all(&q, "=?").to_string()))
        .map(|(qn, q)| (qn, unclosed_quote.replace_all(&q, "=?").to_string()))
        .map(|(qn, q)| {
            if !q.ends_with(';') {
                (qn, format!("{};", q))
            } else {
                (qn, q)
            }
        })
        .collect()
}

const SKIP_FILES: [&str; 1] = ["test02.php"];

fn main() {
    use std::fs::File;
    use std::io::Write;

    use clap::{App, Arg};

    readyset_tracing::Options::default()
        .init("hotsoup/extract_queries")
        .unwrap();

    let matches = App::new("extract_queries")
        .version("0.1")
        .about("Extracts queries from HotCRP code.")
        .arg(
            Arg::new("source")
                .index(1)
                .help("Location of the HotCRP code to work on.")
                .required(true),
        )
        .arg(
            Arg::new("output")
                .short('o')
                .long("output")
                .value_name("FILE")
                .help("Location to write output recipe to.")
                .required(true),
        )
        .arg(
            Arg::new("git_rev")
                .short('g')
                .long("git_rev")
                .value_name("REV")
                .help("Git revision that we're extracting for.")
                .required(true),
        )
        .get_matches();

    let path = matches.value_of("source").unwrap();
    let output = matches.value_of("output").unwrap();
    let git_rev = matches.value_of("git_rev").unwrap();

    let files = traverse(Path::new(path));

    let mut ok = Vec::new();
    let mut rejected = Vec::new();
    for fname in files {
        if SKIP_FILES.contains(&fname.file_name().unwrap().to_str().unwrap()) {
            continue;
        }

        let queries = process_file(fname.as_path(), git_rev);

        let formatted_queries = reformat(queries);

        for (ql, q) in formatted_queries {
            match nom_sql::parse_query(nom_sql::Dialect::MySQL, &q) {
                Ok(_) => ok.push((ql, q)),
                Err(_) => rejected.push((ql, q)),
            }
        }
    }

    let mut f = File::create(output).unwrap();

    info!("Writing {} valid queries...", ok.len());
    for (ql, q) in ok {
        assert!(write!(f, "{}: {}\n", ql, q).is_ok());
    }

    info!("Writing {} rejected queries...", rejected.len());
    for (name, query) in rejected {
        trace!(%name, %query, "failed to parse");
        assert!(write!(f, "# FAIL {}:\n# {}\n", name, query).is_ok());
    }
}
