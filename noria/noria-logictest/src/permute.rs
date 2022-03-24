use std::collections::HashSet;
use std::iter;
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{anyhow, bail, Result};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use itertools::{Either, Itertools};
use noria_logictest::generate::{Generate, GenerateOpts};
use query_generator::{OperationList, Operations};

const SMALL_OPERATIONS: &[&str] = &[
    "count",
    "count_distinct",
    "sum",
    "sum_distinct",
    "avg",
    "avg_distinct",
    "group_concat",
    "max",
    "min",
    "equal_filters",
    "not_equal_filters",
    "greater_filters",
    "greater_or_equal_filters",
    "less_filters",
    "less_or_equal_filters",
    "between_filters",
    "is_null_filters",
    "distinct",
    "inner_join",
    "left_join",
    "single_parameter",
    "multiple_parameters",
    "range_param",
    "multiple_range_params",
    "project_literal",
    "in_parameter",
    "cte",
    "join_subquery",
    "topk",
    "paginate",
];

#[derive(Parser, Debug)]
struct PermutationGenerator {
    /// Only generate scripts with these operations
    #[clap(long)]
    only: Option<String>,

    /// Exclude these operations from all generated scripts
    #[clap(long)]
    exclude: Vec<String>,

    /// Generate scripts with this many operations
    #[clap(long, short = 'd')]
    depth: usize,
}

fn num_permutations_upto(num_ops: usize, depth: usize) -> usize {
    (1..=depth)
        .map(|l| (0..l).map(|n| (num_ops - n)).product::<usize>())
        .sum::<usize>()
}

struct OpsPermutations {
    inner: Box<dyn Iterator<Item = Vec<&'static str>>>,
    len: usize,
}

impl Iterator for OpsPermutations {
    type Item = Vec<&'static str>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.inner.next();
        if next.is_some() {
            self.len -= 1;
        }
        next
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }
}

impl ExactSizeIterator for OpsPermutations {}

impl PermutationGenerator {
    fn generate_tests(self) -> Result<OpsPermutations> {
        let ops = SMALL_OPERATIONS.iter().copied();
        let (ops, num_ops) = if self.exclude.is_empty() {
            (Either::Left(ops), SMALL_OPERATIONS.len())
        } else {
            let exclude = self
                .exclude
                .into_iter()
                .map(|exclude_op| {
                    if !SMALL_OPERATIONS.contains(&exclude_op.as_str()) {
                        bail!("Unknown operation passed to --exclude: {}", exclude_op)
                    } else {
                        Ok(exclude_op)
                    }
                })
                .collect::<Result<HashSet<_>>>()?;
            let num_excluded = exclude.len();
            (
                Either::Right(ops.filter(move |op| !exclude.contains(*op))),
                SMALL_OPERATIONS.len() - num_excluded,
            )
        };

        let (len, ops) = match self.only {
            None => (
                num_permutations_upto(num_ops, self.depth),
                Either::Left((1..=self.depth).flat_map(move |l| ops.clone().permutations(l))),
            ),
            Some(only) => {
                let op: &'static str = *SMALL_OPERATIONS
                    .iter()
                    .find(|op| ***op == only)
                    .ok_or_else(|| anyhow!("Unknown operation: {}", only))?;
                (
                    1 + num_permutations_upto(num_ops, self.depth - 1),
                    Either::Right(iter::once(vec![op]).chain((1..=(self.depth - 1)).flat_map(
                        move |l| {
                            ops.clone().permutations(l).map(move |mut perm| {
                                perm.push(op);
                                perm
                            })
                        },
                    ))),
                )
            }
        };

        Ok(OpsPermutations {
            inner: Box::new(ops),
            len,
        })
    }
}

/// Generate exhaustive suites of logictests by permuting all combinations of operators up to a
/// certain depth
#[derive(Parser, Debug)]
pub struct Permute {
    #[clap(flatten)]
    permutation_generator: PermutationGenerator,

    /// Directory to write tests to
    #[clap(long, short = 'o')]
    out_dir: PathBuf,

    /// Overwrite existing generated tests in the output directory
    #[clap(long)]
    overwrite_tests: bool,

    /// Don't actually generate tests, just print tests that would be generated
    #[clap(long)]
    dry_run: bool,

    /// Maximum recursion depth to use when generating subqueries
    #[clap(long, default_value = "2")]
    subquery_depth: usize,

    #[clap(flatten)]
    script_options: GenerateOpts,
}

impl Permute {
    pub fn run(self) -> Result<()> {
        let mut failures = vec![];
        let tests = self.permutation_generator.generate_tests()?;
        let pb = ProgressBar::new(tests.len() as _).with_style(
            ProgressStyle::default_bar()
                .template("{wide_bar} {msg} {pos}/{len} ({elapsed} / {duration})"),
        );
        for test in tests {
            pb.inc(1);
            let mut output = self.out_dir.clone();
            output.push(format!("{}.test", test.iter().join(",")));
            if output.is_file() && !self.overwrite_tests {
                if self.dry_run {
                    pb.println(format!(
                        "[--dry-run] would skip {} as it exists",
                        output.display()
                    ))
                } else {
                    pb.set_message(format!("Skipping {} as it exists", output.display()));
                }
                continue;
            }

            if self.dry_run {
                pb.println(format!("[--dry-run] would generate: {}", output.display()));
            } else {
                pb.set_message(format!("Generating {}", output.display()));
                let generate = Generate {
                    from: None,
                    query_options: query_generator::GenerateOpts {
                        operations: Some(OperationList(
                            test.into_iter()
                                .map(Operations::from_str)
                                .collect::<Result<Vec<_>>>()?,
                        )),
                        subquery_depth: self.subquery_depth,
                        num_operations: None,
                    },
                    script_options: self.script_options.clone(),
                    output: Some(output.clone()),
                };
                if let Err(e) = generate.run() {
                    pb.println(format!(
                        "Error generating test {}: {:#}",
                        output.display(),
                        e
                    ));
                    failures.push((output.display().to_string(), e.to_string()));
                }
            }
        }
        pb.finish();

        if failures.is_empty() {
            Ok(())
        } else {
            eprintln!("The following tests failed to generate:\n");
            for (filename, errmsg) in failures {
                eprintln!(" * {}\n   Error: {:#}", filename, errmsg);
            }

            Err(anyhow!("Some tests failed to generate"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_exclude() {
        let generator = PermutationGenerator {
            only: None,
            exclude: vec!["topk".to_owned()],
            depth: 1,
        };
        let mut tests = generator.generate_tests().unwrap();

        assert!(tests.all(|test| !test.contains(&"topk")))
    }

    #[test]
    fn generate_exclude_and_only() {
        let generator = PermutationGenerator {
            only: Some("max".to_owned()),
            exclude: vec!["topk".to_owned()],
            depth: 2,
        };
        let mut tests = generator.generate_tests().unwrap();

        assert!(tests.all(|test| !test.contains(&"topk") && test.contains(&"max")))
    }
}
