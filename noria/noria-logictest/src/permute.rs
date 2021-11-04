use std::iter;
use std::path::PathBuf;
use std::str::FromStr;

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
    "project_literal",
    "multiple_parameters",
    "in_parameter",
    "cte",
    "join_subquery",
    "topk",
];

/// Generate exhaustive suites of logictests by permuting all combinations of operators up to a
/// certain depth
#[derive(Parser, Debug)]
pub struct Permute {
    /// Only generate scripts with these operations
    #[clap(long)]
    only: Option<String>,

    /// Generate scripts with this many operations
    #[clap(long, short = 'd')]
    depth: usize,

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

fn num_permutations_upto(depth: usize) -> usize {
    let num_ops = SMALL_OPERATIONS.len();
    (1..=depth)
        .map(|l| (0..l).map(|n| (num_ops - n)).product::<usize>())
        .sum::<usize>()
}

impl Permute {
    pub fn run(self) -> anyhow::Result<()> {
        let (num_tests, ops) = match &self.only {
            None => (
                num_permutations_upto(self.depth),
                Either::Left(
                    (1..=self.depth).flat_map(|l| SMALL_OPERATIONS.iter().copied().permutations(l)),
                ),
            ),
            Some(only) => (
                1 + num_permutations_upto(self.depth - 1),
                Either::Right(iter::once(vec![only.as_str()]).chain(
                    (1..=(self.depth - 1)).flat_map(move |l| {
                        SMALL_OPERATIONS
                            .iter()
                            .copied()
                            .permutations(l)
                            .map(move |mut perm| {
                                perm.push(only.as_str());
                                perm
                            })
                    }),
                )),
            ),
        };

        let pb = ProgressBar::new(num_tests as _).with_style(
            ProgressStyle::default_bar()
                .template("{wide_bar} {msg} {pos}/{len} ({elapsed} / {duration})"),
        );
        for test in ops {
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
                                .collect::<anyhow::Result<Vec<_>>>()?,
                        )),
                        subquery_depth: self.subquery_depth,
                        num_operations: None,
                    },
                    script_options: self.script_options.clone(),
                    output: Some(output.clone()),
                };
                generate.run()?;
            }
        }
        pb.finish();

        Ok(())
    }
}
