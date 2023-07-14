// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
//
// Modified from [tikv/tikv_alloc][1]
// [1][https://github.com/tikv/tikv/tree/master/components/tikv_alloc]
//
// Modifications copyright (C) 2023 ReadySet, Inc.
// Modifications Summary:
// - removed allocators other than jemalloc and related features
// - updated crate documentation
//
//! As of now ReadySet always uses jemalloc on Unix. Although libraries
//! generally shouldn't be opinionated about their allocators like
//! this. It's easier to do this in one place than to have all our
//! bins turn it on themselves.
//!
//! Writing `extern crate readyset_alloc;` will link it to jemalloc when
//! appropriate.

//! # Profiling
//!
//! Profiling with jemalloc requires both build-time and run-time
//! configuration. At build time cargo needs the `--mem-profiling`
//! feature, and at run-time jemalloc needs to set the `opt.prof`
//! option to true, ala `MALLOC_CONF="opt.prof:true".
//!
//! In production you might also set `opt.prof_active` to `false` to
//! keep profiling off until there's an incident. Jemalloc has
//! a variety of run-time [profiling options].
//!
//! [profiling options]: http://jemalloc.net/jemalloc.3.html#opt.prof
//!
//! Here's an example of how you might build and run via cargo, with
//! profiling:
//!
//! ```notrust
//! export MALLOC_CONF="prof:true,prof_active:false,prof_prefix:$(pwd)/jeprof"
//! cargo test --features mem-profiling -p memory-utils -- --ignored
//! ```
//!
//! (In practice you might write this as a single statement, setting
//! `MALLOC_CONF` only temporarily, e.g. `MALLOC_CONF="..." cargo test
//! ...`).
//!
//! When running cargo while `prof:true`, you will see messages like
//!
//! ```notrust
//! <jemalloc>: Invalid conf pair: prof:true
//! <jemalloc>: Invalid conf pair: prof_active:false
//! ```
//!
//! This is normal - they are being emitting by the jemalloc in cargo
//! and rustc, which are both configured without profiling.
//! jemalloc is configured for profiling if you pass
//! `--features=mem-profiling` to cargo for `memory-utils`

#![cfg_attr(test, feature(test))]
#![cfg_attr(test, feature(custom_test_frameworks))]
#![cfg_attr(test, test_runner(runner::run_env_conditional_tests))]
#![feature(core_intrinsics)]

#[macro_use]
extern crate lazy_static;

pub mod error;
pub mod trace;

pub type AllocStats = Vec<(&'static str, usize)>;

#[path = "jemalloc.rs"]
mod imp;

pub use crate::imp::*;
pub use crate::trace::*;

#[global_allocator]
static ALLOC: imp::Allocator = imp::allocator();

#[cfg(test)]
mod runner {
    extern crate test;
    use test::*;

    /// Check for ignored test cases with ignore message "#ifdef <VAR_NAME>".
    /// The test case will be enabled if the specific environment variable
    /// is set.
    pub fn run_env_conditional_tests(cases: &[&TestDescAndFn]) {
        let cases: Vec<_> = cases
            .iter()
            .map(|case| {
                let mut desc = case.desc.clone();
                let testfn = match case.testfn {
                    TestFn::StaticTestFn(f) => TestFn::StaticTestFn(f),
                    TestFn::StaticBenchFn(f) => TestFn::StaticBenchFn(f),
                    ref f => panic!("unexpected testfn {:?}", f),
                };
                if let Some(msg) = desc.ignore_message {
                    let keyword = "#ifdef";
                    if let Some(s) = msg.strip_prefix(keyword) {
                        let var_name = s.trim();
                        if var_name.is_empty() || std::env::var(var_name).is_ok() {
                            desc.ignore = false;
                            desc.ignore_message = None;
                        }
                    }
                }
                TestDescAndFn { desc, testfn }
            })
            .collect();
        let args = std::env::args().collect::<Vec<_>>();
        test_main(&args, cases, None)
    }
}
