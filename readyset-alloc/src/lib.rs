// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
//
// Modified from [tikv/tikv_alloc][1]
// [1][https://github.com/tikv/tikv/tree/master/components/tikv_alloc]
//
// Modifications copyright (C) 2023 ReadySet, Inc.
// Modifications Summary:
// - removed allocators other than jemalloc and related features
// - updated crate documentation
// - add parts of thread module from tikv_util
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

pub mod defer;
pub mod error;
pub mod thread;
pub mod trace;

#[derive(Debug)]
#[allow(dead_code)]
/// [`AllocStats`] provides a summary of memory allocation information for a process, from the
/// perspective of jemalloc(3)
pub struct AllocStats {
    /// Total number of bytes allocated by the application.
    allocated: usize,
    /// Total number of bytes in active pages allocated by the application. This is a multiple of
    /// the page size, and greater than or equal to "stats.allocated". This does not include
    /// (jemalloc(3)) "stats.arenas.<i>.pdirty" and pages entirely devoted to allocator metadata.
    active: usize,
    /// Total number of bytes dedicated to metadata, which comprise base allocations used for
    /// bootstrap-sensitive allocator metadata structures (see jemalloc(3) stats.arenas.<i>.base)
    /// and internal allocations (see stats.arenas.<i>.internal). Transparent huge page
    /// (enabled with opt.metadata_thp) usage is not considered.
    metadata: usize,
    /// Maximum number of bytes in physically resident data pages mapped by the allocator,
    /// comprising all pages dedicated to allocator metadata, pages backing active allocations, and
    /// unused dirty pages. This is a maximum rather than precise because pages may not actually be
    /// physically resident if they correspond to demand-zeroed virtual memory that has not yet
    /// been touched. This is a multiple of the page size, and is larger than stats.active.
    resident: usize,
    /// Total number of bytes in chunks mapped on behalf of the application. This is a multiple of
    /// the chunk size, and is at least as large as "stats.active". This does not include inactive
    /// chunks.
    mapped: usize,
    /// Total number of bytes in virtual memory mappings that were retained rather than being
    /// returned to the operating system via e.g. munmap(2) or similar. Retained virtual memory is
    /// typically untouched, decommitted, or purged, so it has no strongly associated physical
    /// memory (see extent hooks for details). Retained memory is excluded from mapped memory
    /// statistics, e.g. stats.mapped.
    retained: usize,
    /// Total number of bytes that are resident but not "active" or "metadata". These bytes were
    /// once used by the process but have not been reclaimed by the OS.
    dirty: usize,
    /// Total number of bytes that are in active pages but are not "allocated" by the
    /// process--meaning they exist as the result of memory fragmentation.
    fragmentation: usize,
}

#[path = "jemalloc.rs"]
mod imp;

pub use crate::defer::*;
pub use crate::imp::*;
pub use crate::thread::*;
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
