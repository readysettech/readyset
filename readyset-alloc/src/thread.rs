// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
//
// Modified from [tikv/tikv_util][1]
// [1][https://github.com/tikv/tikv/tree/master/components/tikv_util]
//
// Modifications copyright (C) 2023 ReadySet, Inc.
// Modifications Summary:
// - Only copied over memory related thread wrappers
// - Updated crate documentation

//! This module provides unified APIs for accessing thread/process related
//! information.

use std::io::Result;
use std::{io, thread};

use crate::{add_thread_memory_accessor, defer, remove_thread_memory_accessor};

/// Using `spawn_wrapper` will register a thread with the
/// [`THREAD_MEMORY_MAP`][crate::jemalloc::THREAD_MEMORY_MAP] on thread spawn and safely
/// deregister it before the thread exits.
pub trait StdThreadBuildWrapper {
    fn spawn_wrapper<F, T>(self, f: F) -> io::Result<thread::JoinHandle<T>>
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send + 'static;
}

impl StdThreadBuildWrapper for std::thread::Builder {
    fn spawn_wrapper<F, T>(self, f: F) -> Result<std::thread::JoinHandle<T>>
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send + 'static,
    {
        #[allow(clippy::disallowed_methods)]
        self.spawn(|| {
            // SAFETY: we will call `remove_thread_memory_accessor` at defer.
            unsafe { add_thread_memory_accessor() };
            defer! {{
                remove_thread_memory_accessor();
            }};
            f()
        })
    }
}

pub trait ThreadBuildWrapper {
    /// Register all system hooks along with a custom hook pair.
    fn with_sys_and_custom_hooks<F1, F2>(&mut self, after_start: F1, before_end: F2) -> &mut Self
    where
        F1: Fn() + Send + Sync + 'static,
        F2: Fn() + Send + Sync + 'static;

    /// Register some generic hooks like memory tracing or thread lifetime
    /// tracing.
    fn with_sys_hooks(&mut self) -> &mut Self {
        self.with_sys_and_custom_hooks(|| {}, || {})
    }
}

impl ThreadBuildWrapper for tokio::runtime::Builder {
    fn with_sys_and_custom_hooks<F1, F2>(&mut self, start: F1, end: F2) -> &mut Self
    where
        F1: Fn() + Send + Sync + 'static,
        F2: Fn() + Send + Sync + 'static,
    {
        #[allow(clippy::disallowed_methods)]
        self.on_thread_start(move || {
            // SAFETY: we will call `remove_thread_memory_accessor` at
            // `on_thread_stop`.
            // FIXME: What if the user only calls `after_start`?
            unsafe {
                add_thread_memory_accessor();
            }
            start();
        })
        .on_thread_stop(move || {
            end();
            remove_thread_memory_accessor();
        })
    }
}

impl ThreadBuildWrapper for futures::executor::ThreadPoolBuilder {
    fn with_sys_and_custom_hooks<F1, F2>(&mut self, start: F1, end: F2) -> &mut Self
    where
        F1: Fn() + Send + Sync + 'static,
        F2: Fn() + Send + Sync + 'static,
    {
        #[allow(clippy::disallowed_methods)]
        self.after_start(move |_| {
            // SAFETY: we will call `remove_thread_memory_accessor` at
            // `on_thread_stop`.
            // FIXME: What if the user only calls `after_start`?
            unsafe {
                add_thread_memory_accessor();
            }
            start();
        })
        .before_stop(move |_| {
            end();
            remove_thread_memory_accessor();
        })
    }
}
