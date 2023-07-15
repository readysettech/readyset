// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
//
// Modified from [tikv/tikv_util][1]
// [1][https://github.com/tikv/tikv/tree/master/components/tikv_util]
//
// Modifications copyright (C) 2023 ReadySet, Inc.
// Modification Summary
// - pulled out defer related utilities into this module

/// Invokes the wrapped closure when dropped.
pub struct DeferContext<T: FnOnce()> {
    t: Option<T>,
}

impl<T: FnOnce()> DeferContext<T> {
    pub fn new(t: T) -> DeferContext<T> {
        DeferContext { t: Some(t) }
    }
}

impl<T: FnOnce()> Drop for DeferContext<T> {
    fn drop(&mut self) {
        self.t.take().unwrap()()
    }
}

/// Simulates Go's defer.
///
/// Please note that, different from go, this defer is bound to scope.
/// When exiting the scope, its deferred calls are executed in last-in-first-out
/// order.
#[macro_export]
macro_rules! defer {
    ($t:expr) => {
        let __ctx = $crate::DeferContext::new(|| $t);
    };
}
