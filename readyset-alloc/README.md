# readyset-alloc

The `readyset-alloc` crate contains memory allocator configuration and
utilities for memory tracking and profiling.

This crate is a fork of TiKV's [tikv_alloc][tikv-alloc] with some modifications
due to the fact that ReadySet only uses jemalloc.

The original [LICENSE][./LICENSE] of `tikv_alloc` is included in this crate,
and files that have been modified have been commented with:

```
Modifications copyright (C) 2023 ReadySet, Inc.
```

[tikv-alloc]: https://github.com/tikv/tikv/tree/master/components/tikv_alloc
