# Style Guide 
## Introduction
This guide is a work-in-progress and is loosely inspired by [Google's style guides](https://google.github.io/styleguide/cppguide.html).

## Error Handling
Generally, the `ReadySetError` type should be used for nearly all failure conditions that occur within ReadySet crates.

Some exceptions to this are inside crates that interface with outside systems that expect specific kinds of errors.

#### Creating New Kinds of Errors
New kinds of errors should be added as new variants to the `ReadySetError` enum. Nesting enums inside of `ReadySetError` is discouraged in favor of adding top-level variants to `ReadySetError`.

**Rationale:** Consistently using only one error type simplifies function signatures and allows all failure possibilities to be easily propagated. Introducing extra error enums to nest within `ReadySetError` can complicate function signatures and restrict the ability to return other kinds of `ReadySetError`s that may be relevant. This can be especially troublesome when trying to use convenience macros such as `internal!` that throw `ReadySetError`s. Additionally, nesting other enums inside `ReadySetError` is cumbersome to set up and reduces the top-level granularity of the `ReadySetError`, making it more difficult to handle specific error cases individually.