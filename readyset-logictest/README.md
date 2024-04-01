This crate is a Readyset-specific, rust implmentation of the [SQLite
sqllogictest](https://sqlite.org/sqllogictest/doc/trunk/about.wiki) 
testing suite. We follow most of the conventions of the original
sqllogictest, so please check out it's wiki.

# Supported data types

Sqllogictest supports three data types:

- `I` - integer
- `T` - varchar(30)
- `R` - real

The intent is to keep things as simple as possible for this kind of test.
We have found that to be mostly sufficient, but we've added support to
`readyset-logictest` for a few additional data types:

- `D` - date, although this is actually a `datetime` or `timestamp`
- `M` - time, a simple clock time with no date component
- `Z` - timestamptz, a datetime with timezone information
- `BV` - bit vector

The following types have partial support; YMMV as of this writing:

- `F` - numeric, a fixed-point number
- `B` - byte array

