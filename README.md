# nom-sql

[![Build Status](https://travis-ci.org/ms705/nom-sql.svg)](https://travis-ci.org/ms705/nom-sql)

An incomplete Rust SQL parser written using [nom](https://github.com/Geal/nom).

This parser is a work in progress. It currently supports:
 * most `CREATE TABLE` queries;
 * most `INSERT` queries;
 * simple `SELECT` queries;
 * simple `UPDATE` queries; and
 * simple `DELETE` queries.

We try to support both the [SQLite](https://sqlite.org/lang.html) and
[MySQL](https://dev.mysql.com/doc/refman/5.7/en/sql-syntax.html) syntax; where
they disagree, we choose MySQL. (It would be nice to support both via feature
flags in the future.)
