# Introduction
Welcome to ReadySet! 

This guide walks through how to set up your local development environment and 
merge your first code changes. It also provides a high level explanation of 
the underlying system and product.

## What is Noria?
Noria accepts a set of parameterized SQL queries (think [prepared
statements](https://en.wikipedia.org/wiki/Prepared_statement)), and produces a [data-flow
program](https://en.wikipedia.org/wiki/Stream_processing) that maintains [materialized
views](https://en.wikipedia.org/wiki/Materialized_view) for the output of those queries. Reads
now become fast lookups directly into these materialized views, as if the value had been
directly read from a cache (like memcached). The views are automatically kept up-to-date by
Noria through the data-flow.


