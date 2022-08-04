# Overview
The ReadySet codebase builds on the Noria project, an open-source research prototype built by a team of grad students, post-docs, professors, and research affiliates at the MIT Parallel and Distributed Operating Systems (PDOS) lab, of which ReadySet’s founding team was a part.

# What are the differences between ReadySet and Noria?

## Intent
Noria is a research database that is not intended for production use. Its initial purpose was to demonstrate that the idea of partially-stateful streaming dataflow was viable, and it was successful – check out the associated research paper and example applications! It, however, is not a production-ready, robust system; if you send Noria a query, there’s a good chance that it will either crash or otherwise return an incorrect result.

ReadySet takes the ideas behind Noria and makes them ready for production. We extensively test for correctness and fault tolerance and continue to expand the scope of our testing over time.

## Ergonomics
There are a few key ergonomic differences between Noria and ReadySet:
ReadySet is a plug-and-play caching layer that transparently sits in front of your existing database to improve read performance. When using ReadySet, you still maintain an upstream source-of-truth database.
ReadySet supports both the MySQL and Postgres wire protocols.
ReadySet provides fine-grained control over which queries are cached, whereas Noria supports only MySQL and caches every query that is issued against it. ReadySet reduces adoption overhead and application complexity because you don’t have to do query routing at a higher layer in the stack.

## Query Support & Ecosystem Compatibility

ReadySet supports a wider range of queries as compared to Noria, and is compatible with many more ORMs and database client libraries. See the full list here.

## Active Development

ReadySet is under heavy, ongoing development - join our community Slack to take part and follow along!
