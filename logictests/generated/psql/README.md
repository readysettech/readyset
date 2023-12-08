# Psql generated logictests

Since these tests take the longest in our nightly pipeline, they are sharded
into 6 subdirectories to run in parallel.

If you are adding a new test, choose any subdirectory, trying to keep them
roughly uniformly distributed by runtime duration.
