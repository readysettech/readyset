# Psql generated logictests

Since these tests take the longest in our nightly pipeline, they are sharded
into subdirectories to run in parallel.

If you are adding a new test, check the nightly build to identify the parallel
job that is currently the shortest and add the test to that subdir.
