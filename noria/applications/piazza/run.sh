# basic results for session creation time
./benchmarks/piazza/experiments/session-creation.sh basic benchmarks/piazza/basic-policies.json benchmarks/piazza/post-queries.sql

# results for comparing the effect of group policies
./benchmarks/piazza/experiments/session-creation.sh group benchmarks/piazza/ta-policies.json benchmarks/piazza/post-queries.sql

./benchmarks/piazza/experiments/session-creation.sh nogroup benchmarks/piazza/ta-policies-nogroups.json benchmarks/piazza/post-queries.sql