CREATE TABLE IF NOT EXISTS benchmark_data (
  id SERIAL PRIMARY KEY,
  buildkite_commit TEXT,
  name TEXT,
  build TEXT,
  execution_end_time timestamp without timezone,
  data JSON,
)
