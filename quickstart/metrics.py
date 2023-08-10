##!/usr/bin/env python3
import argparse
import sys
import urllib3
from prometheus_client.parser import text_string_to_metric_families
from tabulate import tabulate, SEPARATING_LINE

parser = argparse.ArgumentParser(
    description="print out formatted ReadySet query statistics from the metrics endpoint")
parser.add_argument("--host",
		    help="host for running ReadySet instance",
					default="localhost")
parser.add_argument("--filter-queries",
		    help="filter output by piping in output from 'SHOW PROXIED QUERIES'/'SHOW CACHES'",
					action="store_true")
args = parser.parse_args()

http = urllib3.PoolManager()
metrics_endpoint = "http://" + args.host + ":6034/metrics"
r = http.request('GET', metrics_endpoint)

results = {}
readyset_results = {}

if args.filter_queries:
	# We ignore the first two lines and the last two lines by slicing with the
	# range 2:-2 to remove the lines from the output that don't contain actual
	# data
	query_ids = set()
	for line in sys.stdin.readlines()[2:-2]:
		print(line)
		query_id = line.split("|")[0].strip().strip("`")
		print(query_id)
		query_ids.add(query_id)

# Each "family" consists of every sample for a particular metric
for family in text_string_to_metric_families(r.data.decode('utf-8')):
	for sample in family.samples:
		# Samples for a particular family include the metrics
		# themselves and aggregates like sums and counts, so we
		# need to make sure we're looking at the right samples
		if sample.name == "readyset_query_log_execution_time":
			labels = sample.labels

			if not args.filter_queries or labels["query_id"] in query_ids:
				latency = sample.value * 1000
				database_type = labels["database_type"]
				query_text = labels["query"]
				quantile = labels["quantile"]

				if "readyset" in database_type:
					if query_text not in readyset_results:
						readyset_results[query_text] = {}

					readyset_results[query_text]["p" + quantile] = latency
				else:
					if query_text not in results:
						results[query_text] = {}

					results[query_text]["p" + quantile] = latency
		elif sample.name == "readyset_query_log_execution_time_count":
			labels = sample.labels

			if not args.filter_queries or labels["query_id"] in query_ids:
				query_count = int(sample.value)
				database_type = labels["database_type"]
				query_text = labels["query"]

				if "readyset" in database_type:
					readyset_results[query_text]["count"] = query_count
				else:
					results[query_text]["count"] = query_count

tabulated_rows = []
tabulated_readyset_rows = []
for k in results.keys():
	tabulated_rows.append([k, results[k]["count"], results[k]["p0.5"], results[k]["p0.9"], results[k]["p0.99"]])
	tabulated_rows.append(SEPARATING_LINE)

for k in readyset_results.keys():
	tabulated_readyset_rows.append([k, readyset_results[k]["count"], readyset_results[k]["p0.5"], readyset_results[k]["p0.9"], readyset_results[k]["p0.99"]])
	tabulated_readyset_rows.append(SEPARATING_LINE)

try:
	if len(results.keys()) > 0 or len(readyset_results.keys()) > 0:
		if (len(results.keys()) > 0):
			print ("Proxied Queries")
			print (tabulate(tabulated_rows, headers=["query text", "count", "p50 (ms)", "p90 (ms)", "p99 (ms)"], tablefmt="psql", floatfmt=".3f", maxcolwidths=70))

		if (len(readyset_results.keys()) > 0):
			print ("ReadySet Queries")
			print (tabulate(tabulated_readyset_rows, headers=["query text", "count", "p50 (ms)", "p90 (ms)", "p99 (ms)"], tablefmt="psql", floatfmt=".3f", maxcolwidths=70))
	else:
		raise ValueError("Oops! There are no query-specific metrics. Have you run a query yet? Did you pass --query-log and --query-log-ad-hoc flags when running ReadySet?")
except ValueError as err:
	print (str(err.args[0]))


