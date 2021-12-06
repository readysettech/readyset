#!/bin/bash

HELP_TEXT="
Run Yaml Template Tool (YTT) for all files in a given directory matching the pattern *.ytt.* and given value file
and dump the resulting YAML files to a another directory.

Arguments:
  -i | Input Directory | Directory where ytt templates are stored
  -o | Outpit Directory | Directory to dump generated YAML files to
  -f | Values file | YTT variable file

Example usage:
  ytt_apply.sh -i ../ytt_templates -f values.yaml  -o ./
"
VERSION_NUM="0.0.1"

while getopts i:o:f:v flag
do
    case "${flag}" in
        i) indir=${OPTARG};;
        o) outdir=${OPTARG};;
        f) valfile=${OPTARG};;
        v) echo "YTT Apply version: $VERSION_NUM"; exit;;
        *) echo "$HELP_TEXT"; exit;;
    esac
done

for ytt_path in "$indir"/*.ytt.*
do
  ytt_file=$(basename -- "$ytt_path")
  ytt -f "$ytt_path" -f "$valfile" > "$outdir${ytt_file%%.*}.yaml"
  rain fmt "$outdir${ytt_file%%.*}.yaml" --write
done