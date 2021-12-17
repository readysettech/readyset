#!/bin/bash

HELP_TEXT="
Run variable template tool (hand rolled python program YTT-) for all files in a given directory matching the pattern
*.ytt.* using a specified packer manifest file and dump the resulting YAML files to a another directory.

Arguments:
  -i | Input Directory | Directory where ytt templates are stored
  -o | Outpit Directory | Directory to dump generated YAML files to
  -m | Manifest File | packer-manifest.json File to extract variables from

Example usage:
  ytt_apply.sh -i ../ytt_templates -m packer-manifest.json  -o ./
"
VERSION_NUM="0.0.2"

while getopts i:o:m:v flag
do
    case "${flag}" in
        i) indir=${OPTARG};;
        o) outdir=${OPTARG};;
        m) manifestfile=${OPTARG};;
        v) echo "YTT Apply version: $VERSION_NUM"; exit;;
        *) echo "$HELP_TEXT"; exit;;
    esac
done

for ytt_path in "$indir"/*.ytt.*
do
  ytt_file=$(basename -- "$ytt_path")
  python3.8 -m ytt-minus \
    --manifest "$manifestfile" \
    --infile "$ytt_path" \
    --outfile "$outdir${ytt_file%%.*}.yaml"
done