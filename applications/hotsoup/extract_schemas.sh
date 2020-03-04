#!/bin/bash
dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
repo="https://github.com/kohler/hotcrp.git"

cargo build --features="binaries" --release --bin extract_hotcrp_queries

mkdir -p ${dir}/schemas
mkdir -p ${dir}/queries

rm -rf /tmp/hotcrp
git clone ${repo} /tmp/hotcrp
cd /tmp/hotcrp

prev_schema_ver=99999999

for l in $(git log --abbrev=8 --oneline --follow src/schema.sql | cut -d' ' -f1); do
  # HotCRP schema versioning started in 80eeb701
  if [[ $l == "80eeb701" ]]; then
    break
  fi

  git checkout $l

  schema_file=$(find . -name schema.sql)

  res=$(grep "values ('allowPaperOption" ${schema_file})
  cur_schema_ver=$(echo ${res} | cut -d' ' -f 8 | sed -e "s/);//" | tr -d [:space:])

  if [[ ${cur_schema_ver} -lt ${prev_schema_ver} ]]; then
    echo "schema ${prev_schema_ver} -> ${cur_schema_ver}"
    # we have advanced to a new schema
    cp ${schema_file} ${dir}/schemas/hotcrp_${cur_schema_ver}.sql
    # extract the queries
    ${dir}/../../target/release/extract_hotcrp_queries $(pwd) --output \
      ${dir}/queries/hotcrp_${cur_schema_ver}.sql --git_rev ${l}
    prev_schema_ver=${cur_schema_ver}
  elif [[ ${cur_schema_ver} -gt ${prev_schema_ver} ]]; then
    echo "SCHEMA WENT BACKWARDS: ${cur_schema_ver} > ${prev_schema_ver} @ ${l}"
    exit 1
  fi
done

rm -rf /tmp/hotcrp
