#!/usr/bin/env bash
# shellcheck disable=SC2039

set -eo pipefail

CURRENT_SUBJECT=''

proper_subject_length() {
  local sha=${1}

  CURRENT_SUBJECT=$(git log --format="%s" "${sha}^..${sha}")
  awk '{ if(length($0) > 80) { exit 1 } }' <<<"${CURRENT_SUBJECT}"
}

contains_body() {
  local sha=${1}
  local body

  CURRENT_SUBJECT=$(git log --format="%s" "${sha}^..${sha}")
  body=$(git log --format="%b" "${sha}^..${sha}" | grep -v "Change-Id:")
  grep -E '.+' <<<"${body}" >/dev/null
}

second_line_empty() {
  local sha=${1}
  local body

  CURRENT_SUBJECT=$(git log --format="%s" "${sha}^..${sha}")
  test -z "$(git log --format="%B" "${sha}^..${sha}" | sed '2q;d')"
}

body_wrapped() {
  local sha=${1}
  local body

  body=$(git log --format="%b" "${sha}^..${sha}")
  awk '{ if(length($0) > 80) { exit 1  }  }' <<<"${body}"
}

failed() {
  .buildkite/set_gerrit_status.sh failed
  exit 1
}

sha="${BUILDKITE_COMMIT}"

echo "Linting: ${sha}"
if ! second_line_empty "${sha}"; then
  echo >&2 "Second line not empty: '$CURRENT_SUBJECT' (${sha})"
  failed
fi

if ! proper_subject_length "${sha}"; then
  echo >&2 "Subject longer than 80 characters: '$CURRENT_SUBJECT' (${sha})"
  failed
fi

if ! contains_body "${sha}"; then
  echo >&2 "Commit (${sha}) is missing a body"
  echo >&2 "All commit messages should have a body describing the purpose of the change, etc."
  echo >&2 "To format a commit message with a body, add a blank line after the subject of your commit, then write a longer description of the commit wrapped to 80 characters"
  failed
fi

if ! body_wrapped "${sha}"; then
  echo >&2 "At least one line in commit body more than 80 characters: '$CURRENT_SUBJECT' (${sha})"
  echo >&2 "Note that commit bodies need to be hard-wrapped to 80 characters"
  failed
fi

echo 'Success!'
exit 0
