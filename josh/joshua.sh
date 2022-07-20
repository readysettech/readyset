#!/bin/bash

set -e

function diffs() {
  local filter=$1
  local ref=$2
  local remote=$3

  remote_url=$(git config --get "remote.${remote}.url")
  echo "$(tput setaf 2)Here's the diffs that will be pushed to the remote repository at$(tput setaf 6) ${remote_url} $(tput sgr0)"
  echo "$(tput setaf 2)On the$(tput setaf 6) ${remote} $(tput setaf 2) branch.$(tput sgr0)"
  git --no-pager diff --compact-summary FETCH_HEAD FILTERED_HEAD
}

function push() {
  local filter=$1
  local ref=$2
  local remote=$3

  echo "$(tput setaf 2)Pushing filtered repository to:$(tput setaf 6) ${remote}:${ref} $(tput sgr0)"
  git push "${remote}" "FILTERED_HEAD:${ref}"
}

function joshua() {

  local filter=$1
  local ref=$2
  local remote=$3
  local push=$4

  echo "$(tput setaf 2)Filtering repo using$(tput setaf 6) ${filter} $(tput sgr0)"
  josh-filter "${filter}"

  echo "$(tput setaf 2)Fetching remote$(tput setaf 6) ${remote} $(tput setaf 2)to check if a push is needed.$(tput sgr0)"
  git fetch "${remote}" "${ref}"

  if git merge-base --is-ancestor FILTERED_HEAD FETCH_HEAD; then
    echo "$(tput setaf 1)Commit already present, nothing to push.$(tput sgr0)"
    exit 0
  fi

  if [[ "$push" == "true" ]]
  then
    echo "$(tput setaf 2)Here's the diffs that will be pushed to the remote repository at$(tput setaf 6) ${remote_url} $(tput sgr0)"
    push "$filter" "$ref" "$remote"
    exit
  else
    diffs "$filter" "$ref" "$remote"
    exit
  fi

}

function help() {
  echo "

  Usage:
    | Flag | Long Name | Description                   | Example          |
    |---------------------------------------------------------------------|
    | -f   | filter    | JOSH filter statement         | :/tests          |
    | -r   | ref       | git refs to push to           | refs/heads/main  |
    | -m   | remote    | git remote branch to push to  | origin           |
    | -p   | push      | push changes to remote branch | true             |
  "
}

while getopts r:f:m:p: flag
do
    case "${flag}" in
        f) filter=${OPTARG};;
        r) ref=${OPTARG};;
        m) remote=${OPTARG};;
        p) push=${OPTARG};;
        *) help;;
    esac
done

joshua "$filter" "$ref" "$remote" "$push"