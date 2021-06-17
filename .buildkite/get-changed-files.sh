#!/bin/bash

git fetch origin master
git diff --name-only origin/master..HEAD
