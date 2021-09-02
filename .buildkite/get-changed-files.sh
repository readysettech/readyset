#!/bin/bash

git fetch origin main
git diff --name-only origin/main..HEAD
