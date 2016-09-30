#!/bin/sh
for f in "$(dirname "$0")"/*.r; do
	R -q --no-readline --no-restore --no-save --args "$@" < "$f"
done
