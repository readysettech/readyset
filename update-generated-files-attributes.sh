#!/bin/sh
git grep --name-only "# managed by Substrate; do not edit by hand" | sed "s/\$/ linguist-generated=true/" > .gitattributes
