#!/bin/sh
git grep --name-only "# managed by Substrate; do not edit by hand" | grep -v update-generated-files-attributes.sh | sed "s/\$/ linguist-generated=true/" > .gitattributes
echo "modules/intranet/regional/substrate-intranet.zip linguist-generated=true" >> .gitattributes
