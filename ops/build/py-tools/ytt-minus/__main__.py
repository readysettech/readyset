import argparse
import json
import yaml
import re
import sys
from os import getcwd, path
from functools import reduce
from itertools import chain

VERSION = "0.0.2"

def parse_packer_manifest(packer_manifest_contents):
    def parse_build(build_json):

        name = build_json["name"]

        for artifact in build_json["artifact_id"].split(","):
            region, ami = artifact.split(":")

            # names will be in the format <x>-<y>-<x>
            # We need to remove hyphens and replace with something YAML can tolerate
            yield (f"{name}-{region}".replace("-", "_"), ami)

    return dict(reduce(chain, (parse_build(x) for x in packer_manifest_contents)))

def template_variable_sub(text_file_lines, variable_dict):
    # Match strings with pattern #@ {<variable>}
    variable_match = re.compile('(?<=#@).*{(.*?)}+')
    variable_match_full_pattern = re.compile('(#@).*{(.*?)}+')

    def template_line(text_file_line):
        match = re.search(variable_match, text_file_line)
        if match is not None:
            match_term = match.group(1)
            replaced = re.sub(variable_match_full_pattern, variable_dict[match_term], text_file_line)
            print( f'Replaced {text_file_line} with {replaced}')
        else:
            replaced = text_file_line
        return replaced

    return ''.join([template_line(text_file_line)for text_file_line in text_file_lines])

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="\
        Use a packer manifest file to perform variable substitution on a CFN template. \
        Uses #@ {<variable>} Syntax for template substitution. \
        Note there can be any number of spaces between #@ and the left curly brace. \
        *Current version can only handle one template variable per line*\
        "
    )
    parser.add_argument(
        "--manifest",
        type=str,
        help="packer-manifest.json file path relative to the current directory.",
        default="ops/image-deploy/packer-manifest.json",
    )
    parser.add_argument(
        "--infile",
        type=str,
        help="Temaplted CFN file to perform variable substitution on.",
        default="ops/cfn/ytt_templates/readyset-mysql-template.ytt.yaml",
    )
    parser.add_argument(
        "--outfile",
        type=str,
        help="Path to rendered CFN file.",
        default="ops/cfn/templates/readyset-mysql-template.yaml",
    )
    parser.add_argument(
        '--version',
        help="Print the program version number.",
        action='version',
        version=f'CFN Deploy version: {VERSION}'
    )

    args = parser.parse_args()

    dirname = path.abspath(getcwd())
    packer_manifest_path = path.abspath(path.join(dirname, args.manifest))
    input_file_path = path.abspath(path.join(dirname, args.infile))
    output_file_path = path.abspath(path.join(dirname, args.outfile))

    try:
        with open(packer_manifest_path, "r") as f:
            data = json.load(f)
            print(f"Loaded Packer Manifest file from {packer_manifest_path}")
            parsed_manifest = parse_packer_manifest(data["builds"])
            print("Successfully parsed packer manifest.")
        if not data:
            print("No data in file " + packer_manifest_path)
    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))

    try:
        with open(input_file_path, "r") as f:
            input_file_text = f.readlines()
            rendered_cfn_template = template_variable_sub(input_file_text, parsed_manifest)

        if not data:
            print("No data in file " + input_file_path)
    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))

    try:
        with open(output_file_path, "w") as writer:
            writer.write(rendered_cfn_template)
            print(f"Wrote CFN file to {output_file_path}")
    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))
