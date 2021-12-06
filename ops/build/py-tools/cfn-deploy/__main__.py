import argparse
import json
import yaml
import sys
import os
from functools import reduce
from itertools import chain

VERSION = "0.0.1"

def parse_packer_manifest(packer_manifest_contents):
    def parse_build(build_json):

        name = build_json["name"]

        for artifact in build_json["artifact_id"].split(","):
            region, ami = artifact.split(":")

            # names will be in the format <x>-<y>-<x>
            # We need to remove hyphens and replace with something YAML can tolerate
            yield (f"{name}-{region}".replace("-", "_"), ami)

    return dict(reduce(chain, (parse_build(x) for x in packer_manifest_contents)))


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Generate Yaml Template Tool, ytt, templates required for Cloud Formation Templates."
    )
    parser.add_argument(
        "--infile",
        type=str,
        help="packer-manifest.json file path relative to the current directory.",
        default="ops/image-deploy/packer-manifest.json",
    )
    parser.add_argument(
        "--outfile",
        type=str,
        help="ytt template file paths relative to the current directory.",
        default="ops/cfn/templates/values.yaml",
    )

    parser.add_argument(
        '--version',
        help="Print the program version number.",
        action='version',
        version=f'CFN Deploy version: {VERSION}'
    )

    args = parser.parse_args()

    input_file_path = args.infile

    dirname = os.path.abspath(os.getcwd())
    packer_manifest_path = os.path.join(dirname, input_file_path)
    output_file_path = os.path.join(dirname, args.outfile)

    try:
        with open(packer_manifest_path, "r") as f:
            data = json.load(f)
            print(f"Loaded Packer Manifest file from {packer_manifest_path}")
            raw_yaml_values = yaml.dump(parse_packer_manifest(data["builds"]))
            # We need to add a ytt specific header that marks our output file as a variable / patch file
            rendered_yaml_values = f"#@data/values\n---\n{raw_yaml_values}"
            print("Successfully rendered YTT data values")
        if not data:
            print("No data in file " + packer_manifest_path)
    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))

    try:
        with open(output_file_path, "w") as writer:
            writer.write(rendered_yaml_values)
            print(f"Wrote YTT data values to {output_file_path}")
    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))
