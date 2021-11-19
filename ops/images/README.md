# images

This defines [Packer](https://packer.io) builds for all of the images built at
ReadySet.

Images that start with `readyset-` are meant for external use and all others are
for internal use.

## Layout

* **source-\*.pkr.hcl**: Packer source definitions that define a new image to
  create and what image it should be based on.
* **build-\*.pkr.hcl**: Packer build definitions that define what should be
  provisioned on a specific image.
* **provisioners/scripts/\***: Scripts that are run on the instance before an
  the image is taken from the running instance.
* **provisioners/files/\***: Files to upload to the image to be run when being
  used.

## Known Issues

* Should use [Pipeline Builds][0] to build images based upon other images we
  are building to avoid duplicating running the same code.

[0]: https://www.packer.io/guides/packer-on-cicd/pipelineing-builds
