#!/bin/sh
set -eux
# cloud-init runs post boot on all cloud providers and might need to wrap up
# some work before we begin.

cloud-init status --wait
