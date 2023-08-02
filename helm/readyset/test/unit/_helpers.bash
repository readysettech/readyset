# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

# chart_dir returns the directory for the chart
chart_dir() {
    echo ${BATS_TEST_DIRNAME}/../..
}
