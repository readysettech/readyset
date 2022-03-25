#!/bin/sh
set -eux

sudo install -D -o vector -g vector -m 644 \
  /tmp/vector-agent/etc_vector_base.toml \
  /opt/readyset/vector.d/base.toml

sudo install -D -o vector -g vector -m 644 \
  /tmp/vector-agent/etc_vector_metadata_to_cw.toml \
  /opt/readyset/vector.d/metadata_to_cw.toml

sudo install -D -o vector -g vector -m 644 \
  /tmp/vector-agent/etc_vector_metrics_to_cw.toml \
  /opt/readyset/vector.d/metrics_to_cw.toml

sudo install -D -o vector -g vector -m 644 \
  /tmp/vector-agent/etc_vector_metadata_to_agg.toml \
  /opt/readyset/vector.d/metadata_to_agg.toml

sudo install -D -o vector -g vector -m 644 \
  /tmp/vector-agent/etc_vector_metrics_to_agg.toml \
  /opt/readyset/vector.d/metrics_to_agg.toml

sudo install -D -o root -g root -m 755 \
  /tmp/vector-agent/usr_local_bin_configure-vector-agent.sh \
  /usr/local/bin/configure-vector-agent.sh
