#!/bin/bash
helm install rs-mysql \
  --set auth.rootPassword=noria,auth.database=noria \
    bitnami/mysql
