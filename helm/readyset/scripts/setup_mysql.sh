#!/bin/bash
helm install rs-mysql \
  --set auth.rootPassword=readyset,auth.database=readyset \
    bitnami/mysql
