#!/bin/bash
# Gets the pod name of the readyset-adapter and follows its logs

ADAPTER=$(kubectl get pods | grep adapter | cut -d' ' -f1);
kubectl logs ${ADAPTER} -c readyset-adapter -f
