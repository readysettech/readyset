#!/bin/bash
# Gets the pod name of the readyset adapter and forwards port 3306, allowing
# mysql> access 

ADAPTER=$(kubectl get pods | grep adapter | cut -d' ' -f1);
# For MySQL
kubectl port-forward pod/${ADAPTER} 3306:3306
# For PostgreSQL
# kubectl port-forward pod/${ADAPTER} 5432:5432
