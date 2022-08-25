# ReadySet Helm Chart

## Disclaimer

This helm chart is provided for development purposes and is not intended to be
used in production workflows.

The current chart assumes it will be run on EKS or at least that the gp2
storage class is available.

## Scripts

1. Install a mysql helm chart with an example testing database. This is an alternative to setting up RDS
```
  ./scripts/setup_mysql.sh
```

2. Create a kubernetes secret with the test database env variables
```
  ./scripts/create_secret.sh
```

4. Install the readyset helm chart from the current directory
```
  helm install rs .
```

5. Wait for pods to come up
```
  kubectl get pods -w
```

6. Port forward the adapter
```
  ./scripts/port_forward_adapter.sh
```

7. Follow adapter logs (assumes a single adapter pod)
```
./scripts/adapter_logs.sh
```

8. Follow server logs
```
kubectl logs rs-readyset-server-0 -c readyset-server -f
```

9. Spawn windows with server/adapter logs (requires an active tmux session)
```
./scripts/k8logs.sh
```

