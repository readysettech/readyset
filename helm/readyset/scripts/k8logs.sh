#!/bin/bash
# Must be used in an active tmux session
# Creates a tmux window setup with an empty terminal on top and the server and
# adapter logs following in windows below

server_pod=$(kubectl get pods | grep readyset-server | cut -d' ' -f1);
adapter_pod=$(kubectl get pods | grep readyset-adapter | cut -d' ' -f1);

tmux split-window -v kubectl logs -f $server_pod -c readyset-server
tmux split-window -v kubectl logs -f $adapter_pod -c readyset-adapter
tmux select-layout even-vertical


