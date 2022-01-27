# Monitoring

## Detecting Restarts

### Builtin Fallback Metric
In the case where we can't monitor the orchestrator, Noria provides a metric
that can be leveraged as a fallback for detecting restarts:
`startup_timestamp`. At startup, this is populated with the then-current unix
timestamp (in milliseconds). This metric can be combined with the `changes`
function to chart the number of restarts in a specified window.

Example:
```
changes(startup_timestamp[15m])
```
This will graph the number of restarts in a sliding 15 minute window (this can
be changed by changing the `15m` in the above to a different value).