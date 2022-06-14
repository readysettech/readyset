# Dashboard Dev Process

This page documents the workflow for making changes to the Grafana dashboards
that are distributed as part of the installer

## Dashboard-only changes

Follow these steps when making changes *only* to the dashboards and not to
the readyset server or adapter, eg with already-existing metrics.

1. Run the installer to get a local readyset deployment running. You can either
   do this by running the released version of the installer:

   ```console
   $ bash -c "$(curl -sSL https://launch.readyset.io)"
   ```

   Or by running a locally compiled version of the installer:

   ```console
   $ cargo run --bin readyset-installer
   ```

   Note that in both cases, since this pulls down pre-built docker images with
   the `readyset-server` and `readyset-adapter` binaries, this will not include
   any local modifications to that code.

2. Open the Grafana web UI at http://localhost:4000

3. Make any changes to the relevant dashboard in-place.

   Remember to *not* refresh the page - since Grafana is configured to load
   dashboards from disk, this will revert all your changes.

4. Press the "Save" button on the dashboard. This will bring up a modal window
   with a text box containing some JSON and a button to copy that JSON to your
   clipboard. Paste the JSON into the corresponding dashboard file in
   `//installer/src/compose/templates` (and the dashboard JSON file in the
   installer deploy directory, if you want to test your changes after
   refreshing), then stage and commit!

## Dashboard and ReadySet changes

If making changes to the dashboard that depend on changes to ReadySet, eg new
metrics or fixed metrics bugs, a couple of extra steps are required to get a
good workflow:

1. Run your ReadySet deployment locally, ensuring that everything is listening
   on `0.0.0.0` and you've passed [the `--prometheus-metrics`
   flag][prometheus-metrics] to all relevant binaries
2. Edit `compose/vector/aggregator.toml` in the installer state directory for
   your OS (`~/.local/share/readyset/` on Linux, `~/Library/Application
   Support/io.readyset.ReadySet/` on MacOS), and change the target of the
   prometheus-adapter source to the host machine (`172.17.0.1` on Linux,
   `host.docker.internal` on MacOS).
3. Restart the Vector container by running `docker-compose -f
   <state-directory>/compose/<deployment>.yml restart vector`

You should now be able to see metrics from the ReadySet cluster running on your
host machine

[prometheus-metrics]: http://docs/rustdoc/noria_client_adapter/struct.Options.html#structfield.prometheus_metrics
