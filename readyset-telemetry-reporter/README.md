# ReadySet Telemetry Reporter

This crate provides the telemetry reporting capability for ReadySet applications. It implements the [Segment HTTP Tracking API](https://segment.com/docs/connections/sources/catalog/libraries/server/http-api/). More details can be found [here](https://docs.readyset.io/using/telemetry).

## Configuration
Applications using this crate support disabling telemetry with the `--disable-telemetry` command-line flag.

They can also be configured by setting the following environment variables:

| Environment Variable   | Default | Description |
|------------------------|---------|-------------|
| `RS_API_KEY`           | `""`    | Provided by the ReadySet console. Used to associate telemetry with users. If omitted, telemetry will attempt to generate an anonymous (i.e. hashed with blake2b) uuid based on the machine it is running on. |
| `RS_SEGMENT_WRITE_KEY` | ReadySet write key | Identifies a Segment source. By default, telemetry is reported to the ReadySet, Inc. Segment account. Users can receive telemetry from their ReadySet deployment by providing their own write key. |

## License

Use of the Unicode Character Database, as this crate does, is governed by the <a
href="LICENSE-UNICODE">Unicode License Agreement &ndash; Data Files and Software
(2016)</a>.

