# Noria web UI
This repository contains the web UI for [Noria](https://github.com/mit-pdos/noria)
servers. You connect it to a running Noria deployment by pointing the web UI at the
Noria controller's _external port_.

### Requirements
Python and a recent version of Pystache.

On Ubuntu:
```
$ apt-get install python-pystache
```
Other platforms with `pip`:
```
$ pip install pystache
```

## Serving the UI

To generate static HTML files, run:
```
$ make
```

To serve the UI on `localhost` port 8000 via Python's `simplehttp`:
```
$ ./run.sh
```

You can now access the UI in your browser at
[http://localhost:8000](http://localhost:8000).

Use the top-right hand control to point the UI to a running controller's
external REST API address.

Noria prints this address on startup:
```
Oct 08 18:01:56.004 INFO became leader at epoch 587761
Oct 08 18:01:56.006 INFO found initial leader
Oct 08 18:01:56.007 INFO leader listening on external address V4(127.0.0.1:6033)
```
The last line specifies the external address and port; the default port is 6033.
