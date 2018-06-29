# distributary-ui
Web UI for distributary clusters

### Requirements
Python and a recent version of Pystache.

On Ubuntu:
```
# apt-get install python-pystache
```
Other platforms with `pip`:
```
pip install pystache
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

Use the top-right hand control to point the UI to a running controller's REST
API address.
