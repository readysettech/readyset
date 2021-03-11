# readyset node.js client
The `readyset` npm package and its `js-client` rust backend live here.

## Building and running the client
To compile this npm package for use in a node.js script, run `npm run build` from this directory. This will compile the rust crate and copy the cdylib artifact to the right place.

A sample node.js script using the `readyset` node.js client is in the `./examples` directory. The demo file `./examples/basic_demo.js` can be conveniently run with `npm start`.