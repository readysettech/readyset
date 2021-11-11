This directory contains JSON-serialized instances of noria_server::Config from
previous versions of the codebase, named after the commit that created them
(usually the commit right before we make a change to the data structure in
Rust). Config must always be backwards-compatible to allow for in-place upgrades
of Noria, which is checked by the accompanying `config_backwards_compatibility`
test suite.
