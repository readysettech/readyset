{ pkgs ? import <nixpkgs> {} }:
with pkgs;

let
  gems = bundlerEnv {
    name = "readyset-solidus";
    gemdir = ./.;
  };
in mkShell {
  buildInputs = [
    bundix
    gems
    gems.wrappedRuby
    v8
    sqlite.dev
    mysql80
    openssl.dev
    zlib.dev
    zstd.dev
    docker-compose
  ];

  FREEDESKTOP_MIME_TYPES_PATH="${shared-mime-info}/share/mime/packages/freedesktop.org.xml";
}
