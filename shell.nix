with import (builtins.fetchTarball {
  url = "https://github.com/nixos/nixpkgs/archive/6cc260cfd60f094500b79e279069b499806bf6d8.tar.gz";
  sha256 = "0vak6jmsd33a7ippnrypqmsga1blf3qzsnfy7ma6kqrpp9k26cf6";
}) {};
mkShell {
  name = "readyset";
  buildInputs = [
    cmake
    cyrus_sasl
    rustup
    llvmPackages.bintools
    llvmPackages.clang
    llvmPackages.libclang.lib
    libffi
    lz4
    libxml2
    zstd
    ncurses
    openssl.dev
    pkg-config
    rdkafka
    rust-analyzer
    docker-compose
    cargo-audit
    postgresql
  ];

  LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib/libclang.so";
  CC="sccache clang";
  CXX="sccache clang++";

  PGHOST="localhost";
  PGUSER="postgres";
  PGPASSWORD="noria";
  PGDATABASE="noria";

  shellHook = ''
    export CC="sccache clang"
    export CXX="sccache clang++"
  '';
}
