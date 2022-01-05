with import (builtins.fetchTarball {
  url = "https://github.com/nixos/nixpkgs/archive/adf7f03d3bfceaba64788e1e846191025283b60d.tar.gz";
  sha256 = "13586j87n8jxin940rcr0q74bfv4zbmwz02l2p923dcgksvfjxds";
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
    openssl.out
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
  LD="${pkgs.mold}/bin/mold";
  # Necessary for executables linked by mold to be able to find these shared
  # libraries
  LD_LIBRARY_PATH = lib.strings.makeLibraryPath [
    openssl
    lz4
    zstd
    zlib
  ];

  PGHOST="localhost";
  PGUSER="postgres";
  PGPASSWORD="noria";
  PGDATABASE="noria";

  shellHook = ''
    export CC="sccache clang"
    export CXX="sccache clang++"
  '';
}
