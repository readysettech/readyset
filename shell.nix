with import (builtins.fetchTarball {
  url = "https://github.com/nixos/nixpkgs/archive/87d34a6b8982e901b8e50096b8e79ebc0e66cda0.tar.gz";
  sha256 = "0dqjw05vbdf6ahy71zag8gsbfcgrf7fxz3xkwqqwapl0qk9xk47a";
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
