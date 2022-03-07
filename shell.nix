with import (builtins.fetchTarball {
  url = "https://github.com/nixos/nixpkgs/archive/062a0c5437b68f950b081bbfc8a699d57a4ee026.tar.gz";
  sha256 = "0vfd7g1gwy9lcnnv8kclqr68pndd9sg0xq69h465zbbzb2vnijh9";
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
