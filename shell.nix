with import (builtins.fetchTarball {
  url = "https://github.com/nixos/nixpkgs/archive/fd364d268852561223a5ada15caad669fd72800e.tar.gz";
  sha256 = "133i5fsx0gix37q4nxm1vfsl9hqbfzv458xykilqhgxmv45jmfl2";
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
