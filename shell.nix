with import (builtins.fetchTarball {
  url = "https://github.com/nixos/nixpkgs/archive/4c87cb87a2db6b9eb43541c1cf83f2a2f725fa25.tar.gz";
  sha256 = "0jjnj05fav5b0rynz6y2fx223rljbqd3myjr8lm22sj46y1sv0zw";
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
  ];

  LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib/libclang.so";
  CC="sccache clang";
  CXX="sccache clang++";

  shellHook = ''
    export CC="sccache clang"
    export CXX="sccache clang++"
  '';
}
