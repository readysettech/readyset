with import (builtins.fetchTarball {
  url = "https://github.com/nixos/nixpkgs/archive/a2b0ea6865b2346ceb015259c76e111dcca039c5.tar.gz";
  sha256 = "12dgwajv3w94p13scj68c24v01p055k9hcpcsn7w9i8fys72s99d";
}) {};

mkShell {
  buildInputs = [
    terraform_0_14
  ];
}
