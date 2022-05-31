with import <nixpkgs> {};

let
  terraform_1_1_6 = mkTerraform {
    version = "1.1.6";
    sha256 = "1fpyizz0z191gxr9vhx7hps9kwrwyjqmdc4sm881aq7a5i212l33";
    vendorSha256 = "1pddglgr3slvrmwrnsbv7jh3hnas6mzw6akss2pmsxqgy1is44a6";
  };
in

mkShell {
  buildInputs = [
    terraform_1_1_6
    packer
  ];
}
