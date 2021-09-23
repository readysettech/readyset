with import <nixpkgs> {};

let
  terraform_1_0_2 = mkTerraform {
    version = "1.0.2";
    sha256 = "0gnv6hajpn1ks4944cr8rgkvly9cgvx4zj1zwc7nf1sikqfa8r1a";
    vendorSha256 = "0q1frza5625b1va0ipak7ns3myca9mb02r60h0py3v5gyl2cb4dk";
  };
in

mkShell {
  buildInputs = [
    terraform_1_0_2
  ];
}
