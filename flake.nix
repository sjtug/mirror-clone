{
  description = "Mirror clone @ SJTUG";

  inputs = {
    nixpkgs.url = "git+https://mirrors.tuna.tsinghua.edu.cn/git/nixpkgs.git?ref=nixpkgs-unstable&shallow=1";
  };

  outputs = { self, nixpkgs }@inputs:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs {
        inherit system;
      };
    in
  {
    packages.${system} = {
      mirror-clone = pkgs.callPackage ./package.nix {
         version = self.rev or "dirty"; 
      };
      default = self.packages.${system}.mirror-clone;
    };
  };
}
