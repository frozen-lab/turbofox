{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
      in {
        devShells = {
          default = pkgs.mkShell {
            name = "dev";
            buildInputs = with pkgs; [
              # c
              gcc
              gdb
              perf
            
              # rust
              rustc
              cargo
              rustfmt
              clippy
              rust-analyzer
            ];

            shellHook = ''
              export RUST_BACKTRACE=1
              
              echo " : $(gcc --version)"
              echo " : $(rustc --version)"
            '';
          };
        };
       }
    );
}
