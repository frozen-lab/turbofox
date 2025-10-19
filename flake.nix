{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";
    flake-utils.url = "github:numtide/flake-utils";
    rust.url = "github:adityamotale/dotfiles?dir=rust";
  };

  outputs = { nixpkgs, flake-utils, rust, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
      in {
        devShells.default = pkgs.mkShell {
          name = "dev";
          inputsFrom = [ rust.devShells.${system}.default ];
          buildInputs = with pkgs; [
            # rust (bench and profile)
            gdb
            linuxPackages.perf
            cargo-show-asm
          ];
        };
    });
}
