{
  description = "remote-cli — canonical cache, daemon, Unix socket, and remote SSE substrate for Rust CLIs";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
        remoteCli = pkgs.rustPlatform.buildRustPackage {
          pname = "remote-cli";
          version = "0.1.0";
          src = ./.;
          cargoLock.lockFile = ./Cargo.lock;
          doCheck = true;
          meta = {
            description = "Canonical cache, daemon, local Unix socket, remote SSE, and smart-client substrate";
            homepage = "https://github.com/harryaskham/remote-cli";
            license = pkgs.lib.licenses.mit;
          };
        };
      in
      {
        packages.default = remoteCli;
        packages.remote-cli = remoteCli;
        checks.test = remoteCli;
        apps.release = {
          type = "app";
          program = "${pkgs.writeShellScript "remote-cli-release" ''
            export PATH="${pkgs.lib.makeBinPath [ pkgs.git pkgs.cargo ]}:$PATH"
            exec ${pkgs.bash}/bin/bash ${./scripts/release.sh} "$@"
          ''}";
        };
        devShells.default = pkgs.mkShell {
          inputsFrom = [ remoteCli ];
          packages = with pkgs; [ cargo rustc rustfmt clippy rust-analyzer ];
          buildInputs = pkgs.lib.optionals pkgs.stdenv.isDarwin [ pkgs.libiconv ];
        };
        formatter = pkgs.nixfmt-rfc-style;
      }
    ) // {
      lib.mkDaemonModules = args: import ./nix/service-modules.nix args;
    };
}
