# {
#   inputs = {
#     utils.url = "github:numtide/flake-utils";
#     naersk.url = "github:nmattia/naersk";
#   };
#
#   outputs = { self, nixpkgs, utils, naersk }:
#     utils.lib.eachDefaultSystem (system: let
#       pkgs = nixpkgs.legacyPackages."${system}";
#       naersk-lib = naersk.lib."${system}";
#     in rec {
#       # `nix build`
#       packages.my-project = naersk-lib.buildPackage {
#         pname = "my-project";
#         root = ./.;
#       };
#       defaultPackage = packages.my-project;
#
#       # `nix run`
#       apps.my-project = utils.lib.mkApp {
#         drv = packages.my-project;
#       };
#       defaultApp = apps.my-project;
#
#       # `nix develop`
#       devShell = pkgs.mkShell {
#         nativeBuildInputs = with pkgs; [ rustc cargo ];
#       };
#     });
# }

{
  description = "my project description";

  inputs.flake-utils.url = "github:numtide/flake-utils";

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem
      (system:
        let pkgs = nixpkgs.legacyPackages.${system}; in
        {
          devShell = import ./shell.nix { inherit pkgs; };
        }
      );
}
