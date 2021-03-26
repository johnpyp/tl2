let
  moz_overlay = import (builtins.fetchTarball https://github.com/mozilla/nixpkgs-mozilla/archive/master.tar.gz);
  nixpkgs = import <nixpkgs> { overlays = [ moz_overlay ]; };
in
with nixpkgs;
stdenv.mkDerivation {
  name = "rust-env";
  buildInputs = [
    rustup
    nixpkgs.pkgconfig
    nixpkgs.openssl
  ];
}
