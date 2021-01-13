let
  moz_overlay = import (builtins.fetchTarball https://github.com/mozilla/nixpkgs-mozilla/archive/master.tar.gz);
  nixpkgs = import <nixpkgs> { overlays = [ moz_overlay ]; };
in
with nixpkgs;
stdenv.mkDerivation {
  name = "rust-env";
  buildInputs = [
    rustup
    # # Note: to use use stable, just replace `nightly` with `stable`
    # nixpkgs.latest.rustChannels.nightly.rust
    # nixpkgs.latest.rustChannels.nightly.cargo
    # nixpkgs.latest.rustChannels.nightly.rust-src

    # Add some extra dependencies from `pkgs`
    nixpkgs.pkgconfig
    nixpkgs.openssl
  ];


  # shellHook = ''
  #   export RUST_SRC_PATH="${rustChannel.rust-src}/lib/rustlib/src/rust/library"
  # '';

  # Set Environment Variables
  # RUST_BACKTRACE = 1;
}
