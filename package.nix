{
  lib,
  rustPlatform,
  pkg-config,
  openssl,
  version ? null,
}:

rustPlatform.buildRustPackage (finalAttrs: {
  pname = "mirror-clone";
  inherit version;

  # src = ./.;
  src = lib.fileset.toSource {
    root = ./.;
    fileset = lib.fileset.intersection (lib.fileset.fromSource (lib.sources.cleanSource ./.)) (
      lib.fileset.unions [
        ./src
        ./Cargo.lock
        ./Cargo.toml
        ./LICENSE
        ./rust-toolchain.toml
      ]
    );
  };

  cargoLock = {
    lockFile = ./Cargo.lock;
  };

  nativeBuildInputs = [ pkg-config ];

  buildInputs = [ openssl ];

  env.OPENSSL_NO_VENDOR = 1;

  meta = {
    description = "All-in-one mirror utility for SJTUG mirror ";
    homepage = "https://github.com/sjtug/mirror-clone";
    license = with lib.licenses; [ mit ];
    platforms = lib.platforms.linux;
    maintainers = with lib.maintainers; [ definfo ];
  };
})

