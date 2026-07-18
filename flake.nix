{
  description = "Aeron development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs =
    { self, nixpkgs, ... }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forAllSystems =
        f:
        nixpkgs.lib.genAttrs systems (
          system:
          f {
            inherit system;
            pkgs = nixpkgs.legacyPackages.${system};
          }
        );
    in
    {
      devShells = forAllSystems (
        { pkgs, system }:
        let
          inherit (pkgs) lib stdenv;

          jdk = pkgs.zulu17;
          linuxDriverLibs = lib.optionals stdenv.isLinux [
            pkgs.libbsd
            pkgs.util-linux
          ];
        in
        {
          default = pkgs.mkShell {
            packages =
              with pkgs;
              [
                jdk
                gradle_9
                cmake
                ninja
                gnumake
                pkg-config
                clang-tools
                lcov
                doxygen
                graphviz
                git
              ]
              ++ lib.optional stdenv.isLinux gcc
              ++ linuxDriverLibs;

            shellHook = ''
              export JAVA_HOME="${jdk}"
              export BUILD_JAVA_HOME="${jdk}"
              export BUILD_JAVA_VERSION=17
            '';
          };
        }
      );
    };
}
