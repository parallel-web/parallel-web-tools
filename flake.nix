{
  description = "Nix flake for parallel-web-tools development";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
    ...
  }:
    flake-utils.lib.eachDefaultSystem (system: let
      pkgs = import nixpkgs {
        inherit system;
      };

      python = pkgs.python312;

      basePackages = with pkgs; [
        python
        uv
        nodejs_24
        pnpm
      ];
    in {
      devShells.default = pkgs.mkShell {
        packages = basePackages;

        env = {
          UV_PYTHON = "${python}/bin/python3";
        };

        shellHook = ''
          if command -v git >/dev/null 2>&1; then
            REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
          else
            REPO_ROOT="$PWD"
          fi
          export PATH="$REPO_ROOT/.venv/bin:$PATH"

          echo "parallel-web-tools dev shell"
          echo "python: $(python --version 2>/dev/null)"
          echo "uv: $(uv --version 2>/dev/null)"
          echo "node: $(node --version 2>/dev/null)"
          echo
          echo "Suggested setup:"
          echo "  uv sync --extra dev"
          echo "  uv run pre-commit install"
          echo "  uv run parallel-cli --help"

          echo "To build a binary"
          echo "  uv run python scripts/build.py"
        '';
      };
    });
}
