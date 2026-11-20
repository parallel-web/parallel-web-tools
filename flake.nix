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

      # Runs parallel-cli from the checkout the dev shell was entered in, from
      # any directory. uv syncs the project's venv first when it is stale.
      parallelCliDev = pkgs.writeShellScriptBin "parallel-cli-dev" ''
        if [ -z "''${PARALLEL_WEB_TOOLS_ROOT:-}" ]; then
          echo "parallel-cli-dev: PARALLEL_WEB_TOOLS_ROOT is not set; enter the dev shell with 'nix develop'" >&2
          exit 1
        fi
        exec ${pkgs.uv}/bin/uv run --project "$PARALLEL_WEB_TOOLS_ROOT" parallel-cli "$@"
      '';

      basePackages = with pkgs; [
        python
        uv
        nodejs_24
        pnpm
        parallelCliDev
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
          export PARALLEL_WEB_TOOLS_ROOT="$REPO_ROOT"

          echo "parallel-web-tools dev shell"
          echo "python: $(python --version 2>/dev/null)"
          echo "uv: $(uv --version 2>/dev/null)"
          echo "node: $(node --version 2>/dev/null)"
          echo
          echo "Suggested setup:"
          echo "  uv sync --extra dev"
          echo "  uv run pre-commit install"
          echo "  uv run parallel-cli --help"
          echo
          echo "Run this checkout's CLI from any directory:"
          echo "  parallel-cli-dev --help"
          echo

          echo "To build a binary"
          echo "  uv run python scripts/build.py"
        '';
      };
    });
}
