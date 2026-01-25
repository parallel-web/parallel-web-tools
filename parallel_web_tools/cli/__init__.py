"""CLI for Parallel Data."""

try:
    from parallel_web_tools.cli.commands import main
except ImportError:
    import sys

    def main():
        """Stub for when CLI dependencies are not installed."""
        print("parallel-cli requires additional dependencies.")
        print("")
        print("Install with: pip install parallel-web-tools[cli]")
        sys.exit(1)


__all__ = ["main"]
