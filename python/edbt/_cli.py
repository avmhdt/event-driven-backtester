"""Console-script entry point for the ``edbt`` command installed with the wheel."""

import sys

from edbt._edbt import cli_main


def main() -> None:
    """Run the Rust CLI with this process's arguments and exit with its code."""
    sys.exit(cli_main(["edbt", *sys.argv[1:]]))


if __name__ == "__main__":
    main()
