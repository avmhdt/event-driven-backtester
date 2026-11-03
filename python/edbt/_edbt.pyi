"""Type stubs for the Rust extension module."""

__all__ = ["__version__", "cli_main"]
__version__: str

def cli_main(argv: list[str]) -> int:
    """Run the edbt CLI with ``argv`` (including the program name)."""
