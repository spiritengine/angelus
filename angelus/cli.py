"""Command-line entry point for Angelus."""

from __future__ import annotations

import click

from angelus.daemon import main as daemon_main


@click.group()
def main() -> None:
    """Angelus scheduling and escalation spine."""


@main.command()
def daemon() -> None:
    """Start the Angelus daemon."""
    daemon_main()
