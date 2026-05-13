"""Command-line entry point for Angelus."""

from __future__ import annotations

import click


@click.group()
def main() -> None:
    """Angelus scheduling and escalation spine."""


@main.command()
def daemon() -> None:
    """Start the Angelus daemon."""
    click.echo("angelus daemon is not implemented yet")
