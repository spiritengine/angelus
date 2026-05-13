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


@main.command()
def health() -> None:
    """Show daemon and dependency health."""
    click.echo("angelus health is not implemented yet")


@main.command()
@click.argument("source", required=False)
def reprocess(source: str | None = None) -> None:
    """Reprocess observations for a source."""
    click.echo("angelus reprocess is not implemented yet")


@main.command()
@click.argument("dedup_key", required=False)
@click.argument("duration", required=False)
def mute(dedup_key: str | None = None, duration: str | None = None) -> None:
    """Mute dispatches for a dedup key."""
    click.echo("angelus mute is not implemented yet")


@main.group()
def incident() -> None:
    """Inspect or update incidents."""


@incident.command("list")
def incident_list() -> None:
    """List incidents."""
    click.echo("angelus incident list is not implemented yet")


@incident.command("close")
@click.argument("incident_id", required=False)
@click.option("--comment", default=None)
def incident_close(incident_id: str | None = None, comment: str | None = None) -> None:
    """Close an incident."""
    click.echo("angelus incident close is not implemented yet")


@main.command()
@click.argument("finding_id", required=False)
def replay(finding_id: str | None = None) -> None:
    """Replay dispatch for a finding."""
    click.echo("angelus replay is not implemented yet")
