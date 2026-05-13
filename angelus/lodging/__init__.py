"""Lodging scaffolding for YAML-backed configuration."""
"""Lodging config loading."""

from .config import Channel, Lodging, Pipe, ScheduledSource, Triager, load_lodging

__all__ = [
    "Channel",
    "Lodging",
    "Pipe",
    "ScheduledSource",
    "Triager",
    "load_lodging",
]
