# scripts/parking_common.py
"""Shared utilities for parking_delta.py and parking_audit.py.

No external deps so unit tests stay light. gh CLI calls live in
parking_delta.py / parking_audit.py.
"""
from __future__ import annotations

import json
import re
import subprocess
from dataclasses import dataclass
from typing import Iterable, Sequence

SNAP_TAG_RE = re.compile(r"^snap-(\d{4}-\d{2}-\d{2})-(00|12)$")
REPO_DEFAULT = "wangmm001/dns-tracking"


def apex_of(domain: str) -> str:
    """Last-two-labels heuristic for apex extraction.

    Matches `.com / .net / .io` correctly; misses ccTLD2LDs like `.co.uk`
    (acceptable for v1 since OpenINTEL upstream barely covers ccTLDs).
    """
    if not domain:
        return ""
    s = domain.rstrip(".").lower()
    parts = s.split(".")
    if len(parts) <= 2:
        return s
    return ".".join(parts[-2:])


def parse_snap_tag(tag: str) -> tuple[str, str]:
    """('snap-2026-05-28-12') -> ('2026-05-28', '12'). Raises ValueError."""
    m = SNAP_TAG_RE.match(tag or "")
    if not m:
        raise ValueError(f"not a snap tag: {tag!r}")
    return m.group(1), m.group(2)


def snap_url(tag: str, topic: str, shard: int, repo: str = REPO_DEFAULT) -> str:
    return (
        f"https://github.com/{repo}/releases/download/"
        f"{tag}/{topic}.shard-{shard}.parquet"
    )


def shard_urls(tag: str, shards_json_path: str, repo: str = REPO_DEFAULT) -> dict[str, list[str]]:
    """Return {topic: [url, ...]} for all topics listed in shards.json."""
    cfg = json.load(open(shards_json_path))
    return {
        topic: [snap_url(tag, topic, i, repo) for i in range(meta["shards"])]
        for topic, meta in cfg.items()
    }


@dataclass(frozen=True)
class Provider:
    name: str
    org: str
    active: bool
    watchlist: bool
    ns_suffix: tuple[str, ...]
    note: str | None
    seeded_from: str | None


def load_providers(config_path: str) -> list[Provider]:
    raw = json.load(open(config_path))
    return [
        Provider(
            name        = p["name"],
            org         = p["org"],
            active      = p["active"],
            watchlist   = bool(p.get("watchlist", False)),
            ns_suffix   = tuple(p["ns_suffix"]),
            note        = p.get("note"),
            seeded_from = p.get("seeded_from"),
        )
        for p in raw["providers"]
    ]


# ---- gh CLI thin wrappers (mockable; tested in dry-run integration only) ----

def gh_release_assets(tag: str, repo: str = REPO_DEFAULT) -> list[str]:
    """Return asset filenames for `tag`, or [] if release does not exist."""
    proc = subprocess.run(
        ["gh", "release", "view", tag, "-R", repo,
         "--json", "assets", "--jq", ".assets[].name"],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        return []
    return [n for n in proc.stdout.splitlines() if n]


def gh_download_asset(tag: str, asset: str, dest_dir: str,
                      repo: str = REPO_DEFAULT) -> str:
    """Download a single asset; returns local path. Caller mkdir's dest_dir."""
    subprocess.run(
        ["gh", "release", "download", tag, "-R", repo,
         "--pattern", asset, "--dir", dest_dir, "--clobber"],
        check=True,
    )
    return f"{dest_dir}/{asset}"


def gh_upload_assets(tag: str, files: Sequence[str],
                     repo: str = REPO_DEFAULT, title: str | None = None) -> None:
    """Create or update `tag` release, upload files with --clobber."""
    proc = subprocess.run(
        ["gh", "release", "view", tag, "-R", repo],
        capture_output=True,
    )
    if proc.returncode != 0:
        subprocess.run(
            ["gh", "release", "create", tag, "-R", repo,
             "--title", title or tag, "--notes", ""],
            check=True,
        )
    subprocess.run(
        ["gh", "release", "upload", tag, *files, "-R", repo, "--clobber"],
        check=True,
    )
