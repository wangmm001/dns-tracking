#!/usr/bin/env python3
# scripts/parking_delta.py
"""Per-snap parking-NS delta producer.

Reads one snap-YYYY-MM-DD-HH release, filters across 3 Avro topics for
NS records whose hostname matches a configured ns_suffix, anti-joins
against the cumulative seen-YYYY.parquet state, LEFT JOINs CT signal,
and writes one delta.{provider}.parquet + .jsonl per provider plus a
manifest.json.

See docs/specs/2026-05-29-parking-tracking-design.md.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import time
from pathlib import Path

from scripts.parking_common import (
    REPO_DEFAULT, load_providers, parse_snap_tag, shard_urls,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("snap_tag", help="snap-YYYY-MM-DD-HH")
    p.add_argument("--config",       default=".github/parking_providers.json")
    p.add_argument("--shards-config", default=".github/shards.json")
    p.add_argument("--repo",         default=REPO_DEFAULT)
    p.add_argument("--state-release", default="parking-state")
    p.add_argument("--delta-release-prefix", default="parking-DAY-")
    p.add_argument("--workdir",      default=None,
                   help="Working dir; defaults to a tempdir")
    p.add_argument("--dry-run", action="store_true",
                   help="Skip state download and all uploads; outputs go to --workdir")
    p.add_argument("--force-incomplete", action="store_true",
                   help="Run even if snap-* release is missing shards")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    date, hour = parse_snap_tag(args.snap_tag)
    print(f"snap_tag={args.snap_tag} date={date} hour={hour}", file=sys.stderr)

    workdir = Path(args.workdir or tempfile.mkdtemp(prefix="parking-delta-"))
    workdir.mkdir(parents=True, exist_ok=True)
    print(f"workdir={workdir} dry_run={args.dry_run}", file=sys.stderr)

    providers = load_providers(args.config)
    active = [p for p in providers if p.active]
    print(f"providers: {len(active)} active / {len(providers)} total",
          file=sys.stderr)

    # Subsequent tasks add: shard verification, SQL execution, upload steps.
    return 0


if __name__ == "__main__":
    sys.exit(main())
