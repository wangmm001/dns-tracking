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


def verify_snap_complete(snap_tag: str, shards_config: str, repo: str,
                         retry_after_s: int = 300,
                         max_retries: int = 1) -> None:
    """Sleep+retry until all shards from shards.json have landed on the
    snap-* release; raise after max_retries if still incomplete."""
    from scripts.parking_common import gh_release_assets
    expected_files = set()
    cfg = json.load(open(shards_config))
    for topic, meta in cfg.items():
        for i in range(meta["shards"]):
            expected_files.add(f"{topic}.shard-{i}.parquet")
    for attempt in range(max_retries + 1):
        have = set(gh_release_assets(snap_tag, repo))
        missing = expected_files - have
        if not missing:
            print(f"snap_tag={snap_tag} complete ({len(have)} assets)",
                  file=sys.stderr)
            return
        print(f"snap_tag={snap_tag} missing {len(missing)}: "
              f"{sorted(missing)[:3]}…", file=sys.stderr)
        if attempt == max_retries:
            raise RuntimeError(
                f"snap {snap_tag} still missing {len(missing)} shards after "
                f"{attempt + 1} checks"
            )
        time.sleep(retry_after_s)


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

    if not args.force_incomplete:
        verify_snap_complete(args.snap_tag, args.shards_config, args.repo)
    else:
        print(f"--force-incomplete: skipping shard verification",
              file=sys.stderr)

    # Subsequent tasks add: SQL execution, upload steps.
    return 0


if __name__ == "__main__":
    sys.exit(main())
