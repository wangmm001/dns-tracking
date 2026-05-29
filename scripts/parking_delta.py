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


def build_today_ns_sql(snap_tag: str, shards_config: str, repo: str,
                       active_providers: list) -> str:
    """Return a SQL block that produces a `today_ns` temp table.

    Columns: provider, domain, ns_set, source_topics, first_ms, last_ms,
    observations.
    """
    urls = shard_urls(snap_tag, shards_config, repo)
    avro_topics = [
        "newly_registered_domains_measurements",
        "newly_registered_fqdn_measurements",
        "newly_issued_certificates_measurements",
    ]
    all_urls = [u for t in avro_topics for u in urls[t]]
    url_list_sql = ",\n    ".join(f"'{u}'" for u in all_urls)

    union_blocks = []
    for prov in active_providers:
        for suf in prov.ns_suffix:
            union_blocks.append(
                f"SELECT '{prov.name}' AS provider, topic, d, s, fs, ls, n "
                f"FROM src WHERE k='ns' AND s IS NOT NULL "
                f"AND d IS NOT NULL AND ends_with(s, '{suf}')"
            )
    union_sql = "\n  UNION ALL\n  ".join(union_blocks)

    return f"""
CREATE OR REPLACE TEMP TABLE today_ns AS
WITH src AS (
  SELECT * FROM read_parquet([
    {url_list_sql}
  ])
),
matched AS (
  {union_sql}
)
SELECT provider,
       d AS domain,
       list_sort(array_agg(DISTINCT s))     AS ns_set,
       list_sort(array_agg(DISTINCT topic)) AS source_topics,
       MIN(fs) AS first_ms,
       MAX(ls) AS last_ms,
       SUM(n)  AS observations
FROM matched
GROUP BY provider, d;
"""


def download_seen_years(state_release: str, repo: str, dest: Path,
                        dry_run: bool) -> list[Path]:
    """Download all seen-YYYY.parquet assets from the state release.
    Returns local paths (possibly empty). Bootstrap empty if release missing."""
    if dry_run:
        return []
    from scripts.parking_common import gh_release_assets, gh_download_asset
    have = gh_release_assets(state_release, repo)
    matching = sorted(n for n in have if n.startswith("seen-") and n.endswith(".parquet"))
    out: list[Path] = []
    for name in matching:
        path = Path(gh_download_asset(state_release, name, str(dest), repo))
        out.append(path)
    return out


def build_anti_join_sql(seen_paths: list[Path]) -> str:
    """Replace today_ns with a copy excluding (provider, domain) pairs already in seen."""
    if not seen_paths:
        return (
            "CREATE OR REPLACE TEMP TABLE today_new AS "
            "SELECT * FROM today_ns;"
        )
    seen_list = ", ".join(f"'{p}'" for p in seen_paths)
    return f"""
CREATE OR REPLACE TEMP TABLE seen AS
SELECT provider, domain FROM read_parquet([{seen_list}], union_by_name=true);

CREATE OR REPLACE TEMP TABLE today_new AS
SELECT t.* FROM today_ns t
ANTI JOIN seen USING (provider, domain);
"""


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

    sql_file = workdir / "delta.sql"
    today_ns_sql = build_today_ns_sql(
        args.snap_tag, args.shards_config, args.repo, active,
    )

    seen_dir = workdir / "seen"
    seen_dir.mkdir(exist_ok=True)
    seen_paths = download_seen_years(args.state_release, args.repo,
                                     seen_dir, args.dry_run)
    print(f"loaded {len(seen_paths)} seen-YYYY.parquet year(s)",
          file=sys.stderr)

    anti_join_sql = build_anti_join_sql(seen_paths)
    # Update sql_file: prepend after today_ns, before counts.
    counts_sql = (
        "SELECT provider, COUNT(*) AS new_today "
        "FROM today_new GROUP BY provider ORDER BY new_today DESC;"
    )
    sql_file.write_text(
        "INSTALL httpfs; LOAD httpfs;\n"
        "SET memory_limit='6GB';\n"
        "SET enable_progress_bar=false;\n"
        f"{today_ns_sql}\n"
        f"{anti_join_sql}\n"
        f"{counts_sql}\n"
    )
    print(f"wrote SQL to {sql_file}", file=sys.stderr)

    if args.dry_run:
        import subprocess
        subprocess.run(["duckdb", "-f", str(sql_file)], check=True)
        return 0


if __name__ == "__main__":
    sys.exit(main())
