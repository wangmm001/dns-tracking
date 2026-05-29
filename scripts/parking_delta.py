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


def build_export_sql(workdir: Path, active_providers: list) -> str:
    """Generate one COPY TO parquet + one COPY TO jsonl per provider."""
    blocks = []
    for prov in active_providers:
        blocks.append(f"""
COPY (
  SELECT * FROM today_new_ct WHERE provider = '{prov.name}'
) TO '{workdir}/delta.{prov.name}.parquet'
  (FORMAT 'parquet', COMPRESSION 'zstd');

COPY (
  SELECT * FROM today_new_ct WHERE provider = '{prov.name}'
) TO '{workdir}/delta.{prov.name}.jsonl'
  (FORMAT 'json');
""")
    return "\n".join(blocks)


def write_manifest(workdir: Path, snap_tag: str, runtime_s: float,
                   config_version: int, counts: dict[str, int]) -> Path:
    manifest = workdir / "manifest.json"
    obj = {
        "snap_tag":       snap_tag,
        "config_version": config_version,
        "runtime_s":      round(runtime_s, 1),
        "run_id":         os.environ.get("GITHUB_RUN_URL", ""),
        "providers": [
            {"name": name, "new_domains": cnt}
            for name, cnt in sorted(counts.items(), key=lambda kv: -kv[1])
        ],
    }
    manifest.write_text(json.dumps(obj, indent=2))
    return manifest


def build_ct_signal_sql(snap_tag: str, shards_config: str, repo: str) -> str:
    """Add ct_sources and ct_fingerprints to today_new via apex-level
    LEFT JOIN with certstream_domains in the same snap.

    apex_of() heuristic in SQL: take last 2 dot-labels of the lowercased
    domain (matches .com/.net/.io etc; misses .co.uk-style ccTLD2LDs, which
    are absent from upstream data anyway).
    """
    ct_urls = shard_urls(snap_tag, shards_config, repo)["certstream_domains"]
    url_list_sql = ",\n    ".join(f"'{u}'" for u in ct_urls)
    return f"""
CREATE OR REPLACE TEMP TABLE ct_unnest AS
WITH cs AS (
  SELECT source, fingerprint, lower(rtrim(unnest(domain_list), '.')) AS raw_d
  FROM read_parquet([
    {url_list_sql}
  ])
  WHERE domain_list IS NOT NULL
)
SELECT
  array_to_string(list_slice(string_split(raw_d, '.'), -2, -1), '.') AS apex,
  source, fingerprint
FROM cs
WHERE raw_d <> '';

CREATE OR REPLACE TEMP TABLE today_new_ct AS
SELECT
  t.provider, t.domain, t.ns_set, t.source_topics,
  t.first_ms, t.last_ms, t.observations,
  list_sort(array_agg(DISTINCT c.source)      FILTER (WHERE c.source      IS NOT NULL)) AS ct_sources,
  list_sort(array_agg(DISTINCT c.fingerprint) FILTER (WHERE c.fingerprint IS NOT NULL)) AS ct_fingerprints
FROM today_new t
LEFT JOIN ct_unnest c ON c.apex = t.domain
GROUP BY t.provider, t.domain, t.ns_set, t.source_topics,
         t.first_ms, t.last_ms, t.observations;
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
    ct_sql = build_ct_signal_sql(args.snap_tag, args.shards_config, args.repo)

    import subprocess
    t0 = time.time()

    # Phase 1: build today_new_ct + report counts.
    sql_file.write_text(
        "INSTALL httpfs; LOAD httpfs;\n"
        "SET memory_limit='6GB';\n"
        "SET enable_progress_bar=false;\n"
        f"{today_ns_sql}\n"
        f"{anti_join_sql}\n"
        f"{ct_sql}\n"
    )

    # Phase 2: export per-provider files.
    export_sql = build_export_sql(workdir, active)
    counts_file = workdir / "counts.csv"
    sql_file.write_text(sql_file.read_text() + export_sql + f"""
COPY (
  SELECT provider, COUNT(*) AS new_today
  FROM today_new_ct GROUP BY provider
) TO '{counts_file}' (HEADER, FORMAT 'csv');
""")
    print(f"wrote SQL to {sql_file}", file=sys.stderr)
    subprocess.run(["duckdb", "-f", str(sql_file)], check=True)

    # Parse counts.csv.
    import csv
    with open(counts_file) as f:
        counts = {row["provider"]: int(row["new_today"]) for row in csv.DictReader(f)}
    # Provider with zero new domains: csv has no row; backfill 0.
    for p in active:
        counts.setdefault(p.name, 0)

    runtime = time.time() - t0
    cfg_version = json.load(open(args.config))["version"]
    manifest = write_manifest(workdir, args.snap_tag, runtime, cfg_version, counts)
    print(f"wrote {manifest}", file=sys.stderr)

    if args.dry_run:
        print(f"DRY RUN: outputs in {workdir}", file=sys.stderr)
        return 0


if __name__ == "__main__":
    sys.exit(main())
