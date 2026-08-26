#!/usr/bin/env python3
# scripts/parking_audit.py
"""Monthly parking-NS audit.

Scans the most recent N days of snap-* releases for top NS-apex by new-domain
count and reports any high-volume apex NOT covered by the active ns_suffix
configuration. Output: report.md + topk_ns.parquet uploaded to
parking-audit-YYYY-MM release, plus a GitHub Issue summarizing findings.
"""
from __future__ import annotations

import argparse
import csv
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scripts.parking_common import (
    REPO_DEFAULT, gh_release_assets, gh_upload_assets, load_providers,
    parse_snap_tag, shard_urls,
)


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo", default=REPO_DEFAULT)
    p.add_argument("--config", default=".github/parking_providers.json")
    p.add_argument("--shards-config", default=".github/shards.json")
    p.add_argument("--window-days", type=int, default=30)
    p.add_argument("--topk", type=int, default=500)
    p.add_argument("--memory-limit", default="8GB",
                   help="DuckDB memory_limit; the rest of the working set "
                        "spills to --workdir (ubuntu-latest has 16 GB)")
    p.add_argument("--audit-release-prefix", default="parking-audit-")
    p.add_argument("--workdir", default=None)
    p.add_argument("--dry-run", action="store_true",
                   help="Skip uploads and issue creation")
    return p.parse_args(argv)


def recent_snap_tags(repo: str, days: int) -> list[str]:
    proc = subprocess.run(
        ["gh", "release", "list", "-R", repo, "-L", "1000",
         "--json", "tagName", "--jq", ".[].tagName"],
        capture_output=True, text=True, check=True,
    )
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    tags = []
    for tag in proc.stdout.splitlines():
        try:
            d, _ = parse_snap_tag(tag)
        except ValueError:
            continue
        if d >= cutoff:
            tags.append(tag)
    return sorted(tags)


def build_topk_sql(tags: list[str], shards_config: str, repo: str,
                   topk: int, out_path: Path, temp_dir: Path,
                   memory_limit: str = "8GB") -> str:
    urls = []
    for tag in tags:
        urls.extend(shard_urls(tag, shards_config, repo)["newly_registered_domains_measurements"])
    url_list = ",\n    ".join(f"'{u}'" for u in urls)
    scan = f"""FROM read_parquet([{url_list}])
WHERE k='ns' AND s IS NOT NULL AND d IS NOT NULL"""
    apex = "array_to_string(list_slice(string_split(s, '.'), -2, -1), '.')"
    # Memory discipline. The single-statement version of this query OOM'd at
    # 5.5 GiB in 2026-07 and 2026-08, because COUNT(DISTINCT ...) and
    # array_agg(DISTINCT d ORDER BY d) build per-group state that DuckDB pins
    # and cannot spill; that state grows with the window, so no fixed limit
    # survives a 30-day one. Instead:
    #   * de-duplicate with GROUP BY, which does spill, into on-disk tables,
    #     then COUNT(*) the rows;
    #   * min(d, 5) keeps only the five smallest per group, replacing an
    #     array_agg that materialised and sorted every distinct domain per
    #     apex just to slice five off the front.
    # Deriving both tables straight from the scan costs a second pass over the
    # release parquets, but parquet column pruning makes each pass ~4 minutes
    # for 660 shards -- far cheaper than de-duplicating (ns_apex, d, s) triples
    # first, which is a larger intermediate than either table it feeds.
    return f"""
INSTALL httpfs; LOAD httpfs;
SET memory_limit='{memory_limit}';
SET temp_directory='{temp_dir}';
SET preserve_insertion_order=false;
SET enable_progress_bar=false;

CREATE OR REPLACE TABLE apex_domain AS
SELECT {apex} AS ns_apex, d
{scan}
GROUP BY ALL;

CREATE OR REPLACE TABLE apex_host AS
SELECT {apex} AS ns_apex, s
{scan}
GROUP BY ALL;

COPY (
  SELECT ad.ns_apex,
         ad.new_domains,
         ah.distinct_ns_hosts,
         ah.sample_ns_host,
         ad.sample_domains
  FROM (
    SELECT ns_apex, COUNT(*) AS new_domains, min(d, 5) AS sample_domains
    FROM apex_domain GROUP BY ns_apex
  ) ad
  JOIN (
    SELECT ns_apex, COUNT(*) AS distinct_ns_hosts, min(s) AS sample_ns_host
    FROM apex_host GROUP BY ns_apex
  ) ah USING (ns_apex)
  -- ns_apex breaks ties so the monthly report diffs cleanly run to run.
  ORDER BY new_domains DESC, ns_apex
  LIMIT {topk}
) TO '{out_path}' (FORMAT 'parquet', COMPRESSION 'zstd');
"""


def configured_apexes(providers) -> set[str]:
    """Strip leading dot to get apexes ('.dns-parking.com' -> 'dns-parking.com')."""
    out: set[str] = set()
    for p in providers:
        if p.active:
            for s in p.ns_suffix:
                out.add(s.lstrip("."))
    return out


def render_report(topk_path: Path, providers, window_days: int,
                  tags: list[str], threshold: int = 1000) -> tuple[str, int]:
    csv_path = topk_path.with_suffix(".csv")
    with open(csv_path, "w") as fh:
        subprocess.run(
            ["duckdb", "-csv", "-c", f"SELECT * FROM '{topk_path}'"],
            stdout=fh, check=True,
        )
    configured = configured_apexes(providers)
    unhandled, handled = [], []
    with open(csv_path) as f:
        for row in csv.DictReader(f):
            cnt = int(row["new_domains"])
            if cnt < threshold:
                continue
            if row["ns_apex"] in configured:
                handled.append(row)
            else:
                unhandled.append(row)
    lines = [
        f"# Parking-NS audit — last {window_days} days",
        f"",
        f"- Window: {tags[0]} → {tags[-1]} ({len(tags)} snaps)",
        f"- Configured (active) provider apexes: {len(configured)}",
        f"- Unhandled high-concentration apexes (≥ {threshold} new domains): "
        f"**{len(unhandled)}**",
        "",
        "## Unhandled (review and triage)",
        "",
        "| ns_apex | new_domains | distinct_ns_hosts | sample_ns_host | sample_domains |",
        "|---|---:|---:|---|---|",
    ]
    for r in unhandled[:50]:
        lines.append(
            f"| `{r['ns_apex']}` | {r['new_domains']} | {r['distinct_ns_hosts']} | "
            f"`{r['sample_ns_host']}` | `{r['sample_domains']}` |"
        )
    lines += [
        "",
        "## Configured (sanity check)",
        "",
        "| ns_apex | new_domains | distinct_ns_hosts |",
        "|---|---:|---:|",
    ]
    for r in sorted(handled, key=lambda x: -int(x["new_domains"]))[:30]:
        lines.append(
            f"| `{r['ns_apex']}` | {r['new_domains']} | {r['distinct_ns_hosts']} |"
        )
    return "\n".join(lines) + "\n", len(unhandled)


def main(argv=None) -> int:
    args = parse_args(argv)
    workdir = Path(args.workdir or tempfile.mkdtemp(prefix="parking-audit-"))
    workdir.mkdir(parents=True, exist_ok=True)

    tags = recent_snap_tags(args.repo, args.window_days)
    if not tags:
        print("No snaps in window", file=sys.stderr); return 1
    print(f"audit window: {len(tags)} snaps "
          f"({tags[0]} → {tags[-1]})", file=sys.stderr)

    topk_path = workdir / "topk_ns.parquet"
    temp_dir = workdir / "duckdb_tmp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    sql = build_topk_sql(tags, args.shards_config, args.repo,
                         args.topk, topk_path, temp_dir, args.memory_limit)
    sql_file = workdir / "audit.sql"
    sql_file.write_text(sql)
    # Run against an on-disk database, not the default in-memory one: the
    # de-duplicated intermediates are the bulk of the working set and belong
    # on the runner's disk. Start from a clean file so CREATE OR REPLACE never
    # inherits a half-written table from an interrupted run.
    db_path = workdir / "audit.duckdb"
    for stale in (db_path, db_path.with_suffix(".duckdb.wal")):
        stale.unlink(missing_ok=True)
    # Read SQL via stdin (not -f): the -f flag was added in DuckDB CLI 1.4;
    # the GH Actions runner pins an older version that treats the path as a DB.
    with open(sql_file) as fh:
        subprocess.run(["duckdb", str(db_path)], stdin=fh, check=True)

    providers = load_providers(args.config)
    report, unhandled_count = render_report(topk_path, providers, args.window_days, tags)
    report_path = workdir / "report.md"
    report_path.write_text(report)
    print(f"wrote {topk_path} and {report_path}", file=sys.stderr)

    if args.dry_run:
        print(f"DRY RUN: outputs in {workdir}", file=sys.stderr)
        return 0

    month = datetime.now(timezone.utc).strftime("%Y-%m")
    audit_tag = f"{args.audit_release_prefix}{month}"
    gh_upload_assets(audit_tag, [str(topk_path), str(report_path)],
                     args.repo, title=f"Parking audit {month}")
    print(f"uploaded audit to {audit_tag}", file=sys.stderr)

    # Open or update issue.
    title = f"Parking audit {month}: {unhandled_count} candidate{'s' if unhandled_count != 1 else ''}"
    body  = report
    subprocess.run(
        ["gh", "issue", "create", "-R", args.repo,
         "--title", title, "--body", body, "--label", "parking-audit"],
        check=False,  # don't fail the workflow if label is missing
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
