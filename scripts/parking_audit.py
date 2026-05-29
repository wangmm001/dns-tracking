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
import json
import os
import subprocess
import sys
import tempfile
import time
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
                   topk: int, out_path: Path) -> str:
    urls = []
    for tag in tags:
        urls.extend(shard_urls(tag, shards_config, repo)["newly_registered_domains_measurements"])
    url_list = ",\n    ".join(f"'{u}'" for u in urls)
    return f"""
INSTALL httpfs; LOAD httpfs;
SET memory_limit='6GB';
SET enable_progress_bar=false;
COPY (
  WITH base AS (
    SELECT d, s FROM read_parquet([{url_list}])
    WHERE k='ns' AND s IS NOT NULL AND d IS NOT NULL
  ),
  labeled AS (
    SELECT d, s,
           array_to_string(list_slice(string_split(s, '.'), -2, -1), '.') AS ns_apex
    FROM base
  )
  SELECT ns_apex,
         COUNT(DISTINCT d) AS new_domains,
         COUNT(DISTINCT s) AS distinct_ns_hosts,
         any_value(s)      AS sample_ns_host,
         (array_agg(DISTINCT d ORDER BY d))[1:5] AS sample_domains
  FROM labeled
  GROUP BY ns_apex
  ORDER BY new_domains DESC
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
                  tags: list[str], threshold: int = 1000) -> str:
    csv_path = topk_path.with_suffix(".csv")
    subprocess.run(
        ["duckdb", "-csv", "-c",
         f"SELECT * FROM '{topk_path}'"], stdout=open(csv_path, "w"), check=True,
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
    return "\n".join(lines) + "\n"


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
    sql = build_topk_sql(tags, args.shards_config, args.repo,
                         args.topk, topk_path)
    sql_file = workdir / "audit.sql"
    sql_file.write_text(sql)
    subprocess.run(["duckdb", "-f", str(sql_file)], check=True)

    providers = load_providers(args.config)
    report = render_report(topk_path, providers, args.window_days, tags)
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
    title = f"Parking audit {month}: {sum(1 for ln in report.splitlines() if ln.startswith('| `'))} candidates"
    body  = report
    subprocess.run(
        ["gh", "issue", "create", "-R", args.repo,
         "--title", title, "--body", body, "--label", "parking-audit"],
        check=False,  # don't fail the workflow if label is missing
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
