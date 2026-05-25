#!/usr/bin/env python3
"""Mirror wangmm001/dns-tracking parquet snapshots into a local Hive-partitioned archive.

Default behavior: scan every release on the repo and download every parquet
asset that's missing locally. Re-runs back-fill anything a previous run
skipped or that failed mid-flight, so a stalled cron can't leave permanent
gaps. No GitHub login required (anonymous REST API is enough for ~1 run/h).
Set GH_TOKEN / GITHUB_TOKEN to raise the rate limit on a cold bootstrap.

Constraints (per spec):
  - Only tags matching  snap-YYYY-MM-DD-HH  are considered (no bare day tags).
  - Only .parquet assets are downloaded (jsonl.gz is ignored).

Layout (Hive-style — DuckDB picks it up with hive_partitioning=true):
    <archive>/topic=<topic>/date=YYYY-MM-DD/hour=HH/shard-<N>.parquet
    <archive>/topic=<topic>/date=YYYY-MM-DD/hour=HH/sample=true/shard-<N>.sample.parquet
        (samples only when --include-samples)

Writes go through <file>.part + atomic rename, and the final size is checked
against the API-reported size, so a crash/SIGKILL never leaves a half-written
file that looks complete.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

REPO = os.environ.get("REPO", "wangmm001/dns-tracking")
DEFAULT_ARCHIVE = Path(os.environ.get("ARCHIVE", Path.home() / "dns-tracking-archive"))
API = f"https://api.github.com/repos/{REPO}/releases"

TAG_RE = re.compile(r"^snap-(\d{4})-(\d{2})-(\d{2})-(\d{2})$")
ASSET_RE = re.compile(
    r"^(?P<topic>.+?)\.shard-(?P<shard>\d+)(?P<sample>\.sample)?\.parquet$"
)

UA = "dns-tracking-archive/2.0"


def log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] {msg}", flush=True)


def _auth_headers() -> dict[str, str]:
    h = {"User-Agent": UA}
    tok = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if tok:
        h["Authorization"] = f"Bearer {tok}"
    return h


def http_get_json(url: str):
    req = Request(url, headers={**_auth_headers(), "Accept": "application/vnd.github+json"})
    with urlopen(req, timeout=60) as r:
        return json.load(r)


def http_download(url: str, dst: Path, expected_size: int, retries: int = 3) -> None:
    """Download `url` to `dst` with HTTP Range resume on the .part tempfile.

    Resume rules:
      - If .part already equals expected_size: promote it and return (we crashed
        between download finishing and rename).
      - If .part is larger than expected_size: assume corrupt; drop and restart.
      - If .part is smaller: send `Range: bytes=<size>-` and append. If the
        server answers 200 instead of 206 (Range ignored), silently fall back
        to a full overwrite. 416 (Range not satisfiable -- asset replaced or
        .part stale beyond current size) also resets the .part.
    """
    tmp = dst.with_name(dst.name + ".part")
    last_err: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            resume_from = 0
            mode = "wb"
            if tmp.exists():
                cur = tmp.stat().st_size
                if cur == expected_size:
                    tmp.replace(dst)
                    return
                if cur > expected_size or cur < 0:
                    tmp.unlink()
                elif cur > 0:
                    resume_from = cur
                    mode = "ab"

            headers = {**_auth_headers(), "Accept": "application/octet-stream"}
            if resume_from > 0:
                headers["Range"] = f"bytes={resume_from}-"

            req = Request(url, headers=headers)
            with urlopen(req, timeout=600) as r:
                # If we asked for a range but the server returned 200, it
                # served the whole asset -- truncate and rewrite.
                if resume_from > 0 and r.status != 206:
                    mode = "wb"
                    resume_from = 0
                if resume_from > 0:
                    log(f"  resume from byte {resume_from:,}")
                with open(tmp, mode) as f:
                    shutil.copyfileobj(r, f, length=1 << 20)

            actual = tmp.stat().st_size
            if actual != expected_size:
                raise IOError(f"size mismatch: got {actual}, expected {expected_size}")
            tmp.replace(dst)
            return

        except HTTPError as e:
            last_err = e
            # 416: .part offset is past the end of the current asset (the
            # asset was replaced server-side, or .part is corrupt). Wipe and
            # let the next attempt start fresh.
            if e.code == 416:
                tmp.unlink(missing_ok=True)
            if attempt < retries:
                backoff = 2 ** attempt
                log(f"  retry {attempt}/{retries - 1} after {backoff}s: HTTP {e.code} {e.reason}")
                time.sleep(backoff)
        except (URLError, IOError, TimeoutError) as e:
            last_err = e
            # Keep .part on transient errors so the next attempt can resume.
            # Only nuke it if our size check failed (data is provably wrong).
            if isinstance(e, IOError) and "size mismatch" in str(e):
                tmp.unlink(missing_ok=True)
            if attempt < retries:
                backoff = 2 ** attempt
                log(f"  retry {attempt}/{retries - 1} after {backoff}s: {e}")
                time.sleep(backoff)

    assert last_err is not None
    raise last_err


def all_releases():
    page = 1
    while True:
        chunk = http_get_json(f"{API}?per_page=100&page={page}")
        if not chunk:
            return
        yield from chunk
        if len(chunk) < 100:
            return
        page += 1


def parse_date(s: str) -> str:
    datetime.strptime(s, "%Y-%m-%d")
    return s


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--archive", default=str(DEFAULT_ARCHIVE),
                    help=f"Archive root. Default: {DEFAULT_ARCHIVE}")
    ap.add_argument("--date", type=parse_date,
                    help="Restrict to a single date YYYY-MM-DD.")
    ap.add_argument("--since", type=parse_date,
                    help="Only releases on/after this date (inclusive).")
    ap.add_argument("--until", type=parse_date,
                    help="Only releases on/before this date (inclusive).")
    ap.add_argument("--topic", action="append",
                    help="Restrict to specific topic(s); repeatable.")
    ap.add_argument("--include-samples", action="store_true",
                    help="Also archive .sample.parquet files (small).")
    ap.add_argument("--max-releases", type=int, default=0,
                    help="Process at most N releases this run (0 = unlimited). "
                         "Useful for staging a cold-start backfill across multiple cron ticks.")
    ap.add_argument("--dry-run", action="store_true",
                    help="List what would be downloaded; do not write anything.")
    args = ap.parse_args()

    archive = Path(args.archive).expanduser()
    archive.mkdir(parents=True, exist_ok=True)
    log(f"repo={REPO} archive={archive}")

    # Collect matching releases first so we can sort oldest -> newest.
    # Oldest-first makes a partial run resumable in a sensible order
    # (we keep filling in history rather than re-downloading the newest one repeatedly).
    releases: list[tuple[str, str, dict]] = []
    for rel in all_releases():
        m = TAG_RE.match(rel.get("tag_name", ""))
        if not m:
            continue
        y, mo, d, h = m.groups()
        date_str = f"{y}-{mo}-{d}"
        if args.date and date_str != args.date:
            continue
        if args.since and date_str < args.since:
            continue
        if args.until and date_str > args.until:
            continue
        releases.append((date_str, h, rel))
    releases.sort(key=lambda x: (x[0], x[1]))
    log(f"matched {len(releases)} release(s) with strict snap-YYYY-MM-DD-HH tag")
    if not releases:
        return 0

    # Walk every (release, asset) and decide whether the local copy is already good.
    pending: list[tuple[str, str, str, str, bool, dict]] = []  # tag, date, hour, topic, is_sample, asset
    already_ok = 0
    for date_str, hour, rel in releases:
        for asset in rel.get("assets", []):
            m = ASSET_RE.match(asset["name"])
            if not m:
                continue
            topic = m["topic"]
            shard = m["shard"]
            is_sample = bool(m["sample"])
            if is_sample and not args.include_samples:
                continue
            if args.topic and topic not in args.topic:
                continue
            dst = _dst_path(archive, topic, date_str, hour, shard, is_sample)
            size = asset.get("size", -1)
            if dst.exists() and dst.stat().st_size == size:
                already_ok += 1
                continue
            pending.append((rel["tag_name"], date_str, hour, topic, is_sample, asset))

    log(f"local already complete: {already_ok} file(s); need to fetch: {len(pending)} file(s)")
    if not pending:
        return 0

    # Cap per-run release count if requested (count distinct release tags in the pending set).
    if args.max_releases > 0:
        seen: list[str] = []
        keep: list[tuple] = []
        for item in pending:
            tag = item[0]
            if tag not in seen:
                if len(seen) >= args.max_releases:
                    break
                seen.append(tag)
            keep.append(item)
        if len(keep) < len(pending):
            log(f"--max-releases={args.max_releases}: limiting to {len(seen)} release(s), "
                f"{len(keep)}/{len(pending)} files this run")
        pending = keep

    downloaded = failed = 0
    total_bytes = sum(it[5].get("size", 0) for it in pending)
    log(f"will fetch {total_bytes:,} bytes ({total_bytes / 1e9:.2f} GB) across {len(pending)} file(s)")

    for tag, date_str, hour, topic, is_sample, asset in pending:
        dst = _dst_path(archive, topic, date_str, hour, asset_shard(asset), is_sample)
        size = asset.get("size", -1)
        if args.dry_run:
            log(f"DRY-RUN  {tag}/{asset['name']}  ({size:,} B)  -> {dst}")
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        try:
            log(f"GET  {tag}/{asset['name']}  ({size:,} B)")
            http_download(asset["browser_download_url"], dst, size)
            downloaded += 1
        except (HTTPError, URLError, IOError, TimeoutError) as e:
            log(f"FAIL {tag}/{asset['name']}: {e}")
            failed += 1

    log(f"done: downloaded={downloaded} skipped(existing)={already_ok} failed={failed}")
    return 1 if failed else 0


def asset_shard(asset: dict) -> str:
    m = ASSET_RE.match(asset["name"])
    assert m, asset["name"]
    return m["shard"]


def _dst_path(archive: Path, topic: str, date_str: str, hour: str, shard: str, is_sample: bool) -> Path:
    parts = [f"topic={topic}", f"date={date_str}", f"hour={hour}"]
    if is_sample:
        parts.append("sample=true")
        fname = f"shard-{shard}.sample.parquet"
    else:
        fname = f"shard-{shard}.parquet"
    return archive.joinpath(*parts, fname)


if __name__ == "__main__":
    sys.exit(main())
