# Parking-NS tracking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land per-snap parking-NS delta tracking + monthly NS-concentration audit in the dns-tracking repo, per spec `docs/specs/2026-05-29-parking-tracking-design.md`.

**Architecture:** New per-snap workflow (`parking_daily.yml`) drives a DuckDB script (`parking_delta.py`) that scans 3 Avro topics, filters by configured `ns_suffix`, anti-joins against a permanent `seen-YYYY.parquet` state release, LEFT JOINs CT signal, and emits per-provider delta parquet/jsonl assets to `parking-DAY-…` releases. A monthly audit (`parking_audit.yml` + `parking_audit.py`) re-runs top-K NS concentration to surface config gaps and opens a tracking issue. cleanup.yml regex is extended to clean `parking-DAY-…` releases on the same 30-day window.

**Tech Stack:** GitHub Actions (bash + matrix), `gh` CLI for release I/O, DuckDB 1.x (`httpfs` extension for remote parquet reads), Python 3.11 stdlib + pyarrow, pytest for utility-function tests.

---

## File Structure

**Create:**
- `.github/parking_providers.json` — provider config (19 entries: 18 active + 1 watchlist)
- `.github/parking_providers.schema.json` — JSON Schema for config validation
- `.github/workflows/parking_daily.yml` — per-snap delta workflow
- `.github/workflows/parking_audit.yml` — monthly audit workflow
- `scripts/parking_common.py` — shared utilities (apex_of, snap_url, snap_tag parsing)
- `scripts/parking_delta.py` — per-snap delta SQL driver
- `scripts/parking_audit.py` — top-K NS + diff + report
- `tests/test_parking_common.py` — pytest unit tests for pure utilities
- `tests/conftest.py` — pytest path setup
- `pyproject.toml` — minimal pytest config (none exists currently)

**Modify:**
- `.github/workflows/cleanup.yml:60-63` — extend regex to also match `parking-DAY-…`

**Releases (created at runtime, not files):**
- `parking-state` — permanent, asset: `seen-YYYY.parquet` per year
- `parking-DAY-YYYY-MM-DD-HH` — per snap, asset: `delta.{provider}.parquet` + `delta.{provider}.jsonl` + `manifest.json`
- `parking-audit-YYYY-MM` — monthly, asset: `topk_ns.parquet` + `report.md`

---

## Phase 1: Foundation (config + common module + tests)

### Task 1.1: Add pyproject.toml so pytest discovers tests

**Files:**
- Create: `pyproject.toml`

- [ ] **Step 1: Create pyproject.toml**

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
addopts  = "-q"
```

- [ ] **Step 2: Commit**

```bash
git add pyproject.toml
git commit -m "build: add minimal pyproject.toml for pytest discovery"
```

### Task 1.2: Add JSON Schema for parking_providers.json

**Files:**
- Create: `.github/parking_providers.schema.json`

- [ ] **Step 1: Write the schema**

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://github.com/wangmm001/dns-tracking/.github/parking_providers.schema.json",
  "title": "Parking provider configuration",
  "type": "object",
  "required": ["version", "providers"],
  "properties": {
    "version": { "const": 1 },
    "providers": {
      "type": "array",
      "minItems": 1,
      "items": {
        "type": "object",
        "required": ["name", "org", "active", "ns_suffix"],
        "additionalProperties": false,
        "properties": {
          "name":      { "type": "string", "pattern": "^[a-z][a-z0-9_]*$" },
          "org":       { "type": "string", "minLength": 1 },
          "active":    { "type": "boolean" },
          "watchlist": { "type": "boolean" },
          "ns_suffix": {
            "type": "array",
            "minItems": 1,
            "items": { "type": "string", "pattern": "^\\.[a-z0-9.-]+$" }
          },
          "note":         { "type": "string" },
          "seeded_from":  { "type": "string", "pattern": "^snap-[0-9]{4}-[0-9]{2}-[0-9]{2}-(00|12)$" }
        }
      }
    }
  }
}
```

- [ ] **Step 2: Verify schema parses**

Run: `python3 -c "import json; json.load(open('.github/parking_providers.schema.json'))"`
Expected: no output, exit 0

- [ ] **Step 3: Commit**

```bash
git add .github/parking_providers.schema.json
git commit -m "feat(parking): add JSON Schema for parking_providers config"
```

### Task 1.3: Write the seed parking_providers.json

**Files:**
- Create: `.github/parking_providers.json`

- [ ] **Step 1: Write the config**

```json
{
  "version": 1,
  "providers": [
    { "name": "team_internet_dns_parking", "org": "Team Internet", "active": true,
      "ns_suffix": [".dns-parking.com"],
      "note": "Main NS as of 2026; replaced parkingcrew.net for new domains" },
    { "name": "team_internet_dyna_ns", "org": "Team Internet", "active": true,
      "ns_suffix": [".dyna-ns.net"],
      "note": "Inferred Team Internet brand: shares AS206834 with parkingcrew.net" },
    { "name": "team_internet_parkingcrew", "org": "Team Internet", "active": true,
      "ns_suffix": [".parkingcrew.net"],
      "note": "Legacy NS; ~200 new domains/24h as of 2026-05" },
    { "name": "above_domains", "org": "Above.com (Trellian)", "active": true,
      "ns_suffix": [".abovedomains.com"],
      "note": "AS133618; main NS as of 2026" },
    { "name": "above_legacy", "org": "Above.com (Trellian)", "active": true,
      "ns_suffix": [".above.com"],
      "note": "Legacy NS; mostly migrated to abovedomains.com" },
    { "name": "above_redirect", "org": "Above.com (Trellian)", "active": true,
      "ns_suffix": [".dns-redirect.com"],
      "note": "Inferred Above-family: redirector pattern across AS212317 + AWS IE/US" },
    { "name": "parklogic", "org": "ParkLogic", "active": true,
      "ns_suffix": [".parklogic.com"] },
    { "name": "sedo", "org": "Sedo", "active": true,
      "ns_suffix": [".sedoparking.com"],
      "note": "AS47846" },
    { "name": "bodis", "org": "Bodis", "active": true,
      "ns_suffix": [".bodis.com"] },
    { "name": "cashparking", "org": "GoDaddy", "active": true,
      "ns_suffix": [".cashparking.com"] },
    { "name": "internettraffic", "org": "Internet Traffic", "active": true,
      "ns_suffix": [".internettraffic.com"] },
    { "name": "share_dns", "org": "Share-DNS (CN)", "active": true,
      "ns_suffix": [".share-dns.com", ".share-dns.net"],
      "note": "Concurrent NS pair (a* on .com / b* on .net) — combined per design" },
    { "name": "spixiv", "org": "Spixiv (CN)", "active": true,
      "ns_suffix": [".spixiv.com"] },
    { "name": "julydns", "org": "JulyDNS (CN)", "active": true,
      "ns_suffix": [".julydns.com"] },
    { "name": "xundns", "org": "XunDNS (CN)", "active": true,
      "ns_suffix": [".xundns.com"] },
    { "name": "taoa", "org": "Taoa (CN)", "active": true,
      "ns_suffix": [".taoa.com"] },
    { "name": "mismes", "org": "Mismes (CN)", "active": true,
      "ns_suffix": [".mismes.com"] },
    { "name": "jindun9", "org": "Jindun9 (CN)", "active": true,
      "ns_suffix": [".jindun9.com"] },
    { "name": "onclouddns", "org": "OnCloudDNS (CN)", "active": true,
      "ns_suffix": [".onclouddns.com"] },
    { "name": "dnsowl", "org": "Unknown (Sedo AS-adjacent)", "active": false, "watchlist": true,
      "ns_suffix": [".dnsowl.com"],
      "note": "AS47846 (Sedo) but samples look aftermarket/phish; revisit before promoting" }
  ]
}
```

- [ ] **Step 2: Validate against schema**

```bash
python3 - <<'PY'
import json
from urllib.request import urlopen
import sys
try:
    import jsonschema
except ImportError:
    print("pip install jsonschema && retry"); sys.exit(2)
cfg    = json.load(open('.github/parking_providers.json'))
schema = json.load(open('.github/parking_providers.schema.json'))
jsonschema.validate(cfg, schema)
print(f"OK: {len(cfg['providers'])} providers ({sum(1 for p in cfg['providers'] if p['active'])} active)")
PY
```
Expected: `OK: 20 providers (19 active)`

- [ ] **Step 3: Commit**

```bash
git add .github/parking_providers.json
git commit -m "feat(parking): seed parking_providers.json with 19 active + 1 watchlist"
```

### Task 1.4: Test scaffold + apex_of unit tests (TDD)

**Files:**
- Create: `tests/conftest.py`
- Create: `tests/test_parking_common.py`

- [ ] **Step 1: Make `scripts/` a Python package**

```bash
: > scripts/__init__.py
```
(Empty file. Required so `from scripts.parking_common import …` works
under pytest and `python3 -m scripts.parking_delta`.)

- [ ] **Step 2: Write conftest.py to add repo root to sys.path**

```python
# tests/conftest.py
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir))
```

- [ ] **Step 3: Write failing tests for apex_of**

```python
# tests/test_parking_common.py
import pytest
from scripts.parking_common import apex_of, parse_snap_tag, snap_url


class TestApexOf:
    @pytest.mark.parametrize("inp,expected", [
        ("example.com",               "example.com"),
        ("www.example.com",           "example.com"),
        ("a.b.c.example.com",         "example.com"),
        ("EXAMPLE.COM",               "example.com"),
        ("xn--80ak6aa92e.com",        "xn--80ak6aa92e.com"),
        ("foo.io",                    "foo.io"),
        ("",                          ""),
        ("singlelabel",               "singlelabel"),
    ])
    def test_last_two_labels_heuristic(self, inp, expected):
        assert apex_of(inp) == expected

    def test_trailing_dot_stripped(self):
        assert apex_of("www.example.com.") == "example.com"


class TestParseSnapTag:
    @pytest.mark.parametrize("inp,expected", [
        ("snap-2026-05-28-00", ("2026-05-28", "00")),
        ("snap-2026-05-28-12", ("2026-05-28", "12")),
    ])
    def test_valid_tags(self, inp, expected):
        assert parse_snap_tag(inp) == expected

    @pytest.mark.parametrize("bad", [
        "snap-2026-05-28",          # legacy, no HH
        "snap-2026-5-28-00",        # not zero-padded
        "snap-2026-05-28-06",       # invalid hour
        "parking-DAY-2026-05-28-00",
        "",
    ])
    def test_invalid_tags(self, bad):
        with pytest.raises(ValueError):
            parse_snap_tag(bad)


class TestSnapUrl:
    def test_format(self):
        url = snap_url("snap-2026-05-28-12",
                       "newly_registered_domains_measurements", 3,
                       repo="wangmm001/dns-tracking")
        assert url == (
            "https://github.com/wangmm001/dns-tracking/releases/download/"
            "snap-2026-05-28-12/"
            "newly_registered_domains_measurements.shard-3.parquet"
        )
```

- [ ] **Step 4: Run tests, verify they fail (no module yet)**

Run: `pytest tests/test_parking_common.py -v`
Expected: collection error: `ModuleNotFoundError: No module named 'scripts.parking_common'`

- [ ] **Step 5: Commit (tests-first)**

```bash
git add scripts/__init__.py tests/conftest.py tests/test_parking_common.py
git commit -m "test(parking): failing unit tests for parking_common utilities"
```

### Task 1.5: Implement parking_common.py to pass Task 1.4 tests

**Files:**
- Create: `scripts/parking_common.py`

- [ ] **Step 1: Implement utilities**

```python
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
```

- [ ] **Step 2: Run tests, expect PASS**

Run: `pytest tests/test_parking_common.py -v`
Expected: `13 passed` (8 apex_of + 2 valid + 5 invalid + 1 snap_url, give or take parametrize fan-out)

- [ ] **Step 3: Commit**

```bash
git add scripts/parking_common.py
git commit -m "feat(parking): implement parking_common utilities (apex_of, snap_url, gh wrappers)"
```

---

## Phase 2: parking_delta.py — per-snap delta script

The script is built incrementally with each task adding one SQL CTE / one IO step, and after each task we run it in --dry-run against `snap-2026-05-28-12` and assert known baseline counts (from spec §2). Baseline numbers may drift slightly with broker re-runs; allow ±5%.

### Task 2.1: argparse skeleton + --help

**Files:**
- Create: `scripts/parking_delta.py`

- [ ] **Step 1: Write skeleton**

```python
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
```

- [ ] **Step 2: Verify --help works**

Run: `python3 -m scripts.parking_delta --help`
Expected: argparse usage printed, exit 0

- [ ] **Step 3: Verify it loads config & prints provider count**

Run: `python3 -m scripts.parking_delta snap-2026-05-28-12 --dry-run`
Expected stderr to contain: `providers: 19 active / 20 total` (workdir line varies)

- [ ] **Step 4: Commit**

```bash
git add scripts/parking_delta.py
git commit -m "feat(parking): parking_delta.py skeleton (argparse + config loading)"
```

### Task 2.2: Shard-completeness check + retry

**Files:**
- Modify: `scripts/parking_delta.py` (add `verify_snap_complete` and call)

- [ ] **Step 1: Add helper at module scope**

```python
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
```

- [ ] **Step 2: Wire into main()** after `print(f"providers: …")`:

```python
    if not args.force_incomplete:
        verify_snap_complete(args.snap_tag, args.shards_config, args.repo)
    else:
        print(f"--force-incomplete: skipping shard verification",
              file=sys.stderr)
```

- [ ] **Step 3: Smoke-test against a known-complete snap**

Run: `python3 -m scripts.parking_delta snap-2026-05-28-12 --dry-run`
Expected stderr to contain: `snap_tag=snap-2026-05-28-12 complete (20 assets)`

- [ ] **Step 4: Commit**

```bash
git add scripts/parking_delta.py
git commit -m "feat(parking): verify snap-* release completeness before SQL"
```

### Task 2.3: Build today_ns CTE (UNION ALL across active providers)

**Files:**
- Modify: `scripts/parking_delta.py` (add `build_today_ns_sql`)

- [ ] **Step 1: Add SQL builder**

Add at module scope:

```python
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
```

- [ ] **Step 2: Hook into main() — write SQL to file + smoke-execute via duckdb CLI in dry-run**

After provider loading in main():

```python
    sql_file = workdir / "delta.sql"
    today_ns_sql = build_today_ns_sql(
        args.snap_tag, args.shards_config, args.repo, active,
    )
    counts_sql = (
        "SELECT provider, COUNT(*) AS new_today "
        "FROM today_ns GROUP BY provider ORDER BY new_today DESC;"
    )
    sql_file.write_text(
        "INSTALL httpfs; LOAD httpfs;\n"
        "SET memory_limit='6GB';\n"
        "SET enable_progress_bar=false;\n"
        f"{today_ns_sql}\n"
        f"{counts_sql}\n"
    )
    print(f"wrote SQL to {sql_file}", file=sys.stderr)

    if args.dry_run:
        # Quick correctness check: execute and print counts.
        import subprocess
        subprocess.run(["duckdb", str(sql_file)], check=True)
        return 0
```

- [ ] **Step 3: Smoke run + assert known baseline**

Run: `python3 -m scripts.parking_delta snap-2026-05-28-12 --dry-run 2>/dev/null`

Expected output contains rows like:

```
team_internet_dyna_ns      ~44000
team_internet_dns_parking  ~38000
share_dns                  ~31000
spixiv                     ~29000
above_domains              ~14000
…
```

(Allow ±5% drift from spec §2 numbers; the 24h window for this dry-run is only
one snap = 12h, so expect roughly half the spec's 24h baseline.)

- [ ] **Step 4: Commit**

```bash
git add scripts/parking_delta.py
git commit -m "feat(parking): build today_ns CTE (UNION ALL per active provider)"
```

### Task 2.4: Load all seen-YYYY.parquet years and add LEFT ANTI JOIN

**Files:**
- Modify: `scripts/parking_delta.py`

- [ ] **Step 1: Add `download_seen_years` helper**

```python
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
```

- [ ] **Step 2: Add anti-join SQL builder**

```python
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
```

- [ ] **Step 3: Wire into main()** before SQL execution:

```python
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
```

- [ ] **Step 4: Smoke (still dry-run, seen empty)**

Run: `python3 -m scripts.parking_delta snap-2026-05-28-12 --dry-run 2>/dev/null`
Expected: counts unchanged from Task 2.3 (empty seen → today_new == today_ns).

- [ ] **Step 5: Commit**

```bash
git add scripts/parking_delta.py
git commit -m "feat(parking): load seen state and ANTI JOIN against today_ns"
```

### Task 2.5: LEFT JOIN CT signal (apex_of unnest)

**Files:**
- Modify: `scripts/parking_delta.py`

- [ ] **Step 1: Add CT signal SQL builder**

```python
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
```

- [ ] **Step 2: Add to SQL pipeline + update count query to also report CT hit count**

```python
    ct_sql = build_ct_signal_sql(args.snap_tag, args.shards_config, args.repo)
    counts_sql = """
SELECT provider,
       COUNT(*) AS new_today,
       COUNT(*) FILTER (WHERE len(ct_sources) > 0) AS with_ct_signal
FROM today_new_ct
GROUP BY provider
ORDER BY new_today DESC;
"""
    sql_file.write_text(
        "INSTALL httpfs; LOAD httpfs;\n"
        "SET memory_limit='6GB';\n"
        "SET enable_progress_bar=false;\n"
        f"{today_ns_sql}\n"
        f"{anti_join_sql}\n"
        f"{ct_sql}\n"
        f"{counts_sql}\n"
    )
```

- [ ] **Step 3: Smoke run**

Run: `python3 -m scripts.parking_delta snap-2026-05-28-12 --dry-run 2>/dev/null`
Expected: same per-provider counts, plus a `with_ct_signal` column. CT hit rate
should be a small fraction (most parked domains don't get CT certs).

- [ ] **Step 4: Commit**

```bash
git add scripts/parking_delta.py
git commit -m "feat(parking): LEFT JOIN CT signal (apex-level unnest)"
```

### Task 2.6: Per-provider COPY TO parquet + jsonl + manifest

**Files:**
- Modify: `scripts/parking_delta.py`

- [ ] **Step 1: Add export SQL builder**

```python
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
```

- [ ] **Step 2: Add manifest generation in main()**

```python
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
```

- [ ] **Step 3: Refactor main() to run SQL in two phases (counts first, then exports)**

Replace the dry-run branch with:

```python
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
    subprocess.run(["duckdb", str(sql_file)], check=True)

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
```

- [ ] **Step 4: Smoke dry-run**

Run: `python3 -m scripts.parking_delta snap-2026-05-28-12 --dry-run`

Expected: workdir listed in stderr. `ls $WORKDIR` should show 19 `.parquet` + 19 `.jsonl` + `manifest.json` + `counts.csv`. Inspect:

```bash
WD=$(python3 -m scripts.parking_delta snap-2026-05-28-12 --dry-run 2>&1 \
       | awk '/DRY RUN: outputs in/ {print $NF}')
ls "$WD"
cat "$WD/manifest.json" | head -20
duckdb -c "SELECT * FROM '$WD/delta.team_internet_dns_parking.parquet' LIMIT 5"
```

- [ ] **Step 5: Commit**

```bash
git add scripts/parking_delta.py
git commit -m "feat(parking): emit per-provider delta parquet/jsonl + manifest"
```

### Task 2.7: Upload deltas to parking-DAY-* release

**Files:**
- Modify: `scripts/parking_delta.py`

- [ ] **Step 1: Add upload branch after manifest**

Replace `if args.dry_run` block at end of main() with:

```python
    if args.dry_run:
        print(f"DRY RUN: outputs in {workdir}", file=sys.stderr)
        return 0

    from scripts.parking_common import gh_upload_assets
    delta_tag = f"{args.delta_release_prefix}{args.snap_tag.removeprefix('snap-')}"
    delta_files = sorted(str(p) for p in workdir.glob("delta.*.parquet"))
    delta_files += sorted(str(p) for p in workdir.glob("delta.*.jsonl"))
    delta_files.append(str(manifest))
    gh_upload_assets(
        delta_tag, delta_files, args.repo,
        title=f"Parking delta {args.snap_tag.removeprefix('snap-')}",
    )
    print(f"uploaded {len(delta_files)} assets to {delta_tag}",
          file=sys.stderr)
```

- [ ] **Step 2: Verify --help still works** (no smoke run, this writes to GH)

Run: `python3 -m scripts.parking_delta --help | head`
Expected: usage prints; no errors.

- [ ] **Step 3: Commit**

```bash
git add scripts/parking_delta.py
git commit -m "feat(parking): upload delta files to parking-DAY-* release"
```

### Task 2.8: Append to seen-YYYY.parquet (current year, atomic)

**Files:**
- Modify: `scripts/parking_delta.py`

- [ ] **Step 1: Add seen-write SQL**

Add to module:

```python
def build_seen_write_sql(year: str, snap_tag: str,
                        existing_seen: Path | None,
                        out_path: Path) -> str:
    """Merge today_new_ct into existing seen-YYYY.parquet (if any) and write
    the merged result to out_path."""
    base = (
        f"SELECT provider, domain, '{snap_tag}' AS first_snap, first_ms "
        f"FROM today_new_ct"
    )
    if existing_seen is not None:
        union = (
            f"SELECT provider, domain, first_snap, first_ms "
            f"FROM read_parquet('{existing_seen}')"
        )
        merged = f"({base})\nUNION ALL\n({union})"
    else:
        merged = f"({base})"
    return f"""
CREATE OR REPLACE TEMP TABLE merged AS
SELECT provider, domain,
       arg_min(first_snap, first_ms) AS first_snap,
       MIN(first_ms)                 AS first_ms
FROM (
{merged}
) GROUP BY provider, domain;

COPY (SELECT * FROM merged ORDER BY provider, domain)
TO '{out_path}' (FORMAT 'parquet', COMPRESSION 'zstd');
"""
```

- [ ] **Step 2: Wire after upload, before return**

Append to main():

```python
    # Append to seen-YYYY for the current year (snap date).
    year = date[:4]
    existing = workdir / "seen" / f"seen-{year}.parquet"
    existing = existing if existing.exists() else None
    out_seen = workdir / f"seen-{year}.parquet.new"
    seen_sql = build_seen_write_sql(year, args.snap_tag, existing, out_seen)
    seen_sql_file = workdir / "seen.sql"
    seen_sql_file.write_text(
        "INSTALL httpfs; LOAD httpfs;\n"
        "SET memory_limit='6GB';\n"
        # today_new_ct must be regenerated in this fresh duckdb invocation
        # since the previous one exited; for now, re-run the full SQL chain.
        + sql_file.read_text() + seen_sql
    )
    subprocess.run(["duckdb", str(seen_sql_file)], check=True)

    # Atomic upload: rename .new → seen-YYYY.parquet and upload with --clobber.
    final = workdir / f"seen-{year}.parquet"
    out_seen.replace(final)
    gh_upload_assets(args.state_release, [str(final)], args.repo,
                     title="Parking-NS seen state")
    print(f"uploaded seen-{year}.parquet to {args.state_release}",
          file=sys.stderr)
    return 0
```

- [ ] **Step 2.5: Note on naive re-execution**

The re-execution of `sql_file` to materialize `today_new_ct` in the seen-write step is wasteful. For v1 acceptable (each snap is < 10 min total runtime). v2 optimization: have parking_delta.py emit today_new_ct as a parquet, then load it in the seen-write SQL. Defer.

- [ ] **Step 3: Commit**

```bash
git add scripts/parking_delta.py
git commit -m "feat(parking): append today_new_ct to seen-YYYY.parquet and upload state"
```

---

## Phase 3: parking_daily.yml workflow

### Task 3.1: Workflow scaffold + concurrency + triggers

**Files:**
- Create: `.github/workflows/parking_daily.yml`

- [ ] **Step 1: Write the workflow**

```yaml
name: parking-daily

# Per-snap parking-NS delta producer. Reads the most-recent snap-YYYY-MM-DD-HH
# release after retry.yml has settled, filters by ns_suffix per
# .github/parking_providers.json, anti-joins against the cumulative seen state
# in `parking-state`, LEFT-JOINs CT signal, and uploads per-provider delta
# assets to `parking-DAY-YYYY-MM-DD-HH`.
#
# Triggers (belt-and-suspenders):
#   - cron: 05:00 / 17:00 UTC (30min after retry.yml)
#   - workflow_run: when retry.yml completes successfully
#   - workflow_dispatch with optional snap_tag and dry_run inputs
#
# Concurrency: serialized via `parking` group so cron+workflow_run never race
# on seen.parquet. The second to fire is idempotent (anti-join → empty delta).

on:
  schedule:
    - cron: '0 5,17 * * *'
  workflow_run:
    workflows: [retry-missing-shards]
    types: [completed]
  workflow_dispatch:
    inputs:
      snap_tag:
        description: 'snap-YYYY-MM-DD-HH (empty = nearest preceding 12h boundary)'
        required: false
        default: ''
      dry_run:
        description: 'If "true", skip state download and uploads; outputs as artifact'
        required: false
        default: 'false'
      force_incomplete:
        description: 'If "true", run even if snap-* release is missing shards'
        required: false
        default: 'false'

permissions:
  contents: write

concurrency:
  group: parking
  cancel-in-progress: false

jobs:
  delta:
    runs-on: ubuntu-latest
    timeout-minutes: 60
    steps:
      - uses: actions/checkout@v5

      - name: Compute snap_tag
        id: snap
        env:
          SNAP_INPUT: ${{ github.event.inputs.snap_tag }}
        run: |
          set -euo pipefail
          if [ -n "${SNAP_INPUT:-}" ]; then
            TAG="$SNAP_INPUT"
          else
            DAY=$(date -u +%F)
            NOW_H=$(date -u +%H)
            if [ "$((10#$NOW_H))" -lt 12 ]; then HOUR=00; else HOUR=12; fi
            TAG="snap-${DAY}-${HOUR}"
          fi
          echo "tag=$TAG" >> "$GITHUB_OUTPUT"
          echo "Resolved snap_tag=$TAG"

      - name: Setup Python + duckdb + jsonschema
        run: |
          set -euo pipefail
          sudo apt-get update -qq && sudo apt-get install -y -qq unzip
          # DuckDB CLI (pinned major.minor)
          curl -sS -L https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-amd64.zip -o /tmp/duckdb.zip
          unzip -q -o /tmp/duckdb.zip -d /usr/local/bin
          duckdb --version
          python3 -m pip install --quiet jsonschema pyarrow

      - name: Validate parking_providers.json against schema
        run: |
          python3 - <<'PY'
          import json, jsonschema, sys
          cfg    = json.load(open('.github/parking_providers.json'))
          schema = json.load(open('.github/parking_providers.schema.json'))
          jsonschema.validate(cfg, schema)
          print(f"OK: {len(cfg['providers'])} providers")
          PY

      - name: Run parking_delta.py
        env:
          GH_TOKEN: ${{ github.token }}
          GITHUB_RUN_URL: ${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}
          DRY_RUN:          ${{ github.event.inputs.dry_run || 'false' }}
          FORCE_INCOMPLETE: ${{ github.event.inputs.force_incomplete || 'false' }}
          SNAP_TAG:         ${{ steps.snap.outputs.tag }}
        run: |
          set -euo pipefail
          ARGS=("$SNAP_TAG" "--workdir" "$RUNNER_TEMP/parking-delta")
          [ "$DRY_RUN" = "true" ]          && ARGS+=("--dry-run")
          [ "$FORCE_INCOMPLETE" = "true" ] && ARGS+=("--force-incomplete")
          python3 -m scripts.parking_delta "${ARGS[@]}"

      - name: Upload artifact on dry-run
        if: ${{ github.event.inputs.dry_run == 'true' }}
        uses: actions/upload-artifact@v4
        with:
          name: parking-delta-${{ steps.snap.outputs.tag }}
          path: ${{ runner.temp }}/parking-delta/
          retention-days: 7
```

- [ ] **Step 2: Verify YAML syntax**

Run (locally):
```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/parking_daily.yml'))"
```
Expected: no output, exit 0.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/parking_daily.yml
git commit -m "feat(parking): add parking_daily.yml workflow (cron + workflow_run + dispatch)"
```

### Task 3.2: Smoke-test in dry-run dispatch

- [ ] **Step 1: Push branch and open PR**

```bash
git push -u origin <branch>
# Open PR via gh
gh pr create --fill --draft
```

- [ ] **Step 2: Dispatch workflow with dry_run=true against current snap**

```bash
gh workflow run parking_daily.yml \
  -f snap_tag=snap-2026-05-28-12 -f dry_run=true
gh run watch  # wait for completion
```

- [ ] **Step 3: Download artifact and verify**

```bash
RUN_ID=$(gh run list --workflow=parking_daily.yml --limit 1 --json databaseId -q '.[0].databaseId')
gh run download "$RUN_ID" -n "parking-delta-snap-2026-05-28-12" -D /tmp/pd-smoke
ls /tmp/pd-smoke
# Expected: 19 delta.*.parquet + 19 delta.*.jsonl + manifest.json + counts.csv
duckdb -c "SELECT * FROM '/tmp/pd-smoke/delta.team_internet_dns_parking.parquet' LIMIT 5"
cat /tmp/pd-smoke/manifest.json | python3 -m json.tool
```

- [ ] **Step 4: Sanity-check counts against spec §2 baseline**

Manifest's `providers[].new_domains` for `team_internet_dns_parking` should
be roughly half of the 38,888 in spec §2 (since spec measured across two
snaps and the smoke runs against one). Acceptable range: 15,000-25,000.

If counts are wildly off, debug before promoting. Common causes:
- ns_suffix typo (missing leading dot)
- `today_ns` GROUP BY skipping topics

- [ ] **Step 5: No commit needed for smoke** (artifact-only verification)

---

## Phase 4: parking_audit.py + parking_audit.yml

### Task 4.1: parking_audit.py — top-K NS over a window

**Files:**
- Create: `scripts/parking_audit.py`

- [ ] **Step 1: Write the script**

```python
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
    import csv
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
    subprocess.run(["duckdb", str(sql_file)], check=True)

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
```

- [ ] **Step 2: Smoke test dry-run with a short window**

Run: `python3 -m scripts.parking_audit --window-days 2 --topk 60 --dry-run`

Expected: completes in ~5 min, writes `topk_ns.parquet` + `report.md` to a
tempdir. Open report.md, verify the top section lists configured providers and
the "Unhandled" section is sparse (we already configured the top names).

- [ ] **Step 3: Commit**

```bash
git add scripts/parking_audit.py
git commit -m "feat(parking): parking_audit.py — top-K NS audit + report"
```

### Task 4.2: parking_audit.yml workflow

**Files:**
- Create: `.github/workflows/parking_audit.yml`

- [ ] **Step 1: Write workflow**

```yaml
name: parking-audit

# Monthly NS-concentration audit. Re-runs the data-driven top-K NS query
# against the last 30 days of snap-* releases and surfaces high-volume
# ns_apex values not in parking_providers.json. Uploads report + raw
# top-K to parking-audit-YYYY-MM and opens a GitHub Issue for triage.

on:
  schedule:
    - cron: '0 7 1 * *'   # 07:00 UTC on the 1st of each month
  workflow_dispatch:
    inputs:
      window_days:
        description: 'Audit window in days (default 30)'
        required: false
        default: '30'
      dry_run:
        description: 'If "true", skip upload + issue creation'
        required: false
        default: 'false'

permissions:
  contents: write
  issues: write

concurrency:
  group: parking-audit
  cancel-in-progress: false

jobs:
  audit:
    runs-on: ubuntu-latest
    timeout-minutes: 120
    steps:
      - uses: actions/checkout@v5

      - name: Setup duckdb + python deps
        run: |
          set -euo pipefail
          sudo apt-get update -qq && sudo apt-get install -y -qq unzip
          curl -sS -L https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-amd64.zip -o /tmp/duckdb.zip
          unzip -q -o /tmp/duckdb.zip -d /usr/local/bin
          python3 -m pip install --quiet pyarrow

      - name: Run parking_audit.py
        env:
          GH_TOKEN: ${{ github.token }}
          WINDOW_DAYS: ${{ github.event.inputs.window_days || '30' }}
          DRY_RUN:     ${{ github.event.inputs.dry_run     || 'false' }}
        run: |
          set -euo pipefail
          ARGS=("--window-days" "$WINDOW_DAYS"
                "--workdir" "$RUNNER_TEMP/parking-audit")
          [ "$DRY_RUN" = "true" ] && ARGS+=("--dry-run")
          python3 -m scripts.parking_audit "${ARGS[@]}"

      - name: Upload artifact on dry-run
        if: ${{ github.event.inputs.dry_run == 'true' }}
        uses: actions/upload-artifact@v4
        with:
          name: parking-audit
          path: ${{ runner.temp }}/parking-audit/
          retention-days: 30
```

- [ ] **Step 2: Verify YAML**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/parking_audit.yml'))"
```

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/parking_audit.yml
git commit -m "feat(parking): add parking_audit.yml monthly workflow"
```

---

## Phase 5: cleanup.yml extension

### Task 5.1: Extend cleanup regex to also clean parking-DAY-* releases

**Files:**
- Modify: `.github/workflows/cleanup.yml:60-63`

- [ ] **Step 1: Read the current awk regex**

Run: `sed -n '60,63p' .github/workflows/cleanup.yml`
Expected:
```awk
            /^snap-[0-9]{4}-[0-9]{2}-[0-9]{2}(-(00|12))?$/ {
              if (substr($0, 6, 10) < cutoff) print $0
            }' | sort)
```

- [ ] **Step 2: Replace the awk block**

Use the Edit tool with this old_string → new_string:

old_string:
```awk
          OLD=$(echo "$ALL" | awk -v cutoff="$CUTOFF" '
            /^snap-[0-9]{4}-[0-9]{2}-[0-9]{2}(-(00|12))?$/ {
              if (substr($0, 6, 10) < cutoff) print $0
            }' | sort)
```

new_string:
```awk
          # Match snap-YYYY-MM-DD-HH, legacy snap-YYYY-MM-DD, and
          # parking-DAY-YYYY-MM-DD-HH; the parking-state and parking-audit-*
          # releases stay (permanent / kept for trend analysis).
          OLD=$(echo "$ALL" | awk -v cutoff="$CUTOFF" '
            /^snap-[0-9]{4}-[0-9]{2}-[0-9]{2}(-(00|12))?$/ {
              if (substr($0, 6, 10) < cutoff) print $0; next
            }
            /^parking-DAY-[0-9]{4}-[0-9]{2}-[0-9]{2}-(00|12)$/ {
              if (substr($0, 14, 10) < cutoff) print $0
            }' | sort)
```

- [ ] **Step 3: Smoke locally with mock input**

```bash
ALL=$(printf '%s\n' \
  snap-2026-05-28-12 snap-2026-04-15-00 \
  parking-DAY-2026-05-28-12 parking-DAY-2026-04-15-00 \
  parking-state parking-audit-2026-04 \
  v1.0)
CUTOFF=2026-05-01
echo "$ALL" | awk -v cutoff="$CUTOFF" '
  /^snap-[0-9]{4}-[0-9]{2}-[0-9]{2}(-(00|12))?$/ {
    if (substr($0, 6, 10) < cutoff) print $0; next
  }
  /^parking-DAY-[0-9]{4}-[0-9]{2}-[0-9]{2}-(00|12)$/ {
    if (substr($0, 14, 10) < cutoff) print $0
  }'
```
Expected output (only two lines, in any order):
```
snap-2026-04-15-00
parking-DAY-2026-04-15-00
```

- [ ] **Step 4: Verify YAML syntax intact**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/cleanup.yml'))"
```

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/cleanup.yml
git commit -m "feat(parking): extend cleanup regex to prune parking-DAY-* on 30d window"
```

### Task 5.2: Cleanup dry-run dispatch (confirm no regression)

- [ ] **Step 1: Dispatch cleanup.yml with dry_run=true**

```bash
gh workflow run cleanup.yml -f dry_run=true
gh run watch
```

- [ ] **Step 2: Inspect step summary**

```bash
RUN_ID=$(gh run list --workflow=cleanup.yml --limit 1 --json databaseId -q '.[0].databaseId')
gh run view "$RUN_ID" --log | grep -A 30 "Release cleanup"
```

Expected: only `snap-*` tags older than 30 days listed (no `parking-state` or
`parking-audit-*`); since no `parking-DAY-*` exists yet, no parking lines.

- [ ] **Step 3: No commit needed** (dispatch-only verification)

---

## Phase 6: First production seed

### Task 6.1: Seed parking-state with current snap (no historical backfill)

- [ ] **Step 1: Manual dispatch parking_daily.yml against current snap**

```bash
gh workflow run parking_daily.yml -f snap_tag=snap-2026-05-29-00
gh run watch
```

- [ ] **Step 2: Verify state release was created**

```bash
gh release view parking-state -R wangmm001/dns-tracking
# Expected: contains seen-2026.parquet
```

- [ ] **Step 3: Verify delta release was created**

```bash
gh release view parking-DAY-2026-05-29-00 -R wangmm001/dns-tracking \
  --json assets -q '.assets[].name' | head -20
# Expected: 19 delta.*.parquet + 19 delta.*.jsonl + manifest.json
```

- [ ] **Step 4: Sanity-check the delta manifest**

```bash
gh release download parking-DAY-2026-05-29-00 -p manifest.json -D /tmp/
cat /tmp/manifest.json | python3 -m json.tool
```

Expected: per-provider new_domains counts roughly half spec §2 baseline
(single 12h snap vs 24h baseline).

- [ ] **Step 5: Idempotent re-run**

```bash
gh workflow run parking_daily.yml -f snap_tag=snap-2026-05-29-00
gh run watch
# Verify deltas are now mostly empty (anti-join against seen catches everything).
```

### Task 6.2: Enable cron + merge

- [ ] **Step 1: Mark PR ready for review** (`gh pr ready`)

- [ ] **Step 2: After review approval, merge**

- [ ] **Step 3: Monitor first cron-triggered run** (next 05:00 or 17:00 UTC)

- [ ] **Step 4: One week after first cron, dispatch monthly audit manually with short window to validate**

```bash
gh workflow run parking_audit.yml -f window_days=7 -f dry_run=true
```

---

## Open follow-ups (v2 candidates, NOT in v1)

These items appear in spec §8 but are out of v1 scope. Track as issues:

- AS-based parking detection (catch operators sharing parking IP pools without configured NS).
- Aftermarket NS coverage (afternic, dan.com, hugedomains, sav.com).
- FQDN-level CT JOIN (currently apex-only).
- `seeded_from` config field implementation (currently schema placeholder).
- Reduce parking_delta.py's double SQL execution in seen-write step (Task 2.8 step 2.5 note).
- `parking_audit.py --rebuild-seen --year YYYY --since snap-X` (spec §6.3): only useful once we've accumulated > 1 year of history that could be corrupted. v1 first-deployment seen state is fresh; recovery in v1 = delete seen-YYYY.parquet and accept spike on next run.

