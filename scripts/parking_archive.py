#!/usr/bin/env python3
# scripts/parking_archive.py
"""Submit newly-registered parkingcrew apex domains to Internet Archive's
Save Page Now (SPN) v2 API.

Pulls delta.{provider}.parquet from a parking-DAY-* release, filters to
rows where source_topics contains 'domains' (the real new apex from
newly_registered_domains_measurements, NOT the CT-discovered FQDNs which
can be 3k-42k per snap and would blow through SPN quota), then submits
each apex's HTTP root to SPN with rate limiting + 429 backoff. Results
land back on the same release as archive.{provider}.jsonl + .summary.json.

Reads SPN credentials from env: SPN_ACCESS_KEY + SPN_SECRET_KEY.
See https://docs.google.com/document/d/1Nsv52MvSjbLb2PCpHlat0gkzw0EvtSgpKHu4mk0MnrA/ for the SPN v2 spec.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from scripts.parking_common import REPO_DEFAULT, gh_upload_assets, parse_snap_tag

SPN_SAVE_URL = "https://web.archive.org/save"

# Authenticated quota is documented as ~12-15 captures/min; 4s spacing
# gives 15/min steady-state. We back off harder when SPN sends 429.
DEFAULT_RATE_S = 4.0


def build_target_url(domain: str) -> str:
    """Return the canonical target URL for SPN to capture.

    Always http://<domain>/ — SPN will follow redirects (HTTPS upgrade,
    landing-page hostname) on its own, and parked domains often serve
    plain HTTP first.
    """
    return f"http://{domain}/"


def build_spn_request(domain: str, access_key: str, secret_key: str) -> urllib.request.Request:
    """Construct a single SPN v2 POST request for `domain`."""
    body = urllib.parse.urlencode({
        "url":                   build_target_url(domain),
        "capture_outlinks":      "0",
        "capture_all":           "1",
        "delay_wb_availability": "0",
        "skip_first_archive":    "0",
    }).encode()
    return urllib.request.Request(
        SPN_SAVE_URL,
        data=body,
        headers={
            "Authorization": f"LOW {access_key}:{secret_key}",
            "Accept":        "application/json",
            "User-Agent":    "dns-tracking/parking_archive (+https://github.com/wangmm001/dns-tracking)",
        },
        method="POST",
    )


def submit_one(domain: str, access_key: str, secret_key: str,
               timeout_s: float = 30.0) -> dict:
    """Submit one domain. Returns a dict suitable for jsonl output.

    status ∈ {"queued", "no_job_id", "error"}. Records SPN's actual
    response fields (job_id / url / message) when present so a missing
    job_id can be debugged later without re-querying SPN.
    """
    target_url = build_target_url(domain)
    record = {"domain": domain, "url": target_url}
    try:
        spn_req = build_spn_request(domain, access_key, secret_key)
        with urllib.request.urlopen(spn_req, timeout=timeout_s) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                record.update({
                    "status": "error",
                    "error":  "non_json_response",
                    "http":   resp.status,
                    "body":   raw[:300],
                })
                return record
            job_id = payload.get("job_id")
            record.update({
                "http":           resp.status,
                "job_id":         job_id,
                "spn_url":        payload.get("url"),
                "spn_message":    payload.get("message"),
                "spn_status_ext": payload.get("status_ext"),
            })
            record["status"] = "queued" if job_id else "no_job_id"
            return record
    except urllib.error.HTTPError as e:
        try:
            err_body = e.read().decode("utf-8", errors="replace")[:300]
        except Exception:
            err_body = ""
        record.update({
            "status": "error",
            "error":  f"http_{e.code}",
            "http":   e.code,
            "body":   err_body,
        })
        return record
    except urllib.error.URLError as e:
        # URLError wraps the underlying transport failure in .reason.
        # Surface that so URLError isn't an opaque label.
        reason = getattr(e, "reason", e)
        record.update({
            "status": "error",
            "error":  f"urlerror:{type(reason).__name__}",
            "reason": str(reason)[:200],
        })
        return record
    except (TimeoutError, OSError) as e:
        record.update({
            "status": "error",
            "error":  f"network:{type(e).__name__}",
            "reason": str(e)[:200],
        })
        return record


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("snap_tag", help="snap-YYYY-MM-DD-HH")
    p.add_argument("--repo",          default=REPO_DEFAULT)
    p.add_argument("--provider",      default="parkingcrew",
                   help="Provider name (delta filename uses this)")
    p.add_argument("--delta-release-prefix", default="parking-DAY-")
    p.add_argument("--rate-s",        type=float, default=DEFAULT_RATE_S,
                   help="Seconds between submissions (default 4.0 = ~15/min)")
    p.add_argument("--workdir",       default=None)
    p.add_argument("--dry-run",       action="store_true",
                   help="Read delta, print apex count + sample; do not submit")
    p.add_argument("--limit",         type=int, default=0,
                   help="Cap submissions for testing (0 = no cap)")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    parse_snap_tag(args.snap_tag)  # validate format
    workdir = Path(args.workdir or tempfile.mkdtemp(prefix="parking-archive-"))
    workdir.mkdir(parents=True, exist_ok=True)
    print(f"snap_tag={args.snap_tag} provider={args.provider} "
          f"workdir={workdir} dry_run={args.dry_run}", file=sys.stderr)

    delta_tag = f"{args.delta_release_prefix}{args.snap_tag.removeprefix('snap-')}"
    delta_asset = f"delta.{args.provider}.parquet"

    # Download delta parquet for this provider.
    subprocess.run(
        ["gh", "release", "download", delta_tag, "-R", args.repo,
         "-p", delta_asset, "-D", str(workdir), "--clobber"],
        check=True,
    )
    delta_path = workdir / delta_asset

    # Filter to "real new apex": source_topics array contains 'domains'.
    # This excludes CT-discovered FQDNs (which can be 3k-42k/snap and
    # would blow through SPN quota); keeps only apex from
    # newly_registered_domains_measurements (~100-200/snap for parkingcrew).
    proc = subprocess.run(
        ["duckdb", "-csv", "-noheader", "-c",
         f"SELECT domain FROM read_parquet('{delta_path}') "
         f"WHERE list_contains(source_topics, 'domains') ORDER BY domain"],
        capture_output=True, text=True, check=True,
    )
    domains = [ln.strip() for ln in proc.stdout.splitlines() if ln.strip()]
    print(f"real-new-apex count: {len(domains)}", file=sys.stderr)

    if args.limit > 0:
        domains = domains[:args.limit]
        print(f"--limit={args.limit} applied, will submit {len(domains)}",
              file=sys.stderr)

    if args.dry_run:
        print(f"DRY RUN: first 10 = {domains[:10]}", file=sys.stderr)
        print(f"DRY RUN: outputs in {workdir}", file=sys.stderr)
        return 0

    access_key = os.environ.get("SPN_ACCESS_KEY")
    secret_key = os.environ.get("SPN_SECRET_KEY")
    if not access_key or not secret_key:
        print("ERROR: SPN_ACCESS_KEY and SPN_SECRET_KEY required",
              file=sys.stderr)
        return 1

    results_file = workdir / f"archive.{args.provider}.jsonl"
    counts = {"queued": 0, "no_job_id": 0, "error": 0}
    rate_429 = 0

    t0 = time.monotonic()
    with open(results_file, "w") as out:
        for i, d in enumerate(domains):
            if i > 0:
                time.sleep(args.rate_s)
            rec = submit_one(d, access_key, secret_key)
            out.write(json.dumps(rec) + "\n")
            counts[rec["status"]] = counts.get(rec["status"], 0) + 1

            # SPN signals quota exhaustion via 429; back off ~30s before continuing.
            if rec.get("error", "").startswith("http_429"):
                rate_429 += 1
                print(f"  [{i+1}/{len(domains)}] 429 hit; sleep 30s", file=sys.stderr)
                time.sleep(30)

            if (i + 1) % 25 == 0 or (i + 1) == len(domains):
                print(f"  [{i+1}/{len(domains)}] queued={counts.get('queued',0)} "
                      f"no_job_id={counts.get('no_job_id',0)} "
                      f"error={counts.get('error',0)} 429={rate_429}",
                      file=sys.stderr)

    runtime = time.monotonic() - t0
    summary_file = workdir / f"archive.{args.provider}.summary.json"
    summary = {
        "snap_tag":        args.snap_tag,
        "provider":        args.provider,
        "submitted":       len(domains),
        "queued":          counts.get("queued", 0),
        "no_job_id":       counts.get("no_job_id", 0),
        "error":           counts.get("error", 0),
        "rate_429_hits":   rate_429,
        "runtime_s":       round(runtime, 1),
        "rate_s":          args.rate_s,
        "run_id":          os.environ.get("GITHUB_RUN_URL", ""),
    }
    summary_file.write_text(json.dumps(summary, indent=2))
    print(f"summary: {json.dumps(summary)}", file=sys.stderr)

    gh_upload_assets(
        delta_tag,
        [str(results_file), str(summary_file)],
        args.repo,
        title=f"Parking delta {args.snap_tag.removeprefix('snap-')}",
    )
    print(f"uploaded {results_file.name} + {summary_file.name} to {delta_tag}",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
