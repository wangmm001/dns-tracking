# Daily parking-NS delta tracking

Status: draft (2026-05-29)
Owner: TBD
Implementation target: dns-tracking repo

## 1. Goal

For every 12-hour `snap-YYYY-MM-DD-HH` release, emit a per-provider list of
**newly-observed** apex/FQDN domains whose authoritative NS belongs to a known
domain-parking operator (parkingcrew.net, dns-parking.com, bodis.com, …).

"Newly-observed" is defined per `(domain, provider)` pair against a permanent
`seen` state stored in a dedicated GitHub Release; a domain that migrates from
parkingcrew → bodis is emitted once under each provider (parking-migration is
itself a useful signal).

In v1 the same delivery includes a **monthly audit job** that re-runs a
top-K NS-concentration query on raw snapshots and flags high-volume `ns_apex`
values not in the provider config. This keeps the list from rotting as
operators rebrand/migrate (which has clearly already happened, see §2).

Out of scope (v1):
- Aftermarket / for-sale NS (afternic, dan.com, hugedomains, sav.com, …).
  These use a similar NS pattern but are not PPC parking; revisit in v2 if
  needed.
- IP / AS-based parking detection. NS-based covers ~all relevant operators;
  AS-based would be a v2 complementary signal.
- ccTLD coverage. Upstream OpenINTEL Zonestream does not cover most ccTLDs;
  this is a permanent dataset limitation, not a v1 gap.

## 2. Provider config (evidence-driven seed list)

Seed list was validated 2026-05-29 against the 24h window `snap-2026-05-28-00`
+ `snap-2026-05-28-12` on `newly_registered_domains_measurements`. Counts
below are new apex domains observed in that 24h.

Categories:
- **A** Well-known PPC parking operators (high confidence).
- **B** High-concentration NS clusters with parking-shaped samples but
  less-known operators (medium confidence; promoted on sample inspection).
- **W** Watchlist (`active: false`): suspected non-PPC or unverified.

| name (config) | ns_suffix | org | 24h | tier |
|---|---|---|---:|---|
| `team_internet_dns_parking` | `.dns-parking.com` | Team Internet | 38,888 | A |
| `team_internet_dyna_ns` | `.dyna-ns.net` | Team Internet (inferred — shares AS206834 with parkingcrew.net) | 44,068 | B |
| `team_internet_parkingcrew` | `.parkingcrew.net` | Team Internet | 203 | A |
| `above_domains` | `.abovedomains.com` | Above.com (Trellian) — AS133618 | 13,990 | A |
| `above_legacy` | `.above.com` | Above.com (Trellian, legacy NS — migrated to abovedomains.com) | 4 | A |
| `above_redirect` | `.dns-redirect.com` | Above.com (Trellian, inferred — redirector pattern) | 8,197 | B |
| `parklogic` | `.parklogic.com` | ParkLogic (Above ecosystem) | 1,081 | A |
| `sedo` | `.sedoparking.com` | Sedo — AS47846 | 797 | A |
| `bodis` | `.bodis.com` | Bodis | 8 | A |
| `cashparking` | `.cashparking.com` | GoDaddy CashParking | 69 | A |
| `internettraffic` | `.internettraffic.com` | Internet Traffic | 4 | A |
| `share_dns` | `.share-dns.net`, `.share-dns.com` | Share-DNS (CN) | 31,226 | B |
| `spixiv` | `.spixiv.com` | Spixiv (CN) | 29,003 | B |
| `julydns` | `.julydns.com` | JulyDNS (CN) | 10,959 | B |
| `xundns` | `.xundns.com` | XunDNS (CN) | 7,571 | B |
| `taoa` | `.taoa.com` | Taoa (CN) | 5,160 | B |
| `mismes` | `.mismes.com` | Mismes (CN) | 4,803 | B |
| `jindun9` | `.jindun9.com` | Jindun9 (CN) | 3,140 | B |
| `onclouddns` | `.onclouddns.com` | OnCloudDNS (CN) | 2,984 | B |
| `dnsowl` | `.dnsowl.com` | Unknown (Sedo AS-adjacent) | 7,044 | W |

Removed from earlier draft (zero traffic in 2026-05-28 window):
`voodoo.com`, `smartname.com`.

Rule for splitting vs combining `ns_suffix` entries:

- **Split** when the apexes are *alternatives over time* (one operator
  migrated from A → B; some legacy domains still on A). Examples here:
  Team Internet (`parkingcrew.net` + `dns-parking.com` + `dyna-ns.net`),
  Above.com (`above.com` + `abovedomains.com`). Splitting makes the
  migration itself observable (a domain re-emitted under the new entry
  is a signal); combined would silently fold migrated domains back into
  `seen` and lose them.
- **Combine** when the apexes are *concurrent* — one logical NS pair
  whose halves happen to live on different TLDs for redundancy.
  Example: `share-dns.com` (ns_a) + `share-dns.net` (ns_b) serving the
  same domain population (identical 31,226 counts, identical samples).
  Combining gives one row per domain; splitting would double-count.

Downstream analyses can aggregate split entries by `org`.

`ns_suffix` matching is intentionally loose — `ends_with(s, '.dns-parking.com')`
instead of `s IN ('ns1.dns-parking.com', 'ns2.dns-parking.com')` — because
multiple operators use per-customer subdomains under the NS apex (Above.com
uses `230.ns1.above.com`, ParkLogic uses `ns1.645.parklogic.com`, etc.) and
new NS hosts (ns3/ns4/geo/aurora/…) appear without warning.

## 3. Architecture

```
.github/parking_providers.json          ← provider config (v1: ~18 active + 1 watchlist)
.github/workflows/parking_daily.yml     ← per-snap delta (cron 05:00/17:00 UTC + workflow_run)
.github/workflows/parking_audit.yml     ← monthly NS concentration audit
.github/workflows/cleanup.yml           ← MODIFIED: extend regex to ^(snap|parking)-…
scripts/parking_delta.py                ← DuckDB SQL driver for one snap
scripts/parking_audit.py                ← Top-K NS, diffs against config, opens issue

Releases (runtime artifacts):
  parking-state                         single asset seen-YYYY.parquet per year, permanent
  parking-DAY-YYYY-MM-DD-HH             per-snap delta release, one asset per provider
  parking-audit-YYYY-MM                 monthly audit output: topk_ns.parquet + report.md
```

### 3.1 Trigger chain

Slots in around the existing 4-workflow daily loop:

```
00:30 UTC  consume.yml          → snap-DAY-00
04:30 UTC  retry.yml            → fill snap-DAY-00 gaps
05:00 UTC  parking_daily        ← cron, processes snap-DAY-00          [NEW]
           parking_daily        ← also workflow_run(retry.yml completed) [NEW]
06:00 UTC  cleanup.yml          → ^(snap|parking)-… 30-day rolling     [MODIFIED]
12:30 UTC  consume.yml          → snap-DAY-12
16:30 UTC  retry.yml            → fill snap-DAY-12 gaps
17:00 UTC  parking_daily        ← cron, processes snap-DAY-12          [NEW]
           parking_daily        ← workflow_run(retry.yml)               [NEW]

Monthly (1st of month, 07:00 UTC):
           parking_audit                                                [NEW]
```

Both cron and workflow_run triggers are wired (belt-and-suspenders); the
`concurrency: { group: parking, cancel-in-progress: false }` clause makes
the two queue rather than race on `seen-YYYY.parquet`. The second to run is
idempotent: its anti-join finds the first's outputs already in `seen`, the
delta is empty, the upload is a no-op `--clobber`.

### 3.2 Worker boundaries

- `parking_delta.py` and `parking_audit.py` each run in a single GitHub
  Actions job (no shard fan-out) — the per-snap parking subset is small
  (~100k rows after `ns_suffix` filter on 220M total), DuckDB on `ubuntu-latest`
  is well within memory budget.
- All heavy lifting is in DuckDB SQL; the Python wrapper only assembles the
  parquet URL list from `gh release view`, reads/writes config & state, and
  uploads results.

## 4. Data flow — per-snap delta

```
1. Resolve snap_tag (input or computed via consume.yml alignment logic).
2. Verify retry.yml finished: count asset files in snap-* release vs
   .github/shards.json expectations. If short, sleep 5min and re-check;
   after 2 misses, fail (next cron will pick it up).
3. Download .github/parking_providers.json (already in workdir) and ALL
   seen-YYYY.parquet assets from parking-state (one per year that exists).
   Read = union across all years (anti-join must see history); write = only
   the current year's file (new "first sightings" can only be in the current
   year). Bootstrap empty if first run ever.
4. Run scripts/parking_delta.py snap_tag — DuckDB query:
   a. Construct read_parquet list of all 3 Avro topic shards from snap-* URL.
   b. WITH today_ns: UNION ALL per active provider, filtering
      k='ns' AND ends_with(s, suffix) per ns_suffix entry.
   c. GROUP BY (provider, d): aggregate ns_set, source_topics, first_ms,
      last_ms, observations.
   d. LEFT JOIN today_ns AGAINST seen ON (provider, domain) → today_new.
   e. LEFT JOIN today_new AGAINST certstream_domains via
      unnest(domain_list) AS apex (matching at apex level only — see §6.1).
   f. Aggregate ct_sources / ct_fingerprints into list columns.
5. Split per provider → delta.{provider}.parquet (always) + delta.{provider}.jsonl
   (same data, human-readable).
6. Upload all delta files to parking-DAY-YYYY-MM-DD-HH release (gh release
   upload --clobber).
7. Append today_new to seen-YYYY.parquet:
   - Read existing seen-YYYY.parquet (or empty schema).
   - UNION today_new (project to seen schema), then DISTINCT ON (provider, domain)
     keeping minimum first_snap / first_ms.
   - Write back atomically: write to seen-YYYY.parquet.tmp, then upload --clobber.
8. Write manifest.json with: snap_tag, run_id, per-provider new_count, runtime.

Schema sketch (DuckDB SQL):

  CREATE OR REPLACE TEMP TABLE today_ns AS
    WITH src AS (SELECT * FROM read_parquet([... 3 topics × all shards ...])),
         provider_match AS (
           SELECT 'team_internet_dns_parking' AS provider, topic, d, s, fs, ls, n
           FROM src
           WHERE k='ns' AND ends_with(s, '.dns-parking.com')
           UNION ALL ... -- one UNION ALL block per active provider/suffix
         )
    SELECT provider,
           d AS domain,
           list_sort(array_agg(DISTINCT s))     AS ns_set,
           list_sort(array_agg(DISTINCT topic)) AS source_topics,
           MIN(fs) AS first_ms,
           MAX(ls) AS last_ms,
           SUM(n)  AS observations
    FROM provider_match
    GROUP BY provider, d;
```

## 5. Output schemas

### 5.1 `seen-YYYY.parquet` (cumulative state, one per calendar year)

| col | type | meaning |
|---|---|---|
| `domain` | string | apex (or FQDN, depending on which topic surfaced it first) |
| `provider` | string | foreign key to `providers[].name` in config |
| `first_snap` | string | snap_tag where this (domain, provider) was first emitted |
| `first_ms` | int64 | earliest broker_ts (ms epoch) at first emission |

Sharded by calendar year (`seen-2026.parquet`, `seen-2027.parquet`, …) so a
single file stays well under GitHub's 2 GB asset cap. Estimate: 18 providers
× ~200k new daily rows × 365 ≈ 73M rows/year ≈ ~200 MB compressed.

Query pattern for cross-year reads: glob via httpfs/local download,
DuckDB `read_parquet(['seen-2026.parquet','seen-2027.parquet'])`.

### 5.2 `delta.{provider}.parquet` (per snap, one per provider)

| col | type | meaning |
|---|---|---|
| `domain` | string | apex / FQDN |
| `provider` | string | redundant with filename, kept for safety |
| `ns_set` | list&lt;string&gt; | actual NS hostnames seen (e.g. `[ns1.dns-parking.com, ns2.dns-parking.com]`) |
| `source_topics` | list&lt;string&gt; | subset of `[domains, fqdn, certs]` |
| `first_ms` | int64 | earliest broker_ts in this snap |
| `last_ms` | int64 | latest broker_ts in this snap |
| `observations` | int64 | sum of `n` (count of raw measurements rolled up) |
| `ct_sources` | list&lt;string&gt; nullable | CT log names from JOIN with `certstream_domains` |
| `ct_fingerprints` | list&lt;string&gt; nullable | CT cert SHA1 hexes |

Empty `delta.{provider}.parquet` (provider had no new domains this snap) is
still uploaded — keeps the asset count per release predictable, simplifies
downstream globbing.

### 5.3 Per-snap `manifest.json`

```json
{
  "snap_tag": "snap-2026-05-28-12",
  "run_id": "https://github.com/wangmm001/dns-tracking/actions/runs/...",
  "config_version": 1,
  "runtime_s": 412,
  "providers": [
    {"name": "team_internet_dns_parking", "new_domains": 4731, "ct_signal_count": 891},
    {"name": "above_domains",             "new_domains": 1827, "ct_signal_count": 312},
    ...
  ]
}
```

### 5.4 Monthly audit output

`parking-audit-YYYY-MM/report.md`:
- Top 50 `ns_apex` by new-domain count across the month.
- Diff vs `parking_providers.json` config: which apexes are unhandled, with
  sample domains, IP/ASN concentration, paired-NS detection.
- Inactive-provider report: any `active: false` entry that surged?

`parking-audit-YYYY-MM/topk_ns.parquet`:
- The raw top-K table, columns: `ns_apex, new_domains, distinct_ns_hosts,
  sample_ns_hosts, sample_domains, top_asn`, sorted by `new_domains` desc,
  first 500 rows.

The audit job opens a GitHub Issue tagged `parking-audit` containing the
report markdown, with one comment per unhandled high-volume apex so each can
be triaged independently (close = ignored / migrate to PR adding it to config).

## 6. Edge cases & known limitations

### 6.1 CT signal match rate

`certstream_domains.domain_list` includes both SAN entries (often FQDNs like
`www.example.com`, `*.example.com`) and CN. v1 unnests `domain_list` and
matches at apex level only — i.e. extract the apex from each CT domain
(`*.X.example.com` → `example.com`), JOIN against our domain. This catches
all certs where the apex parked domain appears anywhere in the cert, at the
cost of slightly inflating the CT-signal column for shared-cert situations
(`example.com` parked but cert covers `foo.example.com, bar.example.net`).
Acceptable for v1: the column is descriptive, not authoritative.

Implementation:

  WITH ct_unnest AS (
    SELECT certIndex, source, fingerprint,
           apex_of(unnest(domain_list)) AS apex
    FROM read_parquet([... certstream_domains.shard-*.parquet ...])
  )
  -- LEFT JOIN today_new td ON ct_unnest.apex = td.domain

`apex_of()` is a small UDF / SQL expression that strips to the eTLD+1. For
v1 use a simple "last two labels" heuristic (matches `.com / .net / .io`
correctly; misses `.co.uk` but that's already absent from upstream data).

### 6.2 New-provider backfill spike

When a provider is added to config, its first run emits all currently-active
domains under that NS — domains that have been there for weeks but were
never in `seen`. Expected and documented; treat the first delta as a
"backfill batch" and not as an alert-worthy spike.

To avoid this in special cases (e.g. accidentally re-adding a removed
provider), config supports an optional `seeded_from: snap-YYYY-MM-DD-HH`
field that pre-populates `seen` from a historical scan before the first
real run. Not implemented in v1; placeholder in the config schema.

### 6.3 State corruption / rebuild

`seen-YYYY.parquet` is the only mutable state. Loss of one year's file means
that year's "first appearance" timestamps reset; new deltas would re-emit
old domains.

Recovery: `parking_audit.py --rebuild-seen --year YYYY --since snap-X`
scans all snap-* releases still in the 30-day window plus any backfilled
deltas in `parking-DAY-*` releases to reconstruct `seen-YYYY.parquet`. Beyond
30 days the data is gone (cleanup.yml drops snap-* after 30d); this is
accepted — older entries simply aren't restorable.

### 6.4 active=false providers

Daily workflow skips them entirely. Audit workflow still checks their NS
suffixes for traffic — a previously-dormant provider re-surging is itself a
signal (e.g. if `voodoo.com` suddenly hits 1k new domains, audit reports it
even though daily wouldn't have caught it).

### 6.5 Shard incompleteness

`parking_daily.yml` waits for `retry.yml`'s outcome before running. Workflow
explicitly counts assets in the snap-* release against `.github/shards.json`
expectations; if short, the workflow exits non-zero so the next 12h cron
picks it up (or manual dispatch with `--force-incomplete` for analysis on
known-incomplete snaps).

### 6.6 Per-day output expectations

A "day" in our output is per-12h snap, not per calendar day. Downstream
consumers wanting per-calendar-day rollups do a one-line `UNION` of two
adjacent `delta.{provider}.parquet` files. This matches the upstream cadence
without inventing a second derivative.

## 7. Testing strategy

### 7.1 Local dry-run

  python scripts/parking_delta.py --snap snap-2026-05-28-12 --dry-run

Runs the full SQL against remote httpfs URLs, prints per-provider counts,
writes outputs to `/tmp/parking-delta/`. Skips `seen` state I/O and skips
release upload. Lets developers verify a config change against real data in
under 5 minutes.

### 7.2 Config schema validation

`parking_providers.json` has a `$schema` reference; daily workflow validates
via `jq` + a JSON Schema spec in `scripts/parking_providers.schema.json`.
Validation runs as the first step — bad config fails before any data work.

### 7.3 Workflow smoke

`workflow_dispatch.inputs`:
- `snap_tag` (optional override)
- `dry_run` (bool, default false) — runs everything except the two upload
  steps, emits outputs as GH Actions artifacts.

Recommended deployment sequence:
1. Land config + scripts on a branch.
2. Manual dispatch with `dry_run=true` against 2 recent snaps; check
   per-provider counts match this design doc's baseline (§2).
3. Manual dispatch with `dry_run=false` against current snap to seed
   `parking-state/seen-2026.parquet`.
4. Enable cron schedule.

### 7.4 Audit job manual seeding

`parking_audit.py --window 7d` runs the audit logic against the last 7 days
to verify report quality before locking in the monthly schedule.

## 8. v1 / v2 boundary

In v1:
- 18-entry provider config + 1 watchlist (§2).
- `parking_daily.yml` per-snap delta with seen state + CT signal.
- `parking_audit.yml` monthly NS concentration audit + GH issue.
- cleanup.yml regex extension.
- Local dry-run + dispatch dry_run.

Not in v1 (potential v2 work):
- AS-based parking detection (catches operators using shared parking IP
  pools even when NS isn't on our list). Requires building a parking-IP
  classifier from the daily output (positive examples).
- Aftermarket / for-sale NS coverage (afternic, dan, hugedomains, sav).
- Richer CT JOIN (FQDN-level, not just apex).
- `seeded_from` config field (backfill spike suppression).
- Per-day calendar-day rollup as a derived asset (currently downstream's job).
- Public API / web UI for browsing daily lists.

## 9. Open questions

None at design freeze; all clarifications resolved in brainstorming session
2026-05-29. Re-raise here if found during implementation.
