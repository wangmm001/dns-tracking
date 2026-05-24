# dns-tracking

English README (this file) · [中文版](README.zh.md)

Long-term tracking of newly registered domains' DNS state (IP, NS, MX, AS,
country) sourced from [OpenINTEL Zonestream's][1] reactive measurement Avro
streams ([DarkDNS][2], IMC '24).

[1]: https://openintel.nl/data/zonestream/
[2]: https://arxiv.org/abs/2405.12010

## What this is

OpenINTEL publishes several public Kafka topics on
`kafka.zonestream.openintel.nl:9092` (anonymous, PLAINTEXT). We consume four
of them:

- `newly_registered_domains_measurements`   — ~221M msgs/day, Avro
- `newly_registered_fqdn_measurements`      — ~107M msgs/day, Avro
- `newly_issued_certificates_measurements`  —  ~82M msgs/day, Avro
- `certstream_domains`                      —  CT log → domain mapping, JSON

The three Avro topics carry raw per-query DNS measurement results.
`certstream_domains` carries the CT-log-entry → domain-list mapping that
ties cert events back to domains. All four feed into a single observation
stream we maintain here.

Sister project `zonestream-archive` archives the small upstream JSON topics
(`newly_registered_domain` etc); this repo handles the high-volume measurement
streams differently: stream-consume them on GitHub Actions (multi-Gbps
network) and aggregate per-record observations into compact Parquet files.

## What gets stored

One **Release per 12h window**, tagged `snap-YYYY-MM-DD-HH` where `HH ∈ {00, 12}`
marks the window end. Each shard of each topic uploads a single Parquet asset:

```
snap-2026-05-25-00/
  newly_registered_domains_measurements.shard-0.parquet  (~80 MB, dedup'd)
  newly_registered_domains_measurements.shard-1.parquet
  …
  newly_registered_fqdn_measurements.shard-0.parquet     (~30 MB)
  …
  newly_issued_certificates_measurements.shard-0.parquet (~200 MB)
  …
  certstream_domains.shard-0.parquet                     (~1-2 MB)
  certstream_domains.shard-1.parquet
```

All Parquet files use **zstd-3** compression and Parquet's default dictionary
encoding for string columns. Total per 12h window: roughly 1 GB across 20
shard files.

### Schema 1: Avro DNS topics — `consume_group.py` output

The three `*_measurements` topics share a single 16-column observation
schema. Each row is one unique `(kind, key)` tuple observed during the
window, with `first_ts` / `last_ts` / `n` aggregates rolling up the raw
~10-300 repeated DNS queries DarkDNS issues per (domain, IP) pair.

| col | type | meaning |
|---|---|---|
| `k` | string | record kind: `ip` / `ns` / `ns_ip` / `mx` (filter first for correctness) |
| `topic` | string | `domains` / `fqdn` / `certs` (short tag) |
| `d` | string | candidate domain (NULL when `k='ns_ip'`) |
| `s` | string | NS hostname (`k='ns'` or `'ns_ip'`) |
| `m` | string | MX hostname (`k='mx'`) |
| `q` | string | DNS qtype `A` or `AAAA` (`k='ip'` or `'ns_ip'`) |
| `ip` | string | IPv4/v6 address (`k='ip'` or `'ns_ip'`) |
| `a` | string | ASN as decimal string (OpenINTEL's native form) |
| `c` | string | ISO 3166-1 alpha-2 country code |
| `p` | string | IP prefix (CIDR) |
| `pr` | int64 | MX preference (`k='mx'`) |
| `tl` | int64 | TTL (response_ttl, seconds) |
| `fs` | int64 | first_ts (ms since epoch; earliest broker_ts within window) |
| `ls` | int64 | last_ts  (ms since epoch; latest) |
| `n`  | int64 | count of raw observations represented by this row |
| `rc` | int64 | DNS rcode (only set when non-zero) |

NULLs distinguish kind subsets — e.g., a `k='ns'` row has `s` set but `ip`/`q`/`a`/`c`/`p` NULL. Always filter by `k` first when querying.

### Schema 2: `certstream_domains` — `consume_json.py` output

CT log entry → domain list mapping. One row per CT log entry.

| col | type | meaning |
|---|---|---|
| `certIndex` | int64 | per-CT-log entry index |
| `seen` | int64 | unix seconds when the CT entry was observed |
| `submission_timestamp` | int64 | unix seconds when cert was submitted to the CT log |
| `source` | string | CT log name (e.g. `Sectigo 'Tiger2026h2'`) |
| `updateType` | string | `X509LogEntry` or `PrecertLogEntry` |
| `fingerprint` | string | SHA1 hex `AA:BB:...` (unique per cert) |
| `domain_list` | list&lt;string&gt; | all domains in the cert (CN + SANs) |
| `sld_list` | list&lt;string&gt; | deduped second-level domains derived from `domain_list` |

Unknown fields (schema drift) are logged once with a `WARN` and dropped; the
shard's stats include `unknown_fields_seen` so we notice and update the
schema in `scripts/consume_json.py`.

### Cross-schema join key

The 4 topics tie together through:
- **`fingerprint`** — present in certstream_domains and in the Avro
  measurement records (as part of the original `MeasurementResult` `id`).
- **`certIndex`** — present in certstream_domains; also in the Avro record's
  `cert_index` field (when the measurement was CT-triggered).
- **`domain`** — `d` column in the Avro observations, member of `domain_list`
  / `sld_list` in certstream_domains.

The Avro `*_measurements` extraction here keeps the candidate domain
(`d` column) but not the per-record `cert_index`. To join back to specific
CT entries, look up by `domain` against certstream_domains.

## How it works

`consume.yml` runs twice daily at **00:30 and 12:30 UTC** (and on manual
dispatch). Each run consumes a 12-hour window ending at the nearest
preceding 12h UTC boundary, so coverage stays well within the broker's
~24h retention. Matrix strategy with per-topic shard counts sized so all
topics finish in roughly the same wall-clock budget:

| topic | format | shards | per-shard output |
|---|---|---:|---:|
| `newly_issued_certificates_measurements` | `avro_dns` | 3 | ~200 MB |
| `newly_registered_fqdn_measurements` | `avro_dns` | 4 | ~30 MB |
| `newly_registered_domains_measurements` | `avro_dns` | 11 | ~80 MB |
| `certstream_domains` | `json` | 2 | ~1-2 MB |

Total **20 parallel jobs** (GH Free public-repo concurrent-job ceiling).
Topic config lives in [`.github/shards.json`](.github/shards.json), shared
between `consume.yml` (the `plan` job emits the matrix) and `retry.yml`.

`format` selects the per-shard consumer; both produce Parquet output:

- **`avro_dns`** → `scripts/consume_group.py` decodes the Avro
  `MeasurementResult` records and aggregates them into the 16-column
  observation schema (above).
- **`json`** → `scripts/consume_json.py` parses each JSON payload into a
  per-topic schema registered in `TOPIC_SCHEMAS`.

Each shard takes `1/N` of the topic's offset range over the deterministic
12-hour window. Scripts use `assign()+seek()` (not `subscribe()+commit()`)
— the group id (one per shard) is only a label that gives each shard an
independent broker-side fetch quota. **Quota is per-connection**, not per
consumer-group across the cluster, so more shards = linearly more bandwidth
(up to ~10× the single-consumer rate). See `docs/empirical-throughput.md`
(if present) for the throughput experiment.

Re-runs overwrite assets via `--clobber`, so partial-day re-dispatches
don't double-count.

### Why Parquet (and not gzipped JSONL)

Previously this repo wrote gzipped JSONL. We switched to Parquet zstd-3
because of measured wins on the same data:

| query | gunzip+grep | gunzip+python | duckdb+Parquet | speedup |
|---|---:|---:|---:|---:|
| point lookup (1 domain in 6 shards) | 4.5s | 111.6s | **0.36s** | 310× vs python |
| top-15 hosting ASNs (aggregation) | n/a | 112.2s | **0.26s** | 430× |
| NS lookup across 18 shards (~2 GB gz) | n/a | 464.9s (7m38s) | **0.35s** | **1300×** |

File size also dropped ~34% on the avro_dns topics. `certstream_domains`
saw a more modest 11% drop because most of its bytes are high-entropy
fingerprint hex that doesn't compress in any format — but the format
consistency makes cross-topic JOIN trivial (see "Querying" below).

### Auto-retry

`retry.yml` runs twice daily at **04:30 and 16:30 UTC** — 4 hours after
each main consume fire. It auto-detects the most recent 12h boundary
(same logic as `consume.yml`), scans the matching `snap-YYYY-MM-DD-HH`
Release for expected assets, and if any are missing, re-dispatches
`consume.yml` with a `targets` input listing just the missing
`(topic, shard, shard_count, format)` entries plus `day` and `hour` inputs
pinning the window (so a queued retry crossing a boundary still targets
the intended one). Manual dispatch supports a `dry_run` mode that only
logs the missing list.

## Querying

We recommend [DuckDB](https://duckdb.org) — it reads Parquet natively,
including direct HTTP reads (`httpfs`), automatic multi-file globs, and
predicate pushdown that skips unrelated row groups.

### Single-window queries

```sql
-- Hosting ASN distribution across new domains observed in one window
duckdb -c "
SELECT a AS asn, COUNT(*) AS rows
FROM 'snap-2026-05-25-00/newly_registered_domains_measurements.shard-*.parquet'
WHERE k='ip'
GROUP BY a ORDER BY rows DESC LIMIT 15"

-- Domains with the most distinct IPs (IP-churn signal)
duckdb -c "
SELECT d AS domain, COUNT(DISTINCT ip) AS distinct_ips
FROM 'snap-2026-05-25-00/newly_registered_domains_measurements.shard-*.parquet'
WHERE k='ip' AND d IS NOT NULL
GROUP BY d ORDER BY distinct_ips DESC LIMIT 20"

-- Cross-topic join: find CT-log certificates issued for domains we measured
duckdb -c "
WITH cs AS (
  SELECT certIndex, source, fingerprint, unnest(domain_list) AS d
  FROM 'snap-2026-05-25-00/certstream_domains.shard-*.parquet'
)
SELECT cs.source, cs.fingerprint, ip.d, ip.ip, ip.a AS asn
FROM cs JOIN
  'snap-2026-05-25-00/newly_registered_domains_measurements.shard-*.parquet' ip
  ON ip.d = cs.d AND ip.k = 'ip'
WHERE ip.a = '13335'    -- Cloudflare
LIMIT 50"
```

### Cross-day / longitudinal queries

```sql
-- Domains whose hosting AS changed between two windows
duckdb -c "
WITH y AS (
  SELECT d, MIN(a) AS asn FROM 'snap-2026-05-24-12/newly_registered_domains_measurements.shard-*.parquet'
  WHERE k='ip' GROUP BY d
), t AS (
  SELECT d, MIN(a) AS asn FROM 'snap-2026-05-25-00/newly_registered_domains_measurements.shard-*.parquet'
  WHERE k='ip' GROUP BY d
)
SELECT y.d, y.asn AS yesterday_asn, t.asn AS today_asn
FROM y JOIN t USING (d)
WHERE y.asn != t.asn
LIMIT 50"

-- Multi-window scan via glob (note: filename= adds a column with source path)
duckdb -c "
SELECT filename, d, ip, first_ts, n
FROM read_parquet('snap-2026-05-*/newly_registered_domains_measurements.shard-*.parquet', filename=true)
WHERE d = 'example.com' AND k = 'ip'
ORDER BY first_ts"
```

### Read directly from GitHub Releases (no download)

DuckDB's `httpfs` extension lets you query a release asset without
downloading it locally:

```sql
duckdb -c "
INSTALL httpfs; LOAD httpfs;
SELECT COUNT(*) FROM
  'https://github.com/wangmm001/dns-tracking/releases/download/snap-2026-05-25-00/newly_registered_domains_measurements.shard-0.parquet'
WHERE k='ip'"
```

## Consume locally

For ad-hoc consumption without the GitHub Actions infrastructure:

```bash
# Bring up the Avro schema cache (one-time)
curl -sS http://schema.zonestream.openintel.nl/subjects/newly_registered_domains_measurements/versions/latest \
  | jq -r .schema > schemas/measurement_id_1.json

# Required Python deps
pip install fastavro confluent-kafka pyarrow

# Consume a shard of the domains topic over the last 1h
NOW=$(($(date +%s) * 1000))
python scripts/consume_group.py \
    --topic newly_registered_domains_measurements \
    --group local-test-$$ \
    --start-ms $((NOW - 3600000)) --end-ms $NOW \
    --shard-index 0 --shard-count 1 \
    --max-msgs 100000 \
    --out-obs /tmp/local.parquet

# Query it
duckdb -c "SELECT k, COUNT(*) FROM '/tmp/local.parquet' GROUP BY k"
```

`scripts/consume_json.py` works the same way for `certstream_domains`.

## Adapting / extending

- **Adjust shard counts** — edit `.github/shards.json`. Total ≤ 20
  (GH Free concurrent-job limit) and stay within `runs-on: ubuntu-latest`
  6-hour-per-job ceiling.
- **Add a new Avro DNS topic** — add to `.github/shards.json` with
  `format: avro_dns`. Avro schema must be id=1 in the upstream registry
  (other ids would need a schema-cache change).
- **Add a new JSON topic** — add to `.github/shards.json` with
  `format: json`, then add a schema entry in
  `TOPIC_SCHEMAS` (top of `scripts/consume_json.py`). Field types are
  pyarrow types — strings, int64, `pa.list_(pa.string())` etc.
- **Schema drift on existing JSON topic** — observed unknown fields are
  WARN-logged once per shard and surface in `stats.unknown_fields_seen` in
  the job log. Bump `TOPIC_SCHEMAS` and re-run.

## Historical compatibility

Releases tagged before the Parquet switch (`snap-2026-05-23` and earlier)
contain `.jsonl.gz` files using the previous record-per-line JSON schema.
DuckDB can read both formats — `read_json('*.jsonl.gz')` and
`read_parquet('*.parquet')` — and `UNION` them in one query if needed.
No automatic backfill is performed; if you want consistent format
end-to-end, re-consume the desired window with the current scripts
(`workflow_dispatch` with `day`/`hour` inputs).

## Notes

- DarkDNS schedules ~288 measurement rounds per domain over 48 hours after
  CT detection, but in practice we observed **burst-scheduled** measurements
  (peaks at ~7h and ~37h post-detection, not literal 10-minute cron).
  The `n` column in observation records captures repeat counts for that
  window only, not the full 48h history.
- Built-in ASN + GeoIP enrichment is from OpenINTEL's measurement pipeline,
  not added by this repo — the `a` and `c` columns come straight from the
  Avro source.
- **ccTLDs are essentially absent** from the upstream data — OpenINTEL
  only sees domains under TLDs that publish via ICANN CZDS (essentially
  all gTLDs) plus `.ch` / `.li`. Major ccTLDs (`.de`, `.uk`, `.fr`, `.cn`,
  `.jp`, ...) won't appear. See sibling `zonestream-archive` README for
  details.
- The DarkDNS "measurement_node" field reveals all observations come from
  a single physical host (`enschede-01` at OpenINTEL's NL facility) with 16
  parallel worker processes — the "16 nodes" in the paper's wording refers
  to worker instances, not physically distributed vantage points. Don't use
  this data for geographic-bias / cloaking research.
