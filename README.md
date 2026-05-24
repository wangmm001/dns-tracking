# dns-tracking

Long-term tracking of newly registered domains' DNS state (IP, NS, MX, AS,
country) sourced from [OpenINTEL Zonestream's][1] reactive measurement Avro
streams ([DarkDNS][2], IMC '24).

[1]: https://openintel.nl/data/zonestream/
[2]: https://arxiv.org/abs/2405.12010

## What this is

OpenINTEL publishes three high-volume Avro topics on their public Kafka broker
(`kafka.zonestream.openintel.nl:9092`, anonymous, PLAINTEXT) containing the
raw per-query DNS measurement results for newly registered domains:

- `newly_registered_domains_measurements` (~432 GB/day, ~221M msgs/day)
- `newly_registered_fqdn_measurements`    (~209 GB/day, ~107M msgs/day)
- `newly_issued_certificates_measurements` (~157 GB/day,  ~82M msgs/day)

`zonestream-archive` archives the small JSON topics; this repo handles the
large Avro ones in a different way: stream-consume them on GitHub Actions
(multi-Gbps network) and extract only the durable observation records into
gzipped JSONL.

## What gets stored

One Release per UTC day, tagged `snap-YYYY-MM-DD`, with three assets per
topic:

- `<topic>.jsonl.gz`        — extracted observation records
- `<topic>.sample.jsonl.gz` — first 200 full-fidelity decoded `MeasurementResult`
  dicts (so you can inspect schema variants without re-pulling raw Avro)

Observation record shapes (`kind` discriminator):

```jsonc
// kind=ip      A or AAAA lookup of the candidate domain
{"kind":"ip","ts":1779548108867,"topic":"domains","domain":"example.com",
 "qtype":"A","ip":"104.21.55.10","as":"13335","country":"US",
 "prefix":"104.16.0.0/13","ttl":300,"rtt":12.0,"rcode":0}

// kind=ns      NS record of the candidate domain
{"kind":"ns","ts":1779548108867,"topic":"domains","domain":"example.com",
 "ns":"ns1.cloudflare.com","ttl":3600,"rcode":0}

// kind=ns_ip   support lookup: A/AAAA of an NS hostname
{"kind":"ns_ip","ts":1779548108867,"topic":"domains",
 "ns":"ns1.cloudflare.com","qtype":"A","ip":"1.1.1.1","as":"13335",
 "country":"US","prefix":"1.1.1.0/24","ttl":86400,"rtt":3.0,"rcode":0}

// kind=mx      MX record of the candidate domain
{"kind":"mx","ts":1779548108867,"topic":"domains","domain":"example.com",
 "mx":"mail.example.com","preference":10,"ttl":3600,"rcode":0}
```

Compression: ~10-15 GB/day total across all three topics.

## How it works

`consume.yml` runs daily at 00:30 UTC (and on manual dispatch). It uses a
**matrix strategy** with per-topic shard counts sized to historical worst-case
duration, so all topics finish in roughly the same wall-clock budget:

| topic                                    | shards | group id (one per shard)                                       |
|------------------------------------------|-------:|----------------------------------------------------------------|
| `newly_issued_certificates_measurements` |      3 | `gha-dns-tracking-<topic>-shard{0..2}`                         |
| `newly_registered_fqdn_measurements`     |      4 | `gha-dns-tracking-<topic>-shard{0..3}`                         |
| `newly_registered_domains_measurements`  |     13 | `gha-dns-tracking-<topic>-shard{0..12}`                        |

Total 20 parallel jobs (GH Free public-repo concurrent-job ceiling). Each
shard takes 1/N of the topic's offset range over a deterministic window
anchored to `[DAY 00:00 UTC − 24h, DAY 00:00 UTC)`, so consecutive runs tile
the timeline with no overlap or gap. The script uses `assign()+seek()` and
does NOT commit offsets — the group id is only a label that gives each
shard an independent broker-side fetch quota.

Each shard's observation stream is uploaded to a per-day Release
(`snap-YYYY-MM-DD`) as `<topic>.shard-<N>.jsonl.gz`. Re-runs overwrite via
`--clobber`.

Shard counts live in [`.github/shards.json`](.github/shards.json), shared
between `consume.yml` (plan job emits the matrix) and `retry.yml`.

### Auto-retry

`retry.yml` runs daily at 04:00 UTC. It scans `snap-$(date -u +%F)` for
expected assets and, if any are missing, re-dispatches `consume.yml` with
a `targets` input listing just the missing `(topic, shard, shard_count)`
triples and a `day` input pinning the date (so a queued retry crossing
midnight still targets the intended day). Manual dispatch supports a
`dry_run` mode that only logs the missing list.

## Consume locally

```bash
# Bring up the schema cache
curl -sS http://schema.zonestream.openintel.nl/subjects/newly_registered_domains_measurements/versions/latest \
  | jq -r .schema > schemas/measurement_id_1.json

# Stream-consume from a Unix-time millisecond offset; not consumer-group based
pip install fastavro
python scripts/extract.py \
    --topic newly_registered_domains_measurements \
    --start-ms $(( ($(date +%s) - 600) * 1000 )) \
    --max-msgs 10000 --sample-n 50 \
    --out-obs /tmp/obs.jsonl.gz --out-sample /tmp/sample.jsonl.gz
```

## Loading observations for analysis

```python
import gzip, json
from collections import defaultdict

# domain -> sorted [(ts, ip, as, country, prefix), ...]
hist = defaultdict(list)
with gzip.open('newly_registered_domains_measurements.jsonl.gz', 'rt') as f:
    for line in f:
        r = json.loads(line)
        if r['kind'] == 'ip':
            hist[r['domain']].append(
                (r['ts'], r['ip'], r.get('as'), r.get('country'), r.get('prefix')))

# Domains whose IP set changed within the day
changed = {d: obs for d, obs in hist.items()
           if len({o[1] for o in obs}) > 1}
print(f"{len(changed)} domains saw their IP change today")
```

## Notes

- Each individual observation record is timestamped at *query* time, not
  *measurement-scheduled* time. DarkDNS schedules ~288 measurement rounds
  per domain over 48 hours after detection, but in practice we observed
  burst-scheduled measurements (peaks at ~7h and ~37h post-detection, not
  literal 10-minute cron — see `docs/darkdns-empirical.md` if you want the
  full breakdown).
- Built-in ASN + GeoIP enrichment is from OpenINTEL's measurement pipeline,
  not added by this repo.
- ccTLDs are essentially absent — OpenINTEL only sees domains in CZDS gTLDs
  (+ .ch / .li). See sibling `zonestream-archive` README for details.
