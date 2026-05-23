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
**matrix strategy** to consume all three topics in parallel — three jobs, each
with its own stable Kafka consumer group:

```
gha-dns-tracking-dns-tracking-newly_registered_domains_measurements
gha-dns-tracking-dns-tracking-newly_registered_fqdn_measurements
gha-dns-tracking-dns-tracking-newly_issued_certificates_measurements
```

The broker remembers our last committed offset per group, so each run picks
up where the previous left off. A run consumes for up to 5 hours of
wall-clock time (under the 6-hour GH Actions per-job limit), commits offsets
on shutdown, and uploads the day's observation files as Release assets
(`--clobber`, so multi-run-per-day appends just overwrite the asset with the
accumulated content).

## Bootstrap (first run)

Manual dispatch with `offset_reset: latest` and the default 5-hour budget:
this starts the consumer groups at the broker high-water-mark and consumes
~5 hours of fresh data. Daily cron continues from there.

For backfill (consume from earliest available, which on this broker is
~28 days ago), dispatch with `offset_reset: earliest` — but be aware total
data over 28 days is enormous, and you almost certainly want `max_msgs` to
cap the bootstrap or break it across several runs.

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
