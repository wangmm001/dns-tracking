"""Production consumer for the OpenINTEL Zonestream *_measurements topics.

Uses confluent-kafka with a stable consumer group so progress survives between
GH Actions runs. Extracts domain→IP / domain→NS observations and writes them
to gzipped JSONL plus a small full-fidelity sample for schema inspection.

Behavior:
- Connects with the supplied consumer group; the broker remembers our last
  committed offset across runs.
- Polls until run-seconds budget is exhausted, max-msgs reached, or the
  high-water-mark is reached and stays there.
- Commits offsets on close (and periodically during long runs).
- Decoding is fastavro.schemaless_reader against the cached schema id=1.

Output:
- --out-obs:    gzipped JSONL of observation records (kind: ip|ns|ns_ip|mx)
- --out-sample: gzipped JSONL of first --sample-n full MeasurementResult dicts

Env:
- KAFKA_BROKER  (default kafka.zonestream.openintel.nl:9092)
- SCHEMA_PATH   (default ../schemas/measurement_id_1.json)
"""
from __future__ import annotations

import argparse, gzip, io, json, os, sys, time
from pathlib import Path

from fastavro import schemaless_reader, parse_schema
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition


SCHEMA_PATH = Path(os.environ.get('SCHEMA_PATH',
    Path(__file__).resolve().parent.parent / 'schemas' / 'measurement_id_1.json'))
BROKER = os.environ.get('KAFKA_BROKER', 'kafka.zonestream.openintel.nl:9092')

TOPIC_ABBR = {
    'newly_registered_domains_measurements': 'domains',
    'newly_registered_fqdn_measurements': 'fqdn',
    'newly_issued_certificates_measurements': 'certs',
}


def extract_from_msg(d, topic_tag, out_obs):
    """Pull observations out of one decoded MeasurementResult dict; return count."""
    domain = (d.get('id') or '').rstrip('.')
    if not domain:
        return 0
    rows = d.get('resultList') or []
    n = 0
    for r in rows:
        qtype = r.get('query_type')
        qname = (r.get('query_name') or '').rstrip('.')
        ts = r.get('timestamp')
        rcode = r.get('status_code')
        ttl = r.get('response_ttl')
        is_primary = (qname == domain) if qname else False

        if qtype == 'A' or qtype == 'AAAA':
            ip = r.get('ip4_address') or r.get('ip6_address')
            if ip is None:
                continue
            rec = {
                'kind': 'ip' if is_primary else 'ns_ip',
                'ts': ts, 'topic': topic_tag,
                'domain': domain if is_primary else None,
                'ns': qname if not is_primary else None,
                'qtype': qtype, 'ip': ip,
                'as': r.get('as'), 'country': r.get('country'),
                'prefix': r.get('ip_prefix'),
                'ttl': ttl, 'rtt': r.get('rtt'), 'rcode': rcode,
            }
        elif qtype == 'NS' and is_primary:
            ns = (r.get('ns_address') or '').rstrip('.')
            if not ns:
                continue
            rec = {'kind': 'ns', 'ts': ts, 'topic': topic_tag,
                   'domain': domain, 'ns': ns, 'ttl': ttl, 'rcode': rcode}
        elif qtype == 'MX' and is_primary:
            mx = (r.get('mx_address') or '').rstrip('.')
            if not mx:
                continue
            rec = {'kind': 'mx', 'ts': ts, 'topic': topic_tag,
                   'domain': domain, 'mx': mx,
                   'preference': r.get('mx_preference'), 'ttl': ttl, 'rcode': rcode}
        else:
            continue

        rec = {k: v for k, v in rec.items() if v is not None}
        out_obs.write((json.dumps(rec, separators=(',', ':')) + '\n').encode())
        n += 1
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--topic', required=True)
    ap.add_argument('--group', required=True)
    ap.add_argument('--run-seconds', type=int, default=18000)
    ap.add_argument('--max-msgs', type=int, default=0)
    ap.add_argument('--offset-reset', choices=('earliest', 'latest'), default='latest')
    ap.add_argument('--sample-n', type=int, default=200)
    ap.add_argument('--out-obs', required=True)
    ap.add_argument('--out-sample', required=True)
    ap.add_argument('--commit-every', type=int, default=10000,
                    help='Commit offsets after this many messages')
    ap.add_argument('--idle-exit-seconds', type=int, default=30,
                    help='Exit if no message received for this long')
    args = ap.parse_args()

    schema = parse_schema(json.load(open(SCHEMA_PATH)))
    topic_tag = TOPIC_ABBR.get(args.topic, args.topic)

    conf = {
        'bootstrap.servers': BROKER,
        'group.id': args.group,
        'auto.offset.reset': args.offset_reset,
        'enable.auto.commit': False,

        # --- Fetch / network tuning for high-latency cross-region consume ---
        # NL broker, US-hosted GH runners → ~80-100 ms RTT. Defaults
        # (fetch.min.bytes=1) cap throughput at ~1 MB/s. Coalesce fetches
        # into ~1 MB chunks so RTT is amortized.
        'fetch.min.bytes': 1024*1024,        # wait for 1 MB or
        'fetch.wait.max.ms': 500,            # 500 ms, whichever first
        'fetch.message.max.bytes': 16*1024*1024,
        'max.partition.fetch.bytes': 16*1024*1024,
        # Large client-side queue so polling can drain decoded backlog
        'queued.max.messages.kbytes': 1024*1024,     # 1 GiB (librdkafka caps at 2 GiB - 1)
        'queued.min.messages': 1_000_000,
        # Larger socket recv buffer (BDP for ~100 ms RTT × ~10 MB/s ≈ 1 MB,
        # set to 8 MB so kernel doesn't throttle)
        'socket.receive.buffer.bytes': 8*1024*1024,

        # Session / poll keepalives during long extracts
        'session.timeout.ms': 60000,
        'max.poll.interval.ms': 600000,
    }
    consumer = Consumer(conf)
    consumer.subscribe([args.topic])

    obs_f = gzip.open(args.out_obs, 'wb', compresslevel=4)
    sample_f = gzip.open(args.out_sample, 'wt', encoding='utf-8', compresslevel=4)

    n_msgs = n_obs = n_sample = n_errors = 0
    n_bytes = 0
    start_t = time.time()
    last_msg_t = start_t
    last_log_t = start_t
    last_log_msgs = 0
    last_log_bytes = 0
    last_commit_msgs = 0

    print(f"[{args.topic}] group={args.group} budget={args.run_seconds}s reset={args.offset_reset}", flush=True)

    try:
        while True:
            now = time.time()
            if now - start_t >= args.run_seconds:
                print(f"[{args.topic}] run-seconds budget hit", flush=True)
                break
            if args.max_msgs and n_msgs >= args.max_msgs:
                print(f"[{args.topic}] max-msgs reached", flush=True)
                break
            if now - last_msg_t >= args.idle_exit_seconds and n_msgs > 0:
                print(f"[{args.topic}] idle for {args.idle_exit_seconds}s, exiting", flush=True)
                break

            msg = consumer.poll(timeout=5.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[{args.topic}] poll error: {msg.error()}", flush=True)
                continue

            last_msg_t = time.time()
            payload = msg.value()
            if not payload or len(payload) < 5:
                n_errors += 1
                continue
            n_msgs += 1
            n_bytes += len(payload)

            try:
                d = schemaless_reader(io.BytesIO(payload[5:]), schema)
            except Exception as e:
                n_errors += 1
                if n_errors < 5:
                    print(f"[{args.topic}] decode error at offset={msg.offset()}: {e}", file=sys.stderr)
                continue

            if n_sample < args.sample_n:
                def default(o):
                    if hasattr(o, 'isoformat'): return o.isoformat()
                    if isinstance(o, bytes): return o.hex()
                    return str(o)
                sample_f.write(json.dumps({'_kafka_ts': msg.timestamp()[1],
                                           '_kafka_offset': msg.offset(),
                                           **d}, default=default, ensure_ascii=False) + '\n')
                n_sample += 1

            n_obs += extract_from_msg(d, topic_tag, obs_f)

            if n_msgs - last_commit_msgs >= args.commit_every:
                consumer.commit(asynchronous=True)
                last_commit_msgs = n_msgs

            now = time.time()
            if now - last_log_t >= 10.0:
                dt = now - last_log_t
                rate = (n_msgs - last_log_msgs) / dt
                mbps = (n_bytes - last_log_bytes) / dt / 1024 / 1024
                print(f"[{args.topic}] msgs={n_msgs:,} obs={n_obs:,} sample={n_sample}  "
                      f"rate={rate:,.0f}/s ({mbps:.1f}MB/s)  elapsed={now-start_t:.0f}s", flush=True)
                last_log_t = now
                last_log_msgs = n_msgs
                last_log_bytes = n_bytes
    finally:
        obs_f.close()
        sample_f.close()
        try:
            consumer.commit(asynchronous=False)
        except KafkaException as e:
            print(f"[{args.topic}] commit-on-close failed: {e}", flush=True)
        consumer.close()

    elapsed = time.time() - start_t
    stats = {
        'topic': args.topic, 'group': args.group, 'msgs': n_msgs, 'obs': n_obs,
        'sample': n_sample, 'errors': n_errors, 'bytes_in': n_bytes,
        'elapsed_sec': round(elapsed, 1),
        'avg_msgs_per_sec': round(n_msgs / max(elapsed, 1), 1),
        'avg_mbps': round(n_bytes / max(elapsed, 1) / 1024 / 1024, 2),
    }
    print(json.dumps(stats, indent=2))


if __name__ == '__main__':
    main()
