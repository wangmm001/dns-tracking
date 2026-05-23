"""Shard-aware consumer for the OpenINTEL Zonestream *_measurements topics.

Multiple instances (with different --shard-index but the same --start-ms /
--end-ms / --shard-count) cover disjoint offset slices of the same topic
in parallel. Each shard uses a unique consumer group label so the broker
allocates separate fetch quota to each — which is the only way to go past
the per-consumer ~1100 msg/s server-side cap.

Consumption flow:
  1. Compute total offset window [start_off, end_off] from --start-ms / --end-ms
     via Kafka's OffsetsForTimes API.
  2. Divide into shard-count equal-size chunks; shard i takes [start_off + i*chunk,
     start_off + (i+1)*chunk).
  3. assign() + seek() to the shard's start_off and consume until end_off
     (or until the run-seconds budget expires — in which case partial
     coverage of the trailing part of this shard is lost).

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


def compute_shard_range(consumer, topic, start_ms, end_ms, shard_idx, shard_count):
    """Return (shard_start_offset, shard_end_offset) for this shard's slice
    of the [start_ms, end_ms] time window."""
    tp_start = TopicPartition(topic, 0, start_ms)
    tp_end   = TopicPartition(topic, 0, end_ms)
    starts = consumer.offsets_for_times([tp_start], timeout=30.0)
    ends   = consumer.offsets_for_times([tp_end],   timeout=30.0)
    if not starts or starts[0].offset < 0:
        raise RuntimeError(f"could not resolve start offset for ts={start_ms}")
    if not ends or ends[0].offset < 0:
        # end_ms is in the future or beyond retention; use high-water-mark
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=10.0)
        end_off = high
    else:
        end_off = ends[0].offset
    start_off = starts[0].offset

    total = end_off - start_off
    chunk = total // shard_count
    shard_start = start_off + shard_idx * chunk
    shard_end = end_off if shard_idx == shard_count - 1 else start_off + (shard_idx + 1) * chunk
    return start_off, end_off, shard_start, shard_end


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--topic', required=True)
    ap.add_argument('--group', required=True,
                    help='Group ID label; only used for broker fetch-quota accounting since '
                         'we use assign()+seek() not subscribe()+commit().')
    ap.add_argument('--start-ms', type=int, required=True,
                    help='Unix-time ms; lower bound of time window')
    ap.add_argument('--end-ms', type=int, required=True,
                    help='Unix-time ms; upper bound of time window')
    ap.add_argument('--shard-index', type=int, default=0)
    ap.add_argument('--shard-count', type=int, default=1)
    ap.add_argument('--run-seconds', type=int, default=18000)
    ap.add_argument('--max-msgs', type=int, default=0)
    ap.add_argument('--sample-n', type=int, default=200)
    ap.add_argument('--out-obs', required=True)
    ap.add_argument('--out-sample', required=True)
    ap.add_argument('--idle-exit-seconds', type=int, default=30)
    args = ap.parse_args()

    if not (0 <= args.shard_index < args.shard_count):
        ap.error(f"--shard-index ({args.shard_index}) must be in [0, {args.shard_count})")

    schema = parse_schema(json.load(open(SCHEMA_PATH)))
    topic_tag = TOPIC_ABBR.get(args.topic, args.topic)

    conf = {
        'bootstrap.servers': BROKER,
        # Unique group per shard so each shard gets its own broker-side fetch quota.
        # We use assign()+seek() so we DON'T commit offsets; group_id here is just a label.
        'group.id': args.group,
        'enable.auto.commit': False,

        # Fetch / network tuning
        'fetch.min.bytes': 1024*1024,
        'fetch.wait.max.ms': 500,
        'fetch.message.max.bytes': 16*1024*1024,
        'max.partition.fetch.bytes': 16*1024*1024,
        'queued.max.messages.kbytes': 1024*1024,
        'queued.min.messages': 1_000_000,
        'socket.receive.buffer.bytes': 8*1024*1024,

        'session.timeout.ms': 60000,
        'max.poll.interval.ms': 600000,
    }
    consumer = Consumer(conf)

    # Resolve offset window and shard slice
    total_start, total_end, shard_start, shard_end = compute_shard_range(
        consumer, args.topic, args.start_ms, args.end_ms,
        args.shard_index, args.shard_count)
    tp = TopicPartition(args.topic, 0, shard_start)
    consumer.assign([tp])
    consumer.seek(tp)
    print(f"[{args.topic} shard {args.shard_index}/{args.shard_count}] "
          f"window [{args.start_ms}..{args.end_ms}] → offsets [{total_start}..{total_end}], "
          f"this shard = [{shard_start}..{shard_end}) "
          f"({shard_end - shard_start:,} msgs)", flush=True)

    obs_f = gzip.open(args.out_obs, 'wb', compresslevel=4)
    sample_f = gzip.open(args.out_sample, 'wt', encoding='utf-8', compresslevel=4)

    n_msgs = n_obs = n_sample = n_errors = 0
    n_bytes = 0
    start_t = time.time()
    last_msg_t = start_t
    last_log_t = start_t
    last_log_msgs = 0
    last_log_bytes = 0
    done_reason = 'unknown'

    try:
        while True:
            now = time.time()
            if now - start_t >= args.run_seconds:
                done_reason = 'budget-expired'
                break
            if args.max_msgs and n_msgs >= args.max_msgs:
                done_reason = 'max-msgs-reached'
                break
            if now - last_msg_t >= args.idle_exit_seconds and n_msgs > 0:
                done_reason = 'idle-exit'
                break

            msg = consumer.poll(timeout=5.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"poll error: {msg.error()}", flush=True)
                continue

            # Stop when we cross our shard's end offset (exclusive)
            if msg.offset() >= shard_end:
                done_reason = 'shard-end-reached'
                break

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
                    print(f"decode error at offset={msg.offset()}: {e}", file=sys.stderr)
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

            now = time.time()
            if now - last_log_t >= 10.0:
                dt = now - last_log_t
                rate = (n_msgs - last_log_msgs) / dt
                mbps = (n_bytes - last_log_bytes) / dt / 1024 / 1024
                # Progress through shard
                pct = 100 * (msg.offset() - shard_start) / max(shard_end - shard_start, 1)
                print(f"[{args.topic} shard {args.shard_index}] msgs={n_msgs:,} obs={n_obs:,} "
                      f"sample={n_sample}  rate={rate:,.0f}/s ({mbps:.1f}MB/s)  "
                      f"shard_progress={pct:.1f}% elapsed={now-start_t:.0f}s", flush=True)
                last_log_t = now
                last_log_msgs = n_msgs
                last_log_bytes = n_bytes
    finally:
        obs_f.close()
        sample_f.close()
        consumer.close()

    elapsed = time.time() - start_t
    stats = {
        'topic': args.topic, 'shard_index': args.shard_index, 'shard_count': args.shard_count,
        'group': args.group, 'msgs': n_msgs, 'obs': n_obs,
        'sample': n_sample, 'errors': n_errors, 'bytes_in': n_bytes,
        'elapsed_sec': round(elapsed, 1),
        'avg_msgs_per_sec': round(n_msgs / max(elapsed, 1), 1),
        'avg_mbps': round(n_bytes / max(elapsed, 1) / 1024 / 1024, 2),
        'shard_start_offset': shard_start, 'shard_end_offset': shard_end,
        'shard_msgs_total': shard_end - shard_start,
        'shard_msgs_covered': n_msgs,
        'shard_coverage_pct': round(100 * n_msgs / max(shard_end - shard_start, 1), 2),
        'done_reason': done_reason,
    }
    print(json.dumps(stats, indent=2))


if __name__ == '__main__':
    main()
