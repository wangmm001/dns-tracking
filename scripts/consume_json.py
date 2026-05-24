"""Shard-aware consumer for plain-JSON Zonestream topics (no Confluent envelope).

Mirrors the shard-range logic of consume_group.py but skips the Avro decode
step. Each Kafka payload is a JSON object; we parse and write to Parquet
(zstd-3, dict-encoded) using a per-topic schema. Currently supports the
`certstream_domains` topic; other JSON topics would need a schema entry
in TOPIC_SCHEMAS.

Output:
- --out-obs: Parquet file. Compared to gzipped-JSONL the size win is modest
            (~10%) because most bytes go to high-entropy fingerprint hex,
            but downstream queries via duckdb are 100-1000× faster than
            gunzip+jq, and the format matches the avro_dns output (same
            tooling end-to-end).

Schema policy: enumerate all known fields explicitly. Any unknown field in
an incoming record is silently dropped (warning logged once). If OpenINTEL
evolves the schema we'll notice via the warning and add the new field.

Env:
- KAFKA_BROKER  (default kafka.zonestream.openintel.nl:9092)
"""
from __future__ import annotations

import argparse, json, os, sys, time

from confluent_kafka import Consumer, KafkaError, TopicPartition
import pyarrow as pa
import pyarrow.parquet as pq


BROKER = os.environ.get('KAFKA_BROKER', 'kafka.zonestream.openintel.nl:9092')


# Per-topic Parquet schemas. Add a new entry to support a new JSON topic.
TOPIC_SCHEMAS = {
    'certstream_domains': pa.schema([
        # int64 numeric fields
        ('certIndex',            pa.int64()),
        ('seen',                 pa.int64()),
        ('submission_timestamp', pa.int64()),
        # string fields — dictionary-encoded by Parquet default
        ('source',               pa.string()),     # CT log name; ~20 distinct values
        ('updateType',           pa.string()),     # X509LogEntry / PrecertLogEntry
        ('fingerprint',          pa.string()),     # SHA1 hex, unique per row
        # list<string> — domain_list = CN + SANs; sld_list = deduped second-level
        ('domain_list',          pa.list_(pa.string())),
        ('sld_list',             pa.list_(pa.string())),
    ]),
}


def compute_shard_range(consumer, topic, start_ms, end_ms, shard_idx, shard_count):
    """Return (total_start, total_end, shard_start, shard_end) — same semantics
    as consume_group.compute_shard_range."""
    tp_start = TopicPartition(topic, 0, start_ms)
    tp_end   = TopicPartition(topic, 0, end_ms)
    starts = consumer.offsets_for_times([tp_start], timeout=30.0)
    ends   = consumer.offsets_for_times([tp_end],   timeout=30.0)
    if not starts or starts[0].offset < 0:
        raise RuntimeError(f"could not resolve start offset for ts={start_ms}")
    if not ends or ends[0].offset < 0:
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


def _coerce(value, pa_type):
    """Best-effort coerce a JSON value to match a pyarrow primitive type.
    Pyarrow itself handles int↔int and str↔str, but the broker has been
    observed to occasionally emit submission_timestamp as float — silently
    drop the fraction so writing doesn't fail."""
    if value is None:
        return None
    if pa.types.is_integer(pa_type) and isinstance(value, float):
        return int(value)
    return value


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--topic', required=True)
    ap.add_argument('--group', required=True,
                    help='Group ID label (broker-side fetch-quota accounting only; '
                         'we use assign()+seek(), not subscribe()+commit()).')
    ap.add_argument('--start-ms', type=int, required=True)
    ap.add_argument('--end-ms', type=int, required=True)
    ap.add_argument('--shard-index', type=int, default=0)
    ap.add_argument('--shard-count', type=int, default=1)
    ap.add_argument('--run-seconds', type=int, default=18000)
    ap.add_argument('--max-msgs', type=int, default=0)
    ap.add_argument('--out-obs', required=True, help='Parquet output path')
    ap.add_argument('--idle-exit-seconds', type=int, default=30)
    ap.add_argument('--flush-every', type=int, default=200_000,
                    help='Flush a row group to Parquet every N records (bounded memory).')
    args = ap.parse_args()

    if not (0 <= args.shard_index < args.shard_count):
        ap.error(f"--shard-index ({args.shard_index}) must be in [0, {args.shard_count})")

    schema = TOPIC_SCHEMAS.get(args.topic)
    if schema is None:
        ap.error(f"No Parquet schema registered for topic {args.topic!r}. "
                 f"Add it to TOPIC_SCHEMAS in scripts/consume_json.py.")
    fields = [f.name for f in schema]

    conf = {
        'bootstrap.servers': BROKER,
        'group.id': args.group,
        'enable.auto.commit': False,
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

    # Pre-build column buffers + ParquetWriter for chunked write
    buf = {col: [] for col in fields}
    n_buffered = 0
    unknown_fields_seen = set()  # warn once per unknown key

    n_msgs = n_lines = n_errors = 0
    n_bytes = 0
    start_t = time.time()
    last_msg_t = start_t
    last_log_t = start_t
    last_log_msgs = 0
    last_log_bytes = 0
    done_reason = 'unknown'

    writer = pq.ParquetWriter(args.out_obs, schema,
                              compression='zstd', compression_level=3,
                              use_dictionary=True)

    def flush_buffer():
        nonlocal buf, n_buffered
        if n_buffered == 0:
            return
        tbl = pa.table(buf, schema=schema)
        writer.write_table(tbl)
        buf = {col: [] for col in fields}
        n_buffered = 0

    try:
        while True:
            now = time.time()
            if now - start_t >= args.run_seconds:
                done_reason = 'budget-expired'; break
            if args.max_msgs and n_msgs >= args.max_msgs:
                done_reason = 'max-msgs-reached'; break
            if now - last_msg_t >= args.idle_exit_seconds and n_msgs > 0:
                done_reason = 'idle-exit'; break

            msg = consumer.poll(timeout=5.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"poll error: {msg.error()}", flush=True)
                continue

            if msg.offset() >= shard_end:
                done_reason = 'shard-end-reached'; break

            last_msg_t = time.time()
            payload = msg.value()
            if not payload:
                n_errors += 1
                continue
            n_msgs += 1
            n_bytes += len(payload)

            try:
                d = json.loads(payload)
            except Exception as e:
                n_errors += 1
                if n_errors < 5:
                    print(f"decode error at offset={msg.offset()}: {e}", file=sys.stderr)
                continue

            # Track any unexpected fields once each so we notice schema drift
            for k in d.keys():
                if k not in buf and k not in unknown_fields_seen:
                    unknown_fields_seen.add(k)
                    print(f"WARN unknown field {k!r} in topic {args.topic} "
                          f"(offset {msg.offset()}); dropping. Update TOPIC_SCHEMAS to capture.",
                          file=sys.stderr, flush=True)

            # Append known fields into column buffers
            for col, pa_field in zip(fields, schema):
                buf[col].append(_coerce(d.get(col), pa_field.type))
            n_buffered += 1
            n_lines += 1

            if n_buffered >= args.flush_every:
                flush_buffer()

            now = time.time()
            if now - last_log_t >= 10.0:
                dt = now - last_log_t
                rate = (n_msgs - last_log_msgs) / dt
                mbps = (n_bytes - last_log_bytes) / dt / 1024 / 1024
                pct = 100 * (msg.offset() - shard_start) / max(shard_end - shard_start, 1)
                print(f"[{args.topic} shard {args.shard_index}] msgs={n_msgs:,} "
                      f"buffered={n_buffered:,} rate={rate:,.0f}/s ({mbps:.1f}MB/s) "
                      f"progress={pct:.1f}% elapsed={now-start_t:.0f}s", flush=True)
                last_log_t = now
                last_log_msgs = n_msgs
                last_log_bytes = n_bytes
    finally:
        # Always finalize the Parquet file. flush_buffer() can be no-op safely.
        # writer.close() writes the footer — without it the file is unreadable.
        try:
            flush_buffer()
        finally:
            writer.close()
            consumer.close()

    elapsed = time.time() - start_t
    stats = {
        'topic': args.topic, 'shard_index': args.shard_index, 'shard_count': args.shard_count,
        'group': args.group, 'msgs': n_msgs, 'lines_written': n_lines,
        'errors': n_errors, 'bytes_in': n_bytes,
        'elapsed_sec': round(elapsed, 1),
        'avg_msgs_per_sec': round(n_msgs / max(elapsed, 1), 1),
        'avg_mbps': round(n_bytes / max(elapsed, 1) / 1024 / 1024, 2),
        'shard_start_offset': shard_start, 'shard_end_offset': shard_end,
        'shard_msgs_total': shard_end - shard_start,
        'shard_msgs_covered': n_msgs,
        'shard_coverage_pct': round(100 * n_msgs / max(shard_end - shard_start, 1), 2),
        'unknown_fields_seen': sorted(unknown_fields_seen),
        'done_reason': done_reason,
    }
    print(json.dumps(stats, indent=2))


if __name__ == '__main__':
    main()
