"""Shard-aware consumer for plain-JSON Zonestream topics (no Confluent envelope).

Mirrors the shard-range logic of consume_group.py but skips the Avro decode and
DNS-observation extraction — each Kafka payload is already a JSON object, so we
just validate and append to a gzipped JSONL file verbatim. Used for topics like
`certstream_domains` where we want to preserve the original cert↔domain records
in full (issuer index, fingerprint, submission timestamps) for later join with
the Avro measurement streams.

Env:
- KAFKA_BROKER  (default kafka.zonestream.openintel.nl:9092)
"""
from __future__ import annotations

import argparse, gzip, json, os, sys, time

from confluent_kafka import Consumer, KafkaError, TopicPartition


BROKER = os.environ.get('KAFKA_BROKER', 'kafka.zonestream.openintel.nl:9092')


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
    ap.add_argument('--out-obs', required=True, help='Gzipped JSONL output')
    ap.add_argument('--idle-exit-seconds', type=int, default=30)
    args = ap.parse_args()

    if not (0 <= args.shard_index < args.shard_count):
        ap.error(f"--shard-index ({args.shard_index}) must be in [0, {args.shard_count})")

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

    n_msgs = n_lines = n_errors = 0
    n_bytes = 0
    start_t = time.time()
    last_msg_t = start_t
    last_log_t = start_t
    last_log_msgs = 0
    last_log_bytes = 0
    done_reason = 'unknown'

    out_f = gzip.open(args.out_obs, 'wb', compresslevel=4)
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
                json.loads(payload)  # validate; raises on malformed
                out_f.write(payload)
                if not payload.endswith(b'\n'):
                    out_f.write(b'\n')
                n_lines += 1
            except Exception as e:
                n_errors += 1
                if n_errors < 5:
                    print(f"decode error at offset={msg.offset()}: {e}", file=sys.stderr)
                continue

            now = time.time()
            if now - last_log_t >= 10.0:
                dt = now - last_log_t
                rate = (n_msgs - last_log_msgs) / dt
                mbps = (n_bytes - last_log_bytes) / dt / 1024 / 1024
                pct = 100 * (msg.offset() - shard_start) / max(shard_end - shard_start, 1)
                print(f"[{args.topic} shard {args.shard_index}] msgs={n_msgs:,} "
                      f"lines={n_lines:,} rate={rate:,.0f}/s ({mbps:.1f}MB/s) "
                      f"progress={pct:.1f}% elapsed={now-start_t:.0f}s", flush=True)
                last_log_t = now
                last_log_msgs = n_msgs
                last_log_bytes = n_bytes
    finally:
        out_f.close()
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
        'done_reason': done_reason,
    }
    print(json.dumps(stats, indent=2))


if __name__ == '__main__':
    main()
