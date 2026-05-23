"""Stream-consume one of the OpenINTEL *_measurements Avro topics,
extract domain→IP / domain→NS observations to gzipped JSONL.

Usage:
  extract.py <topic> <start_ms> [--end-ms END_MS] [--max-msgs N] \\
             --out-obs OBS.jsonl.gz --out-sample SAMPLE.jsonl.gz

The single JSONL output stream uses a `kind` field to discriminate:
  - {kind: "ip",    ts, domain, qtype: A|AAAA, ip, as, country, prefix, ttl, rtt, rcode}
  - {kind: "ns",    ts, domain, ns, ttl, rcode}
  - {kind: "ns_ip", ts, ns, qtype, ip, as, country, prefix, ttl}
  - {kind: "mx",    ts, domain, mx, preference, ttl}

A separate sample file preserves the first N full-fidelity decoded MeasurementResult
records so the analyst can inspect schema variants without re-pulling raw Avro.
"""
from __future__ import annotations

import argparse, gzip, io, json, os, struct, subprocess, sys, time
from pathlib import Path

from fastavro import schemaless_reader, parse_schema


SCHEMA_PATH = Path(os.environ.get('SCHEMA_PATH',
    Path(__file__).resolve().parent.parent / 'schemas' / 'measurement_id_1.json'))
BROKER = os.environ.get('KAFKA_BROKER', 'kafka.zonestream.openintel.nl:9092')

# Map full topic name → short tag for the `topic` field
TOPIC_ABBR = {
    'newly_registered_domains_measurements': 'domains',
    'newly_registered_fqdn_measurements': 'fqdn',
    'newly_issued_certificates_measurements': 'certs',
}


def read_exact(stream, n):
    """Read exactly n bytes from a binary stream, or return b'' on EOF."""
    buf = bytearray()
    while len(buf) < n:
        chunk = stream.read(n - len(buf))
        if not chunk:
            return bytes(buf) if buf else b''
        buf.extend(chunk)
    return bytes(buf)


def extract_from_msg(d, topic_tag, out_obs):
    """Pull observations out of one decoded MeasurementResult dict."""
    domain = (d.get('id') or '').rstrip('.')
    if not domain:
        return 0
    rows = d.get('resultList') or []
    n_emitted = 0
    for r in rows:
        qtype = r.get('query_type')
        qname = (r.get('query_name') or '').rstrip('.')
        ts = r.get('timestamp')  # ms since epoch
        rcode = r.get('status_code')
        ttl = r.get('response_ttl')

        # The MeasurementResult `id` is the candidate domain being measured.
        # If query_name == id, this row is a *primary* observation about it.
        # If query_name != id, it's a *support* lookup (typically resolving an NS hostname).
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
            # Drop None fields to keep JSONL compact
            rec = {k: v for k, v in rec.items() if v is not None}
            out_obs.write((json.dumps(rec, separators=(',', ':')) + '\n').encode())
            n_emitted += 1

        elif qtype == 'NS' and is_primary:
            ns = (r.get('ns_address') or '').rstrip('.')
            if not ns:
                continue
            rec = {
                'kind': 'ns', 'ts': ts, 'topic': topic_tag,
                'domain': domain, 'ns': ns,
                'ttl': ttl, 'rcode': rcode,
            }
            rec = {k: v for k, v in rec.items() if v is not None}
            out_obs.write((json.dumps(rec, separators=(',', ':')) + '\n').encode())
            n_emitted += 1

        elif qtype == 'MX' and is_primary:
            mx = (r.get('mx_address') or '').rstrip('.')
            if not mx:
                continue
            rec = {
                'kind': 'mx', 'ts': ts, 'topic': topic_tag,
                'domain': domain, 'mx': mx,
                'preference': r.get('mx_preference'),
                'ttl': ttl, 'rcode': rcode,
            }
            rec = {k: v for k, v in rec.items() if v is not None}
            out_obs.write((json.dumps(rec, separators=(',', ':')) + '\n').encode())
            n_emitted += 1

    return n_emitted


def stream_topic(topic, start_ms, end_ms, sample_n, out_obs_path, out_sample_path, max_msgs=None):
    schema = parse_schema(json.load(open(SCHEMA_PATH)))
    topic_tag = TOPIC_ABBR.get(topic, topic)
    print(f"[{topic}] start_ms={start_ms} end_ms={end_ms or '∞'} sample={sample_n} max={max_msgs or '∞'}", flush=True)

    # kcat doesn't support time-range slicing; we run until end_ms is reached or -e fires
    cmd = ['kcat', '-b', BROKER, '-t', topic, '-C',
           '-o', f's@{start_ms}', '-e',
           '-q',  # quiet
           '-f', '%S %T %o\n%s']
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, bufsize=0)
    # Wrap with BufferedReader for fast readline() / read() on binary pipe
    pipe = io.BufferedReader(proc.stdout, buffer_size=4*1024*1024)

    obs_f = gzip.open(out_obs_path, 'wb', compresslevel=4)
    sample_f = gzip.open(out_sample_path, 'wt', encoding='utf-8', compresslevel=4)

    n_msgs = 0
    n_obs = 0
    n_sample = 0
    n_bytes = 0
    n_errors = 0
    last_log_t = time.time()
    last_log_msgs = 0
    last_log_bytes = 0
    start_t = last_log_t

    try:
        while True:
            header = pipe.readline()
            if not header:
                break
            parts = header.rstrip(b'\n').split()
            if len(parts) != 3:
                continue
            try:
                size = int(parts[0]); ts_ms = int(parts[1]); offset = int(parts[2])
            except ValueError:
                continue
            payload = read_exact(pipe, size)
            if len(payload) != size:
                break

            if end_ms and ts_ms >= end_ms:
                # Drain remaining quickly until kcat exits
                proc.terminate()
                break

            n_msgs += 1
            n_bytes += size

            # Decode Avro
            try:
                if len(payload) < 5: raise ValueError("payload too short")
                d = schemaless_reader(io.BytesIO(payload[5:]), schema)
            except Exception as e:
                n_errors += 1
                if n_errors < 5:
                    print(f"[{topic}] decode error at offset={offset}: {e}", file=sys.stderr)
                continue

            # Sample first N full-fidelity records (for schema-variant inspection)
            if n_sample < sample_n:
                # Convert datetime/bytes if any are in the dict (fastavro returns native types)
                def default(o):
                    if hasattr(o, 'isoformat'): return o.isoformat()
                    if isinstance(o, bytes): return o.hex()
                    return str(o)
                sample_f.write(json.dumps({'_kafka_ts': ts_ms, '_kafka_offset': offset, **d},
                                          default=default, ensure_ascii=False) + '\n')
                n_sample += 1

            # Extract observations
            n_obs += extract_from_msg(d, topic_tag, obs_f)

            # Progress every 5 seconds
            now = time.time()
            if now - last_log_t >= 5.0:
                dt = now - last_log_t
                msgs_per_sec = (n_msgs - last_log_msgs) / dt
                mb_per_sec = (n_bytes - last_log_bytes) / dt / 1024 / 1024
                total_dt = now - start_t
                print(f"[{topic}] msgs={n_msgs:,} obs={n_obs:,} sample={n_sample}  "
                      f"rate={msgs_per_sec:,.0f}msg/s ({mb_per_sec:.1f} MB/s)  "
                      f"elapsed={total_dt:.0f}s", flush=True)
                last_log_t = now
                last_log_msgs = n_msgs
                last_log_bytes = n_bytes

            if max_msgs and n_msgs >= max_msgs:
                proc.terminate()
                break
    finally:
        obs_f.close()
        sample_f.close()
        proc.wait(timeout=10)

    total_dt = time.time() - start_t
    print(f"[{topic}] DONE: msgs={n_msgs:,} obs={n_obs:,} errors={n_errors} "
          f"bytes_in={n_bytes/1e9:.2f}GB elapsed={total_dt:.0f}s "
          f"avg_rate={n_msgs/max(total_dt,1):,.0f}msg/s", flush=True)

    return {
        'topic': topic, 'msgs': n_msgs, 'obs': n_obs, 'sample': n_sample,
        'errors': n_errors, 'bytes_in': n_bytes, 'elapsed_sec': total_dt,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--topic', required=True)
    ap.add_argument('--start-ms', type=int, required=True)
    ap.add_argument('--end-ms', type=int, default=None)
    ap.add_argument('--max-msgs', type=int, default=None)
    ap.add_argument('--sample-n', type=int, default=200)
    ap.add_argument('--out-obs', required=True)
    ap.add_argument('--out-sample', required=True)
    args = ap.parse_args()
    stats = stream_topic(args.topic, args.start_ms, args.end_ms, args.sample_n,
                         args.out_obs, args.out_sample, args.max_msgs)
    print(json.dumps(stats, indent=2))


if __name__ == '__main__':
    main()
