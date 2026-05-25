-- DuckDB views over the dns-tracking parquet archive built by download_releases.py
--
-- Usage:
--   export ARCHIVE=$HOME/dns-tracking-archive
--   duckdb dns.duckdb -init archive.sql
--
-- Hive partitioning promotes the path's topic=/date=/hour=/sample= segments
-- into real columns, so WHERE date='…' triggers partition pruning.

SET memory_limit = '4GB';

-- Primary view: every full-shard parquet in the archive.
-- (Samples live under sample=true/ and are excluded by this glob.)
CREATE OR REPLACE VIEW observations AS
SELECT *
FROM read_parquet(
        getenv('ARCHIVE') || '/topic=*/date=*/hour=*/shard-*.parquet',
        hive_partitioning = true,
        union_by_name     = true
     );

-- Samples view (small per-shard reservoir samples), only useful if you ran
-- download_releases.py with --include-samples.
CREATE OR REPLACE VIEW samples AS
SELECT *
FROM read_parquet(
        getenv('ARCHIVE') || '/topic=*/date=*/hour=*/sample=true/shard-*.sample.parquet',
        hive_partitioning = true,
        union_by_name     = true
     );

-- File-level inventory: handy for spotting missing shards / hours.
CREATE OR REPLACE VIEW inventory AS
SELECT
    topic, date, hour,
    regexp_extract(filename, 'shard-(\d+)\.parquet$', 1)::INT AS shard,
    filename
FROM read_parquet(
        getenv('ARCHIVE') || '/topic=*/date=*/hour=*/shard-*.parquet',
        hive_partitioning = true,
        filename          = true
     );

-- ─── Sample queries ──────────────────────────────────────────────────────
--
-- Row counts per topic per (date, hour) — only reads parquet footers:
--   SELECT topic, date, hour, count(*) AS rows
--   FROM   observations
--   GROUP  BY 1, 2, 3
--   ORDER  BY date DESC, hour DESC, topic;
--
-- Drill into one snapshot:
--   SELECT * FROM observations
--   WHERE  date='2026-05-25' AND hour='00'
--     AND  topic='newly_registered_domains_measurements'
--   LIMIT  20;
--
-- Find missing shards (assuming 6 shards per topic per hour — adjust as needed):
--   SELECT topic, date, hour, count(*) AS shards
--   FROM   inventory
--   GROUP  BY 1, 2, 3
--   HAVING shards < 6
--   ORDER  BY date DESC, hour DESC, topic;
