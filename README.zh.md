# dns-tracking

[English README](README.md) · 中文版（本文档）

长期跟踪新注册域名的 DNS 状态（IP / NS / MX / AS / 国家），数据源自
[OpenINTEL Zonestream][1] 的反应式测量 Avro 流（[DarkDNS][2], IMC '24）。

[1]: https://openintel.nl/data/zonestream/
[2]: https://arxiv.org/abs/2405.12010

## 这是什么

OpenINTEL 在 `kafka.zonestream.openintel.nl:9092`（匿名 PLAINTEXT 公共 broker）
发布若干 topic，我们消费其中 4 个：

- `newly_registered_domains_measurements`   — ~221M 消息/天，Avro
- `newly_registered_fqdn_measurements`      — ~107M 消息/天，Avro
- `newly_issued_certificates_measurements`  —  ~82M 消息/天，Avro
- `certstream_domains`                      — CT log → 域名映射，JSON

3 个 Avro topic 承载每次 DNS 主动测量的原始结果；`certstream_domains`
承载 CT log entry → 域名清单的映射，把 cert 事件跟域名关联起来。这 4 个一
起喂进我们的统一观测流。

姊妹项目 `zonestream-archive` 归档上游的小体量 JSON topic
（`newly_registered_domain` 等）；本仓库专门处理大体量测量流：在
GitHub Actions（多 Gbps 网络）上流式消费，并把每条原始记录聚合成紧凑的
Parquet 文件。

## 数据存哪里

**每 12 小时一个 Release**，tag 为 `snap-YYYY-MM-DD-HH`，其中
`HH ∈ {00, 12}` 标记窗口结束时间。每个 topic 的每个 shard 上传一个 Parquet 文件：

```
snap-2026-05-25-00/
  newly_registered_domains_measurements.shard-0.parquet  (~80 MB, 去重后)
  newly_registered_domains_measurements.shard-1.parquet
  …
  newly_registered_fqdn_measurements.shard-0.parquet     (~30 MB)
  …
  newly_issued_certificates_measurements.shard-0.parquet (~200 MB)
  …
  certstream_domains.shard-0.parquet                     (~1-2 MB)
  certstream_domains.shard-1.parquet
```

所有 Parquet 文件用 **zstd-3 压缩 + 字典编码**（Parquet 字符串列默认开启）。
单个 12h 窗口大致 1 GB，跨 20 个 shard 文件。

### Schema 1：Avro DNS topic — `consume_group.py` 输出

3 个 `*_measurements` topic 共享同一个 16 列观测 schema。每行 = 这个窗口内
看到的一个唯一 `(kind, key)` 三元组，附 `first_ts` / `last_ts` / `n` 聚合字段
（DarkDNS 在每个 (domain, IP) 上发起 ~10-300 次重复查询，都被这里折叠成一行）。

| 列名 | 类型 | 含义 |
|---|---|---|
| `k` | string | 记录类型：`ip` / `ns` / `ns_ip` / `mx`（**查询前务必先 filter `k`**）|
| `topic` | string | `domains` / `fqdn` / `certs`（简写标签）|
| `d` | string | 候选域名（`k='ns_ip'` 时为 NULL）|
| `s` | string | NS 主机名（`k='ns'` 或 `'ns_ip'`）|
| `m` | string | MX 主机名（`k='mx'`）|
| `q` | string | DNS qtype `A` 或 `AAAA`（`k='ip'` 或 `'ns_ip'`）|
| `ip` | string | IPv4/v6 地址（`k='ip'` 或 `'ns_ip'`）|
| `a` | string | ASN 十进制字符串（OpenINTEL 原生格式）|
| `c` | string | ISO 3166-1 alpha-2 国家码 |
| `p` | string | IP 前缀（CIDR 形式）|
| `pr` | int64 | MX preference（`k='mx'`）|
| `tl` | int64 | TTL（response_ttl，秒）|
| `fs` | int64 | first_ts（ms epoch；窗口内最早的 broker_ts）|
| `ls` | int64 | last_ts（ms epoch；最晚的）|
| `n`  | int64 | 这一行代表的原始观察次数 |
| `rc` | int64 | DNS rcode（仅非零时设值）|

NULL 用来区分 kind 子集 —— 例如 `k='ns'` 行 `s` 有值，`ip`/`q`/`a`/`c`/`p`
都是 NULL。**查询时永远先按 `k` 过滤**，否则会跨 kind 误聚合。

#### kind 派生规则与 NULL 分布

`k` 列不是 broker 给的，是 `consume_group.py` 根据 `query_type` 和
"qname 是否等于主域" 派生出来：

| `query_type` | `qname == 主域` | 派生 `k` | 含义 |
|---|---|---|---|
| `A` / `AAAA` | 是 | `ip` | 主域解析到的 IP |
| `A` / `AAAA` | 否 | `ns_ip` | 该测量返回的某个 NS 主机自己的 IP |
| `NS` | 是 | `ns` | 主域的 NS 委派 |
| `MX` | 是 | `mx` | 主域的 MX 记录 |

去重 key 的形态因 kind 而异（每个唯一 key 在窗口内对应输出一行）：

- `ip`:    `(kind, topic_tag, domain, qtype, ip, rcode)`
- `ns_ip`: `(kind, topic_tag, ns_hostname, qtype, ip, rcode)` —— 此时 `d`
  列为 NULL，因为这条记录挂在 NS 主机上而不是某个域名上
- `ns`:    `(kind, topic_tag, domain, ns_hostname, rcode)`
- `mx`:    `(kind, topic_tag, domain, mx_hostname, mx_preference, rcode)`

第二次起出现相同 key 时只更新聚合字段：`fs = min(原 fs, ts)`、
`ls = max(原 ls, ts)`、`n += 1`。所以 `n` 只反映**本 12h 窗口**的重复
次数，不是 DarkDNS 完整 48h 测量周期的总次数。

按 kind 的非 NULL 列速查：

| kind | 非 NULL 列 |
|---|---|
| `ip` | `d, q, ip, a, c, p, tl, fs, ls, n`（+ 非零 `rc`）|
| `ns_ip` | `s, q, ip, a, c, p, tl, fs, ls, n`（**`d` 为 NULL**）|
| `ns` | `d, s, tl, fs, ls, n` |
| `mx` | `d, m, pr, tl, fs, ls, n` |

`a` / `c` / `p`（ASN / 国家 / 前缀）是 OpenINTEL 上游测量管线就富化好的，
本仓库不做 GeoIP 查询，直接透传。

### Schema 2：`certstream_domains` — `consume_json.py` 输出

CT log entry → 域名清单映射。一行 = 一条 CT log entry。

| 列名 | 类型 | 含义 |
|---|---|---|
| `certIndex` | int64 | CT log 内部 entry 索引 |
| `seen` | int64 | 观察到 CT entry 的 unix 秒 |
| `submission_timestamp` | int64 | 证书提交到 CT log 的 unix 秒 |
| `source` | string | CT log 名（如 `Sectigo 'Tiger2026h2'`）|
| `updateType` | string | `X509LogEntry` 或 `PrecertLogEntry` |
| `fingerprint` | string | SHA1 hex `AA:BB:...`（每张证书唯一）|
| `domain_list` | list&lt;string&gt; | 证书里全部域名（CN + SAN）|
| `sld_list` | list&lt;string&gt; | 从 `domain_list` 派生的去重二级域 |

未知字段（schema 漂移）会用 `WARN` 日志一次然后丢弃；shard 统计里的
`unknown_fields_seen` 字段会列出来 —— 看到了就更新
`scripts/consume_json.py` 的 schema。

### 跨 schema join 主键

4 个 topic 之间能用这些字段串起来：
- **`fingerprint`** —— certstream_domains 有；Avro 测量记录里也有（在原始
  `MeasurementResult.id` 里）。
- **`certIndex`** —— certstream_domains 有；Avro 记录的 `cert_index` 字段
  里也有（CT-触发的测量）。
- **`domain`** —— Avro 观测里是 `d` 列；certstream_domains 里在
  `domain_list` / `sld_list` 里。

本仓库的 Avro 提取只保留候选 domain（`d` 列），不保留每条 record 的
`cert_index`。要回溯到具体 CT entry，按 `domain` 在 certstream_domains 里查。

## 工作原理

`consume.yml` 每天 **00:30 和 12:30 UTC** 双跑（也支持手动 dispatch）。
每次跑消费一个 12 小时窗口，结束时间对齐到最近一个 12h UTC 边界，所以
覆盖始终在 broker ~24h 保留期之内。Matrix 策略，每个 topic 的 shard 数
做过调整，让所有 topic 的总墙钟接近：

| topic | format | shards | 每 shard 输出 |
|---|---|---:|---:|
| `newly_issued_certificates_measurements` | `avro_dns` | 3 | ~200 MB |
| `newly_registered_fqdn_measurements` | `avro_dns` | 4 | ~30 MB |
| `newly_registered_domains_measurements` | `avro_dns` | 11 | ~80 MB |
| `certstream_domains` | `json` | 2 | ~1-2 MB |

共 **20 个并行 job**（GH Free 公开仓库并发上限）。Topic 配置在
[`.github/shards.json`](.github/shards.json)，`consume.yml` 的 `plan` job
和 `retry.yml` 都读它。

`format` 字段选哪个 consumer 脚本；两者都输出 Parquet：

- **`avro_dns`** → `scripts/consume_group.py` 解 Avro `MeasurementResult`
  记录，按上面那张 16 列 schema 聚合观测。
- **`json`** → `scripts/consume_json.py` 把 JSON 负载按 `TOPIC_SCHEMAS`
  里登记的 per-topic schema 解析。

每个 shard 拿 topic offset 区间的 `1/N`，用 `assign()+seek()`（不是
`subscribe()+commit()`）—— group id 一个 shard 一个，**只是当 broker 端
fetch 配额的标签**。**Broker 配额是 per-connection 而非按 consumer group
全局共享**，所以多 shard 就是线性提速（实测可拉到单 consumer 的 ~10×）。

`--clobber` 上传，partial-day 重跑不会重复。

### 每天的完整时序

四个 workflow 排成一条互不冲突的链：

```
00:30 UTC  consume.yml   →  snap-DAY-00（窗口：前一天 12:00 → 今天 00:00）
04:30 UTC  retry.yml     →  扫 snap-DAY-00，缺哪个 shard 单独重发
06:00 UTC  cleanup.yml   →  删 30 天前的 snap-* release + tag
12:30 UTC  consume.yml   →  snap-DAY-12（窗口：今天 00:00 → 今天 12:00）
16:30 UTC  retry.yml     →  扫 snap-DAY-12
```

cleanup 卡在两次 consume 中间执行，所以永远不会和正在写的 release 撞车。
`consume.yml` 还配了 `concurrency: { group: consume, cancel-in-progress: false }`，
保证排队的手动 dispatch 不会和正在跑的 cron 互相覆盖。

### Shard offset 切片

每个 shard 跑这套流程（见 `consume_group.compute_shard_range`）：

1. 用 `offsets_for_times([start_ms, end_ms])` 把时间窗翻译成 broker offset
   区间 `[start_off, end_off]`。end_ms 超出 retention 时回落到 high-watermark。
2. `chunk = (end_off - start_off) // shard_count`；shard `i` 拿
   `[start_off + i*chunk, start_off + (i+1)*chunk)`；最后一片把余数吃掉。
3. `assign() + seek()` 到 shard 起点，poll 到 `msg.offset() >= shard_end`
   退出。**不 commit offset** —— group id 一个 shard 一个，仅作为 broker
   端 fetch 配额的标签。

broker 限流是 **per-connection**，不是按 cluster 级 consumer group 全局
共享。所以 shard 数线性加带宽 —— 单 consumer ~1100 msg/s 的服务端上限
被 11 个 shard 拉到 ~10×。这就是为什么 `newly_registered_domains_measurements`
（221M 消息/天）能在一个 5h GH job 里跑完。

其它退出条件（任一触发即结束并落盘）：

- `--run-seconds`（默认 18000s，留在 `runs-on: ubuntu-latest` 6h 上限内）
- `--max-msgs`（默认 0 = 不限）
- `--idle-exit-seconds`（默认 30s，无消息提前退）

落盘走 `try / finally` —— 任何退出（包括异常）都会把内存里的 dedup dict
一次性写成合法 Parquet（带 footer）。Parquet 的 footer 是 all-or-nothing 的，
要么完整要么文件不存在，下游 glob 不会读到半截文件。失败的 shard 留给
`retry.yml` 下一轮自动补。

### 为什么用 Parquet（替代 gzipped JSONL）

仓库以前输出 gzipped JSONL，后来切到 Parquet zstd-3，是基于在相同数据上
实测的加速：

| 查询 | gunzip+grep | gunzip+python | duckdb+Parquet | 加速 |
|---|---:|---:|---:|---:|
| 点查 1 个 domain (6 shards) | 4.5s | 111.6s | **0.36s** | 310× vs python |
| Top-15 hosting ASN（聚合）| n/a | 112.2s | **0.26s** | 430× |
| NS 跨 18 shards (~2 GB gz) | n/a | 464.9s (7m38s) | **0.35s** | **1300×** |

avro_dns topic 文件还小了约 34%。`certstream_domains` 只小了 11%（因为多
数字节是高熵 fingerprint hex，gzip/zstd 都压不动）—— 但**格式一致让跨
topic JOIN 一行 SQL 搞定**。

### 自动补漏

`retry.yml` 每天 **04:30 和 16:30 UTC** 跑 —— 跟在 consume 4 小时之后。
自动识别最近一个 12h 边界（跟 `consume.yml` 一样的逻辑），扫描对应
`snap-YYYY-MM-DD-HH` Release 期望有的 asset，缺哪个就 dispatch
`consume.yml`，`targets` input 列出缺失的
`(topic, shard, shard_count, format)`，再加上 `day` 和 `hour` 输入锁定
窗口（防止排队的 retry 跨越边界时打错窗口）。手动 dispatch 支持
`dry_run`，只打印缺失列表不发起补跑。

### 滚动清理

`cleanup.yml` 每天 **06:00 UTC** 跑（卡在 `retry.yml` 之后、下一次
`consume.yml` 之前，不会跟还在写入的 release 撞车）。它列出所有匹配
`snap-YYYY-MM-DD-HH` 的 tag，解析里头的日期，把超过 **30 天**的
release 全部删掉 —— GitHub Release（asset + 页面）和对应的 git tag 一起
清（`gh release delete --cleanup-tag`）。非 `snap-*` 的 tag 不动。手动
dispatch 支持 `keep_days`（默认 `30`）和 `dry_run`（默认 `false`），后者
只打印会被删的列表，不实际动手。

## 查询

推荐用 [DuckDB](https://duckdb.org)：原生读 Parquet，支持 HTTP 直读
（`httpfs`）、自动 multi-file glob、predicate pushdown 跳过无关 row group。

### 单窗口查询

```sql
-- 单窗口的 hosting ASN 分布
duckdb -c "
SELECT a AS asn, COUNT(*) AS rows
FROM 'snap-2026-05-25-00/newly_registered_domains_measurements.shard-*.parquet'
WHERE k='ip'
GROUP BY a ORDER BY rows DESC LIMIT 15"

-- IP 切换最频繁的域名（IP-churn 信号）
duckdb -c "
SELECT d AS domain, COUNT(DISTINCT ip) AS distinct_ips
FROM 'snap-2026-05-25-00/newly_registered_domains_measurements.shard-*.parquet'
WHERE k='ip' AND d IS NOT NULL
GROUP BY d ORDER BY distinct_ips DESC LIMIT 20"

-- 跨 topic JOIN：找 CT 发证的域名同时被我们测量到的
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

### 跨日查询 / 纵向跟踪

```sql
-- 找昨天到今天 hosting AS 切换的域名
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

-- 多窗口 glob 扫描 (filename=true 加一列来源路径)
duckdb -c "
SELECT filename, d, ip, first_ts, n
FROM read_parquet('snap-2026-05-*/newly_registered_domains_measurements.shard-*.parquet', filename=true)
WHERE d = 'example.com' AND k = 'ip'
ORDER BY first_ts"
```

### 直接从 GitHub Releases 查（不下载）

DuckDB 的 `httpfs` 扩展可以直接 query Release asset 的远端 URL：

```sql
duckdb -c "
INSTALL httpfs; LOAD httpfs;
SELECT COUNT(*) FROM
  'https://github.com/wangmm001/dns-tracking/releases/download/snap-2026-05-25-00/newly_registered_domains_measurements.shard-0.parquet'
WHERE k='ip'"
```

### 镜像到本地归档

仓库只保留最近 30 天的 release（见上面"滚动清理"）。想长期留存、或者要
频繁重复跑大查询，就把数据镜像到本地。`archive/download_releases.py` 把
parquet 资产放成 Hive 分区目录树，DuckDB 不需要手写 glob 就能直接查：

```
$ARCHIVE/topic=<topic>/date=YYYY-MM-DD/hour=HH/shard-<N>.parquet
```

**无需 GitHub 登录**（走匿名 REST API）。**幂等可补漏**：每次跑都扫描全部
`snap-YYYY-MM-DD-HH` release 和本地文件按字节数比对，**只下本地缺的**。
所以单条 cron 就够 —— 漏跑的下次自动补上。下载走 `<file>.part` + 原子
rename，跑到一半被杀也不会留下"看起来完整其实是半截"的文件。

```bash
export ARCHIVE=$HOME/dns-tracking-archive

# 全量镜像（或后续跑作为增量补全）
python3 archive/download_releases.py

# 冷启动：把前几周历史分多次跑，摊薄带宽
python3 archive/download_releases.py --max-releases 2

# 限定 topic / 时间窗
python3 archive/download_releases.py \
  --topic newly_registered_domains_measurements --since 2026-05-20
```

配套的 `archive/archive.sql` 给本地归档注册好 DuckDB 视图
（`observations` / `samples` / `inventory`），可以直接把 `topic` / `date`
/ `hour` 当真实列来过滤：

```bash
duckdb dns.duckdb -init archive/archive.sql -c "
  SELECT topic, date, hour, count(*) AS rows
  FROM   observations
  GROUP  BY 1, 2, 3
  ORDER  BY date DESC, hour DESC, topic"
```

Crontab（每小时一次；漏跑下次自动补）：

```cron
17 * * * * ARCHIVE=$HOME/dns-tracking-archive /usr/bin/python3 \
  /path/to/dns-tracking/archive/download_releases.py \
  >> $HOME/dns-tracking-archive/_logs/cron.log 2>&1
```

冷启动时可设 `GH_TOKEN`（或 `GITHUB_TOKEN`），把匿名 60 req/h 限速
提升到 5000 req/h。

## 本地消费

不走 GitHub Actions 的临时消费：

```bash
# 缓存 Avro schema (一次性)
curl -sS http://schema.zonestream.openintel.nl/subjects/newly_registered_domains_measurements/versions/latest \
  | jq -r .schema > schemas/measurement_id_1.json

# Python 依赖
pip install fastavro confluent-kafka pyarrow

# 拉 domains topic 一个 shard 的最近 1h
NOW=$(($(date +%s) * 1000))
python scripts/consume_group.py \
    --topic newly_registered_domains_measurements \
    --group local-test-$$ \
    --start-ms $((NOW - 3600000)) --end-ms $NOW \
    --shard-index 0 --shard-count 1 \
    --max-msgs 100000 \
    --out-obs /tmp/local.parquet

# 查
duckdb -c "SELECT k, COUNT(*) FROM '/tmp/local.parquet' GROUP BY k"
```

`scripts/consume_json.py` 同样用法用于 `certstream_domains`。

## 修改 / 扩展

- **调整 shard 数** —— 改 `.github/shards.json`。总数 ≤ 20（GH Free 并发
  上限），并且每个 shard 工作量得能在单 job 6h 内跑完。
- **新增一个 Avro DNS topic** —— 在 `.github/shards.json` 里加，
  `format: avro_dns`。该 topic 在上游 schema registry 的 Avro schema
  必须是 id=1（其他 id 需要改 schema 缓存逻辑）。
- **新增一个 JSON topic** —— 在 `.github/shards.json` 里加，
  `format: json`，然后在 `scripts/consume_json.py` 顶部的 `TOPIC_SCHEMAS`
  里加一条 schema 定义。字段类型用 pyarrow type：`pa.string()`,
  `pa.int64()`, `pa.list_(pa.string())` 等。
- **已有 JSON topic 出现 schema 漂移** —— 未知字段会被 WARN 日志一次，
  shard 统计里 `unknown_fields_seen` 列出。看到了就更新 `TOPIC_SCHEMAS`
  重跑。

## 历史数据兼容

切到 Parquet 之前的 release（`snap-2026-05-23` 及更早）里是 `.jsonl.gz`
文件，用旧的 record-per-line JSON schema。DuckDB 两种格式都能读 ——
`read_json('*.jsonl.gz')` 和 `read_parquet('*.parquet')` 都行，需要的话
一句 `UNION` 串起来。**老数据不会自动转格式**；要全 Parquet 的话，手动
`workflow_dispatch` 带 `day`/`hour` 输入重跑那个窗口即可。

## 注意事项

- DarkDNS 设计上对每个域名 48 小时内做 ~288 轮主动测量，但**实测是 burst
  调度**（峰值在 CT 检测后 ~7h 和 ~37h，不是文字面的每 10 分钟一次）。
  观测记录里的 `n` 列只表示**这个窗口内**的重复次数，不代表完整 48h 历史。
- ASN + GeoIP 富化数据来自 OpenINTEL 测量 pipeline，不是本仓库加的 ——
  `a` 和 `c` 列直接从 Avro 源拿过来。
- **ccTLD 在上游几乎完全缺失** —— OpenINTEL 只能看到 ICANN CZDS 收录的
  TLD（基本上全部 gTLD）加上 `.ch` / `.li`。主流 ccTLD（`.de`, `.uk`,
  `.fr`, `.cn`, `.jp` 等）在这里看不到。详见姊妹仓库 `zonestream-archive`
  的 README。
- DarkDNS 的 `measurement_node` 字段显示所有观察都来自**单一物理节点**
  （OpenINTEL 在荷兰的 `enschede-01`）上的 16 个并行 worker 进程 ——
  论文里写的 "16 nodes" 其实是 worker instance 数，不是地理分布的
  vantage point。**不能用这数据做地理偏置 / cloaking 研究**。
