# orderbook-lakehouse

A small local data lakehouse for market order-book events. Synthetic exchange events go through a
bronze → silver → gold pipeline and end up as tables you can run trading analytics on. It's
Scala/Spark on Apache Iceberg, with the infrastructure running in Docker.

## The stack

```
Spark job (Scala 2.13, local mode)
   │  Iceberg REST protocol: catalog + metadata
   ▼
Apache Polaris (:8181)  ── Iceberg catalog, OAuth2, in-memory
   │  S3 API
   ▼
MinIO (:9000/:9001)     ── S3-compatible storage, bucket orderbook-warehouse
```

- **`docker-compose.yml`** starts MinIO and Polaris (`docker compose up -d`). Two one-off init
  containers create the bucket and the `orderbook` catalog (`polaris-setup.sh`).
- **`PolarisSpark.scala`** builds the shared `SparkSession`. Spark asks Polaris for table metadata
  but reads and writes data files directly in MinIO.
- **Makefile** has shortcuts for checking Polaris (`make catalogs`,
  `make tables NAMESPACE=bronze`) and for running each job (`make help` lists them).

## The data

The feed has five event types: `add`, `cancel`, `modify`, `trade` and `snapshot`. Each event has
`instrument`, `timestamp`, `side`, `price`, `qty` and `seq_no` (`OrderBookSchema.scala`).
`GenerateSyntheticEvents` makes realistic demo data: it simulates a book for each instrument, so
cancels and trades hit orders that actually exist. The same seed always gives the same data.

## The pipeline

| Layer | Job | What it does |
|---|---|---|
| **Bronze** `bronze.raw_events` | `IngestRawEvents` | Reads JSON/CSV/Parquet files from a landing folder and lines them up with the bronze schema (missing columns become null, extra ones are dropped, types are cast). It records which files it has already loaded in `bronze.ingested_files`, so a rerun only picks up new files. |
| **Silver** `silver.book_events` | `BuildSilverEvents` | Drops malformed rows, removes duplicates on `(instrument, seq_no)`, adds `event_date`, then `MERGE INTO`s silver, so a rerun never inserts the same event twice. It stops before writing if more than 50% of a batch is malformed. Partitioned by instrument and date. |
| **Gold** | `BuildGoldAggregates` | Builds **1-minute OHLCV bars** (open/high/low/close/volume) from trades, and **top-of-book snapshots** (best bid and ask each minute) by keeping a running quantity for every price level. That running book is saved in `gold.book_state`, so orders from earlier runs still count. |

Full run: `make ingest && make silver && make gold`.

### Design points

- **Incremental processing** (`Watermark.scala`): each job saves the last upstream Iceberg
  snapshot ID it processed as a property on its output table. The next run reads only the changes
  since then, using `start-snapshot-id`, instead of rescanning everything.
- **Maintenance** (`MaintainTables`): runs Iceberg's compaction, snapshot expiry and orphan-file
  cleanup on every table, with configurable retention.
- **Known limitations, documented in the code:**
  - `modify` events aren't counted in the book because the schema has no `order_id`.
  - If one minute's events arrive across two runs, gold gets two partial bars for that minute.

## Analytics

Standalone query jobs, each answering one trading question:

- **Spread and cost:** `SpreadOverTime`, `EffectiveVsQuotedSpread`, `VwapVsClose`
- **Volatility:** `RealizedVolatility` (rolling standard deviation of log returns),
  `ParkinsonVolatility` (based on each bar's high/low range)
- **Microstructure signals:** `BookImbalanceVsNextBar` (does bid/ask imbalance predict the next
  bar's direction?), `LiquidityDryUpWindows`, `TradeThroughDetection`
- **Surveillance:** `CancelToAddRatio`, a classic warning sign for spoofing or quote-stuffing
- `VerifyCounts` checks row counts.

Several of these join trades against the snapshot from the previous minute, so the results are
approximate. The code says so.

## Other things to know

- **It's a demo setup, not production.** Polaris keeps its catalog in memory, so it's wiped and
  recreated on every restart. Credentials are hardcoded (`root`/`s3cr3t`, `admin`/`password`), and
  there's no STS.
- **Tests** (munit, `sbt test`) cover the transform functions (`clean`, `ohlcvBars`,
  `topOfBookSnapshots`, `conform`). Those functions don't do any I/O, so they're tested on small
  in-memory DataFrames.
- **No scheduler** is set up yet; you run jobs by hand or with an external cron.
