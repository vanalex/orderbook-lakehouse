# orderbook-lakehouse

A small local data lakehouse for market order-book events. Synthetic exchange events go through a
bronze → silver → gold pipeline and end up as tables you can run trading analytics on. It's
Scala/Spark on Apache Iceberg, with the infrastructure running in Docker.

## The stack

```
Spark job (Scala 2.13, local mode)
   │  Iceberg REST protocol: catalog + metadata
   ▼
Apache Polaris 1.8.0 (:8181) ──► Postgres 17   ── catalog state (persists across restarts)
   │  S3 API
   ▼
SeaweedFS 4.48 (:8333)        ── S3-compatible storage, bucket orderbook-warehouse
```

- **`docker-compose.yml`** starts the stack (`docker compose up -d`). Every image is pinned to a
  version. Startup order:

  | Service             | Purpose                                                                 |
  | ------------------- | ----------------------------------------------------------------------- |
  | `seaweedfs`         | `weed mini` all-in-one: S3 API on `:8333`, admin UI on `:23646`. Creates the `orderbook-warehouse` bucket and the `admin`/`password` S3 identity on startup |
  | `postgres`          | Polaris metastore. `postgres-init.sql` creates `polaris_schema` on first init |
  | `polaris-bootstrap` | One-off: Polaris admin tool creates the realm and the `root`/`s3cr3t` principal in Postgres. Idempotent |
  | `polaris`           | Iceberg REST catalog + management API on `:8181`, health on `:8182`      |
  | `polaris-init`      | One-off: `polaris-setup.sh` creates the `orderbook` catalog and grants. Idempotent (409 = already exists) |

  `docker compose down` keeps the data and the catalog; `docker compose down -v` wipes both
  together, so they never get out of sync.
- **`PolarisSpark.scala`** builds the shared `SparkSession`. Spark asks Polaris for table metadata
  but reads and writes data files directly in SeaweedFS. Endpoints and credentials come from
  environment variables (`POLARIS_*`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`; see
  `.env.example`), with defaults that match the compose stack.
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

- **It's a demo setup, not production.** Credentials are hardcoded (`root`/`s3cr3t` for Polaris,
  `admin`/`password` for S3, `polaris`/`polaris` for Postgres), and there's no STS.
- **Tests** (munit, `sbt test`) cover the transform functions (`clean`, `ohlcvBars`,
  `topOfBookSnapshots`, `conform`). Those functions don't do any I/O, so they're tested on small
  in-memory DataFrames.
- **No scheduler** is set up yet; you run jobs by hand or with an external cron.

## Setup notes

Getting Spark to commit Iceberg tables through Polaris to a non-AWS S3 store needs a few
non-obvious settings, all already in this repo:

1. **`stsUnavailable: true`** on the catalog's `storageConfigInfo` (`polaris-setup.sh`), together
   with `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION`. Polaris doesn't try to hand out STS credentials,
   and Spark uses the S3 keys directly.
2. **`AWS_ENDPOINT_URL_S3` / `AWS_ENDPOINT_URL_STS`** on the `polaris` service, pointing at
   SeaweedFS. Polaris' own internal S3 client (used for server-side commit bookkeeping) otherwise
   defaults to real AWS and fails with `403 The AWS Access Key Id you provided does not exist`.
3. **Virtual-hosted-style bucket addressing.** Polaris' internal client addresses buckets as
   `<bucket>.<host>` regardless of `pathStyleAccess`. `-s3.domainName=seaweedfs` makes SeaweedFS
   accept that style, and the network alias `orderbook-warehouse.seaweedfs` makes the hostname
   resolve to the container.
4. **S3A for `remove_orphan_files`.** That maintenance procedure lists files through Hadoop's
   `FileSystem` API rather than `S3FileIO`, so `PolarisSpark` also registers `S3AFileSystem`
   (`hadoop-aws`) for the `s3://` scheme against the same endpoint.
5. **Polaris + Postgres.** Polaris doesn't create its database schema (`postgres-init.sql` does),
   and with JDBC storage the realm and root principal come from the admin tool's `bootstrap`
   command, not from `POLARIS_BOOTSTRAP_CREDENTIALS`. Keep `apache/polaris` and
   `apache/polaris-admin-tool` on the same version when upgrading. Polaris doesn't migrate its
   schema automatically, so read the release notes first.
