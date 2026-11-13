# orderbook-lakehouse

A small local data lakehouse for market order-book events. Synthetic exchange events go through a
bronze → silver → gold pipeline and end up as tables you can run trading analytics on. It's
Scala/Spark on Apache Iceberg, with the infrastructure running in Docker.

For a step-by-step walkthrough of the demo, see [GUIDE.md](GUIDE.md).

## The stack

```
Producer (make generate) ──HTTP──► Redpanda Connect 4.113 (:4195)
                                      │  NDJSON files → s3://orderbook-warehouse/landing/raw_events/
                                      ▼
Spark job (Scala 2.13, local mode) ◄──┘  reads landed files, writes Iceberg tables
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
  | `postgres`          | Polaris metastore. `infra/postgres/init.sql` creates `polaris_schema` on first init |
  | `polaris-bootstrap` | One-off: Polaris admin tool creates the realm and the `root`/`s3cr3t` principal in Postgres. Idempotent |
  | `polaris`           | Iceberg REST catalog + management API on `:8181`, health on `:8182`      |
  | `polaris-init`      | One-off: `infra/polaris/setup.sh` creates the `orderbook` catalog and grants. Idempotent (409 = already exists) |
  | `redpanda-connect`  | The only way data comes in: accepts events over HTTP on `:4195` and lands them in S3 (see [Ingest](#ingest-redpanda-connect)) |

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
`GenerateSyntheticEvents` (`make generate`) makes realistic demo data and POSTs it to Redpanda
Connect, the same way any other producer would. It runs a small matching
engine for each instrument. Orders that cross the book trade against resting orders (best price
first), market orders hit the best bid or ask, and cancels remove orders that actually exist, so
the book gold rebuilds is never crossed. Events are 1s apart by default (`GEN_INTERVAL`), so 5000
events cover about 80 minutes of one-minute bars. It doesn't emit `modify`, because gold can't
apply one without an `order_id`. The same seed always gives the same data.

## The pipeline

| Layer | Job | What it does |
|---|---|---|
| **Bronze** `bronze.raw_events` | `IngestRawEvents` | Reads the NDJSON files Redpanda Connect landed in S3 and lines them up with the bronze schema (missing columns become null, extra ones are dropped, types are cast). It records which files it has already loaded in `bronze.ingested_files`, so a rerun only picks up new files. |
| **Silver** `silver.book_events` | `BuildSilverEvents` | Drops malformed rows, removes duplicates on `(instrument, seq_no)`, adds `event_date`, then `MERGE INTO`s silver, so a rerun never inserts the same event twice. It stops before writing if more than 50% of a batch is malformed. Partitioned by instrument and date. |
| **Gold** | `BuildGoldAggregates` | Builds **1-minute OHLCV bars** (open/high/low/close/volume) from trades, and **top-of-book snapshots** (best bid and ask each minute) by keeping a running quantity for every price level. That running book is saved in `gold.book_state`, so orders from earlier runs still count. |

From scratch: `make setup && make generate && make ingest && make silver && make gold`.
`make setup` (re)creates every table empty and deletes the landed files (`make clear-landing`), so
it also resets the pipeline on a running stack.

The Scala packages under `src/main/scala/orderbook/` follow the same layers:

| Package | Contents |
|---|---|
| `common` | `PolarisSpark` (Spark session wired to Polaris + SeaweedFS), `OrderBookSchema`, `Watermark` |
| `bronze` / `silver` / `gold` | Each layer's `Create*Table(s)` job and the job that writes it |
| `analytics` | Read-only query jobs (see [Analytics](#analytics)) |
| `ops` | `CreateNamespaces`, `MaintainTables`, `ClearLanding`, `SmokeTest`, `ListCatalogs` |
| `tools` | `GenerateSyntheticEvents` |

Tests mirror the same packages under `src/test/scala/orderbook/`.

### Ingest (Redpanda Connect)

Events only come in over HTTP. The `redpanda-connect` service (`infra/redpanda-connect/landing.yaml`)
accepts them and writes them to `s3://orderbook-warehouse/landing/raw_events/` as newline-delimited
JSON files, one file per batch (up to 5000 events or 5s). `make ingest` then runs
`IngestRawEvents` against that prefix (`STREAM_LANDING`) to append the new files to bronze. Bronze
loading is micro-batch: events land continuously, and each `make ingest` moves what has landed
into Iceberg.

```sh
make generate                    # POSTs the first 5000 synthetic events of the feed
make generate-next               # POSTs the next 5000: new seq_no, later timestamps
make demo-dedup                  # re-sends the last batch, then ingest + silver: all skipped
make snapshots TABLE=silver.book_events   # snapshot history + watermark, from Polaris
make pointers                    # each table's metadata.json pointer, from Postgres
curl --data-binary '{"event_type":"add","instrument":"BTC-USD","timestamp":"2026-10-26T10:00:00Z","side":"buy","price":100.5,"qty":3,"seq_no":1}' \
  http://localhost:4195/events   # or send your own
make ingest && make silver && make gold
```

A request body can hold one event or many, one per line. The request returns once the events
are written to S3. Lines that aren't valid JSON go to `landing/rejected/` instead, because one
unparseable row would make the bronze append fail. Delivery is at least once; silver's dedupe on
`(instrument, seq_no)` removes any duplicates.

### Design points

- **Incremental processing** (`Watermark.scala`): each job saves the last upstream Iceberg
  snapshot ID it processed as a property on its output table. The next run reads only the changes
  since then, using `start-snapshot-id`, instead of rescanning everything.
- **Maintenance** (`MaintainTables`): runs Iceberg's data-file and manifest compaction, snapshot
  expiry and orphan-file cleanup on every table, with configurable retention.
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

1. **`stsUnavailable: true`** on the catalog's `storageConfigInfo` (`infra/polaris/setup.sh`), together
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
5. **Polaris + Postgres.** Polaris doesn't create its database schema (`infra/postgres/init.sql` does),
   and with JDBC storage the realm and root principal come from the admin tool's `bootstrap`
   command, not from `POLARIS_BOOTSTRAP_CREDENTIALS`. Keep `apache/polaris` and
   `apache/polaris-admin-tool` on the same version when upgrading. Polaris doesn't migrate its
   schema automatically, so read the release notes first.
