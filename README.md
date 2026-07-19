# orderbook-lakehouse

A demo lakehouse for order-book data, built on the **Apache Iceberg** table format with an
**Apache Polaris** REST catalog and **MinIO** (S3-compatible) object storage. The application
layer is a Scala 2.13 project using **Apache Spark** (local mode) to read/write Iceberg tables.

> Status: infrastructure bootstrapped, Spark wired to the Polaris REST catalog. The
> `bronze`/`silver`/`gold` namespaces exist and table reads/writes are verified end to end —
> the ingestion/transform jobs themselves are the next step.

## Architecture

```
┌──────────────┐      Iceberg REST       ┌──────────────┐      S3 API      ┌──────────────┐
│  Spark job   │ ──────────────────────► │   Polaris    │ ───────────────► │    MinIO     │
│  (Iceberg    │   catalog + metadata    │  REST catalog│   data + metadata│  object store│
│   client)    │                         │  :8181       │                  │  :9000/:9001 │
└──────────────┘                         └──────────────┘                  └──────────────┘
```

- **MinIO** — S3-compatible object storage that backs the Iceberg warehouse
  (bucket `orderbook-warehouse`).
- **Polaris** — Iceberg REST catalog that manages namespaces, tables, and metadata,
  storing data files in MinIO.
- **Scala/Spark app** — connects to Polaris over the Iceberg REST protocol, using Spark SQL to
  read/write tables (see `example.PolarisSpark`).

## Prerequisites

- Docker + Docker Compose
- JDK 17+ (tested on 21) and [sbt](https://www.scala-sbt.org/) (for the Scala/Spark app) —
  Spark needs `--add-opens` JVM flags to run on Java 17+, which `build.sbt` sets up via `fork`

## Quickstart

Bring up the full stack:

```sh
docker compose up -d
```

Compose starts the services in order and runs two one-shot init containers:

| Service        | Purpose                                                        |
| -------------- | -------------------------------------------------------------- |
| `minio`        | S3 object store — API on `:9000`, web console on `:9001`       |
| `minio-init`   | Creates the `orderbook-warehouse` bucket, then exits           |
| `polaris`      | Iceberg REST catalog + management API on `:8181`, health `:8182` |
| `polaris-init` | Runs `polaris-setup.sh` to create the `orderbook` catalog, then exits |

When `polaris-init` prints
`Polaris ready: catalog 'orderbook' -> s3://orderbook-warehouse (MinIO)`, the lakehouse is ready.

Check status and logs:

```sh
docker compose ps -a
docker compose logs polaris
docker compose logs polaris-init
```

Tear down (add `-v` to also drop the MinIO data volume):

```sh
docker compose down        # keep warehouse data
docker compose down -v     # wipe everything
```

### Endpoints & credentials

| What                 | Endpoint                       | Credentials                     |
| -------------------- | ------------------------------ | ------------------------------- |
| MinIO S3 API         | http://localhost:9000          | `admin` / `password`            |
| MinIO console        | http://localhost:9001          | `admin` / `password`            |
| Polaris catalog/mgmt | http://localhost:8181          | OAuth2 client credentials       |
| Polaris health       | http://localhost:8182/q/health | —                               |
| Iceberg warehouse    | `s3://orderbook-warehouse`     | —                               |

Polaris root principal (realm `default-realm`): client id `root`, secret `s3cr3t`.

Get an access token:

```sh
curl -s -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
  -H "Polaris-Realm: default-realm" \
  -d grant_type=client_credentials \
  -d client_id=root -d client_secret=s3cr3t \
  -d scope=PRINCIPAL_ROLE:ALL
```

## What `polaris-setup.sh` does

Run automatically by the `polaris-init` container against the in-container Polaris
(`http://polaris:8181`):

1. Requests an OAuth token as the `root` principal.
2. Creates the `orderbook` catalog (type `INTERNAL`) pointing at `s3://orderbook-warehouse`,
   using MinIO as the S3 endpoint with path-style access.
3. Grants the auto-created `catalog_admin` role `CATALOG_MANAGE_CONTENT` and attaches it to the
   `service_admin` principal role (which `root` holds), giving `root` full access to the catalog.

## Inspecting Polaris (Makefile)

The `Makefile` wraps read-only ("get") queries against the running Polaris catalog. Each target
fetches a fresh OAuth token as `root` and pretty-prints the JSON response (requires `curl` + `jq`).

```sh
make help              # list all targets
make health            # Polaris health endpoint
make token             # print an access token
make catalogs          # list all catalogs
make catalog           # get the orderbook catalog
make catalog-roles     # list catalog roles
make principals        # list principals
make principal-roles   # list principal roles
make namespaces        # list namespaces in the catalog
make tables NAMESPACE=bronze  # list tables in a namespace
```

Override defaults via variables, e.g. `make catalog CATALOG=orderbook` or
`make catalogs POLARIS=http://localhost:8181`.

## The Scala app

```sh
sbt compile   # build
sbt test      # run tests (munit)
```

Configuration lives in `build.sbt` (Scala 2.13.16, project `orderbook-lakehouse`).

### Jobs

- **`example.ListCatalogs`** — connects to the Polaris management API and lists the registered
  catalogs. Uses `requests-scala` + `upickle` for HTTP/JSON.

  ```sh
  sbt "runMain example.ListCatalogs"
  # or via the Makefile (passes the demo config through as env vars):
  make scala-catalogs
  ```

  Config is read from the environment: `POLARIS_URL`, `POLARIS_REALM`, `POLARIS_CLIENT_ID`,
  `POLARIS_CLIENT_SECRET` (defaults match the demo stack).

  To override without exporting vars by hand, copy the sample env file — `make scala-catalogs`
  sources `.env` (if present) into the environment before running the job:

  ```sh
  cp .env.example .env   # then edit as needed
  make scala-catalogs
  ```

  `.env` is gitignored. The JVM has no built-in `.env` support, so the Makefile's shell loads it;
  running `sbt "runMain example.ListCatalogs"` directly won't pick it up unless you source it
  yourself (`set -a; . ./.env; set +a`).

- **`example.PolarisSpark`** — not a job itself, but the shared `SparkSession` factory every
  Spark job builds on. Configures Spark's Iceberg REST catalog (`spark.sql.catalog.orderbook`)
  to talk to Polaris for metadata and directly to MinIO (via `S3FileIO`) for data files. Config
  is read from the environment, extending the `ListCatalogs` vars with:
  `POLARIS_CATALOG`, `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `AWS_REGION`.

- **`example.CreateNamespaces`** — sanity-check job for the Spark ↔ Polaris wiring: creates the
  `bronze`/`silver`/`gold` namespaces the medallion pipeline will use and lists what's in the
  catalog afterward.

  ```sh
  sbt "runMain example.CreateNamespaces"
  # or:
  make spark-init-namespaces
  ```

- **`example.SmokeTest`** — end-to-end check of the full data path: creates a real Iceberg
  table, writes rows, reads them back, then drops the table. This is what actually needed the
  storage/network fixes below — `CreateNamespaces` only exercises catalog metadata, not MinIO.

  ```sh
  sbt "runMain example.SmokeTest"
  # or:
  make spark-smoke-test
  ```

- **`example.OrderBookSchema`** — not a job, but the order-book event schema (Phase 1 of
  `data_pipeline_plan.md`): the raw feed's five event kinds (`add`, `cancel`, `modify`, `trade`,
  `snapshot`) and the bronze `StructType` for `orderbook.bronze.raw_events` (append-only,
  minimal typing — silver is where types get validated/cast, per Phase 4).

- **`example.CreateBronzeTable`** — creates `orderbook.bronze.raw_events` and
  `orderbook.bronze.ingested_files` (Phase 3's file-tracking table, see `IngestRawEvents` below)
  with the schemas from `OrderBookSchema`. Rerunnable (`createOrReplace`).

  ```sh
  sbt "runMain example.CreateBronzeTable"
  # or:
  make spark-create-bronze-table
  ```

- **`example.GenerateSyntheticEvents`** — Phase 3's source for demo data: simulates a toy order
  book per instrument (`add` pushes a resting order onto a list; `cancel`/`modify`/`trade` pick a
  real resting order off that list, so the feed is internally consistent rather than pure random
  noise) and writes the result to a landing path/format — it doesn't touch the Iceberg table
  itself, the same as a historical replay dump wouldn't. Deterministic given a seed (default
  `42L`) and a fixed epoch anchor, not wall-clock time.

  ```sh
  sbt "runMain example.GenerateSyntheticEvents [count] [instruments] [path] [format]"
  # e.g.:
  sbt "runMain example.GenerateSyntheticEvents 5000 BTC-USD,ETH-USD data/raw_events json"
  ```

- **`example.IngestRawEvents`** — Bronze ingestion (Phase 3 of `data_pipeline_plan.md`): reads
  raw feed files from `SOURCE_PATH`/`SOURCE_FORMAT` (or CLI args), conforms them to
  `OrderBookSchema.bronzeRawEvents` (`conform`: keeps known columns, adds missing ones as null,
  casts to bronze types — tolerant of extra/missing columns and source-specific typing), and
  appends to `orderbook.bronze.raw_events`.

  Tracks which source files it's already ingested in `orderbook.bronze.ingested_files` (one row
  per file path, from Spark's `input_file_name()`), so a run only reads/appends files not already
  recorded there (`IngestRawEvents.newFilesOnly`, a left-anti join against that table) — re-running
  against a landing directory that keeps growing (e.g. another `GenerateSyntheticEvents` batch)
  doesn't re-append files already ingested, and a run with nothing new reports "nothing to do".
  This is file-level idempotency, on top of (not instead of) the row-level dedupe by
  `(instrument, seq_no)` that still happens downstream in silver (Phase 4) — that dedupe remains
  the correctness backstop for duplicate rows within or across files.

  ```sh
  sbt "runMain example.IngestRawEvents <path> [format]"
  # or:
  make spark-ingest-raw-events
  # or, as part of a full pipeline run:
  make ingest
  ```

- **`example.CreateSilverTable`** — creates `orderbook.silver.book_events` with the schema from
  `OrderBookSchema`, partitioned by `instrument`/`event_date`. Rerunnable (`createOrReplace`).

  ```sh
  sbt "runMain example.CreateSilverTable"
  # or:
  make spark-create-silver-table
  ```

- **`example.Watermark`** — not a job, but the incrementality primitive every gold/silver
  transform builds on (Phase 6 of `data_pipeline_plan.md`): records "last upstream snapshot id
  processed" as an Iceberg table property on the sink table (`pipeline.watermark.<name>`), so a
  job can read only the source rows appended since its last run
  (`spark.read.format("iceberg").option("start-snapshot-id", ...).option("end-snapshot-id", ...)`)
  instead of rescanning full history every time. Falls back to a full read when no watermark is
  recorded yet (first run).

- **`example.BuildSilverEvents`** — Silver transform (Phase 4 of `data_pipeline_plan.md`): reads
  `orderbook.bronze.raw_events`, drops malformed rows (unknown `event_type`, missing required
  fields, an invalid `side`, or a missing `price`/`qty` on non-`snapshot` events), dedupes on
  `(instrument, seq_no)`, derives `event_date` from `timestamp`, and `MERGE INTO`s the result into
  `orderbook.silver.book_events` — rerunning it never inserts an `(instrument, seq_no)` already
  present in silver. Aborts before the merge if more than half the batch is dropped as malformed
  (`BuildSilverEvents.checkQuality`). Incremental (Phase 6): only reads bronze rows appended since
  the last run, via `Watermark` recorded on the silver table.

  ```sh
  sbt "runMain example.BuildSilverEvents"
  # or:
  make spark-build-silver-events
  # or, as part of a full pipeline run:
  make silver
  ```

- **`example.CreateGoldTables`** — creates `orderbook.gold.ohlcv_bars_1m` and
  `orderbook.gold.top_of_book_snapshots` (partitioned by `event_date`), plus
  `orderbook.gold.book_state` (unpartitioned running-book state, Phase 6), with the schemas from
  `OrderBookSchema`. Rerunnable (`createOrReplace`).

  ```sh
  sbt "runMain example.CreateGoldTables"
  # or:
  make spark-create-gold-tables
  ```

- **`example.BuildGoldAggregates`** — Gold transform (Phase 5 of `data_pipeline_plan.md`): reads
  `orderbook.silver.book_events` and writes two aggregates. `ohlcvBars` builds one-minute OHLCV
  bars per instrument from `trade` events. `topOfBookSnapshots` reconstructs a running
  per-`(instrument, side, price)` level book from `add`/`cancel`/`trade` events (`add` adds qty,
  `cancel`/`trade` subtract it) and samples the best bid/ask every minute, forward-filling a
  level's qty into windows where it isn't touched. `modify` events aren't netted into level
  totals — their row carries the order's new absolute qty, not a delta, and there's no
  `order_id` in the schema to track an individual order across events, so there's no way to
  recover what changed.

  Incremental (Phase 6): only reads silver rows appended since the last run, via `Watermark`
  recorded on the OHLCV table, and appends the new bars/snapshots rather than recomputing and
  overwriting the whole table. Top-of-book state carries forward across runs through
  `orderbook.gold.book_state`, so a resting order posted in an earlier batch still counts toward
  later batches' top of book. This assumes each run covers a complete batch of new data — if a
  1-minute window's events happened to arrive split across two separate runs, each run would
  append its own partial bar/snapshot for that window instead of merging into one, which is an
  accepted limitation for this batch-oriented pipeline rather than something engineered around
  (see `data_pipeline_plan.md` Phase 6).

  ```sh
  sbt "runMain example.BuildGoldAggregates"
  # or:
  make spark-build-gold-aggregates
  # or, as part of a full pipeline run:
  make gold
  ```

- **Pipeline-stage aliases** (Phase 6) — `make ingest`, `make silver`, `make gold` wrap
  `example.IngestRawEvents`, `example.BuildSilverEvents`, `example.BuildGoldAggregates`
  respectively, so a full incremental run of the pipeline is:

  ```sh
  make ingest && make silver && make gold
  ```

- **`example.MaintainTables`** — Table maintenance (Phase 7 of `data_pipeline_plan.md`): runs
  Iceberg's Spark maintenance procedures against every managed table (`bronze.raw_events`,
  `silver.book_events`, `gold.ohlcv_bars_1m`, `gold.top_of_book_snapshots`, `gold.book_state`), in
  the order Iceberg recommends — compact, then expire, then sweep orphans:

  - `CALL orderbook.system.rewrite_data_files(table => '...')` — compacts the small files that
    Phase 6's frequent incremental appends/merges produce.
  - `CALL orderbook.system.expire_snapshots(table => '...', older_than => ..., retain_last => ...)`
    — drops snapshots past a retention window (default 7 days, always keeping at least the most
    recent one), bounding storage/metadata growth from those same commits.
  - `CALL orderbook.system.remove_orphan_files(table => '...', older_than => ...)` — sweeps files
    under the table's data directory that no live snapshot references (e.g. left behind by a
    failed write), past a conservative default 3-day cutoff so files from an in-flight commit
    aren't swept.

  Retention windows are configurable via `MAINTENANCE_SNAPSHOT_RETENTION_HOURS` (default `168`),
  `MAINTENANCE_RETAIN_LAST_SNAPSHOTS` (default `1`), and `MAINTENANCE_ORPHAN_FILE_RETENTION_HOURS`
  (default `72`).

  ```sh
  sbt "runMain example.MaintainTables"
  # or:
  make spark-maintain-tables
  # or:
  make maintain
  ```

  No scheduler wired up yet, same as the Phase 6 pipeline jobs — run it by hand or cron it
  externally.

## Configuration notes

Polaris is configured for **local/demo use only** — see the `polaris` service environment in
`docker-compose.yml`:

- `polaris.persistence.type: in-memory` — catalog state is **lost on every restart**, so
  `polaris-init` recreates the `orderbook` catalog each time the stack comes up.
- `SUPPORTED_CATALOG_STORAGE_TYPES: ["S3"]` — only S3 storage is allowed. Polaris' production
  readiness checks reject the `FILE` storage type as insecure and abort startup if it's enabled.
- `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION: true` — lets Polaris use ambient AWS credentials
  against MinIO without STS. Convenient for a demo, **not** safe for production.
- RSA token-signing keys are auto-generated on startup rather than supplied.

These are fine for a demo but must be hardened before any real deployment. See
[Configuring Polaris for production](https://polaris.apache.org/in-dev/unreleased/configuring-polaris-for-production).

### Making Polaris + MinIO actually work for table writes

Getting Spark to successfully commit an Iceberg table (not just list catalogs/namespaces)
against Polaris + MinIO needed three non-obvious fixes, all present in this repo already but
worth knowing about if you touch this config:

1. **`stsUnavailable: true`** on the catalog's `storageConfigInfo` (set in `polaris-setup.sh`).
   Without it, Polaris' `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION` flag alone doesn't stop it from
   attempting STS-based credential vending (a known upstream limitation —
   [apache/polaris#379](https://github.com/apache/polaris/issues/379)).
2. **`AWS_ENDPOINT_URL_S3` / `AWS_ENDPOINT_URL_STS`** env vars on the `polaris` service, both
   pointing at MinIO. Polaris' own internal S3 client (used for server-side commit bookkeeping,
   separate from the per-catalog client config) otherwise defaults to real AWS and fails with
   `403 The AWS Access Key Id you provided does not exist in our records`.
3. **`MINIO_DOMAIN` on MinIO + a network alias `orderbook-warehouse.minio`** on the `minio`
   service. Even with the endpoint overrides above, Polaris' internal client addresses buckets
   virtual-hosted-style (`<bucket>.<host>`) regardless of the catalog's `pathStyleAccess`
   setting. `MINIO_DOMAIN` teaches MinIO to recognize that style, and the alias makes
   `orderbook-warehouse.minio` resolve to the MinIO container.

None of this affects how the Spark *client* talks to MinIO (that path already used path-style
access via `s3.path-style-access=true`, set in `example.PolarisSpark`) — it's specifically about
requests Polaris itself makes internally when committing a table.

### Making `remove_orphan_files` work

Iceberg's Spark maintenance actions (Phase 7, `example.MaintainTables`) read/write table data via
`S3FileIO` like everything else — except `remove_orphan_files`, which lists the table's storage
location using Hadoop's `FileSystem` API instead, and failed with
`UnsupportedFileSystemException: No FileSystem for scheme "s3"` until `example.PolarisSpark` also
registered a `FileSystem` for that scheme: the `hadoop-aws` dependency (pinned to the Hadoop
version `spark-sql` already pulls in transitively) provides `S3AFileSystem`, wired up via
`spark.hadoop.fs.s3.impl` and `fs.s3a.*` config pointed at the same MinIO endpoint/credentials as
the `S3FileIO` config above. `rewrite_data_files` and `expire_snapshots` didn't need this — only
the orphan-file sweep touches Hadoop's filesystem layer.
