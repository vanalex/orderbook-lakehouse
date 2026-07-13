# orderbook-lakehouse

A demo lakehouse for order-book data, built on the **Apache Iceberg** table format with an
**Apache Polaris** REST catalog and **MinIO** (S3-compatible) object storage. The application
layer is a Scala 2.13 project (currently a skeleton).

> Status: infrastructure bootstrapped. The Scala app is still a `Hello`-world placeholder —
> the Iceberg read/write jobs are the next step.

## Architecture

```
┌──────────────┐      Iceberg REST       ┌──────────────┐      S3 API      ┌──────────────┐
│  Scala app   │ ──────────────────────► │   Polaris    │ ───────────────► │    MinIO     │
│  (Iceberg    │   catalog + metadata    │  REST catalog│   data + metadata│  object store│
│   client)    │                         │  :8181       │                  │  :9000/:9001 │
└──────────────┘                         └──────────────┘                  └──────────────┘
```

- **MinIO** — S3-compatible object storage that backs the Iceberg warehouse
  (bucket `orderbook-warehouse`).
- **Polaris** — Iceberg REST catalog that manages namespaces, tables, and metadata,
  storing data files in MinIO.
- **Scala app** — connects to Polaris over the Iceberg REST protocol to read/write tables.

## Prerequisites

- Docker + Docker Compose
- JDK 11+ and [sbt](https://www.scala-sbt.org/) (for the Scala app)

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

## The Scala app

```sh
sbt compile   # build
sbt test      # run tests (munit)
sbt run       # run — currently prints "hello"
```

Configuration lives in `build.sbt` (Scala 2.13.16, project `orderbook-lakehouse`).

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
