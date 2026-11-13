# Makefile — read/"get" operations against the local Polaris catalog.
#
# All targets talk to Polaris on the host (http://localhost:8181) using the
# demo root credentials. Each recipe fetches a fresh OAuth token first.
#
# Requires: curl, jq.

POLARIS      ?= http://localhost:8181
REALM        ?= default-realm
CLIENT_ID    ?= root
CLIENT_SECRET?= s3cr3t
CATALOG      ?= orderbook
NAMESPACE    ?=bronze

MGMT     := $(POLARIS)/api/management/v1
CAT      := $(POLARIS)/api/catalog/v1

# Fetch an access token as the root principal. Used inline by other recipes.
define get_token
curl -s -X POST "$(POLARIS)/api/catalog/v1/oauth/tokens" \
  -H "Polaris-Realm: $(REALM)" \
  -d grant_type=client_credentials \
  -d client_id=$(CLIENT_ID) -d client_secret=$(CLIENT_SECRET) \
  -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token
endef

# GET $(1) with a bearer token, pretty-print the JSON.
define authed_get
TOKEN=$$($(get_token)); \
if [ -z "$$TOKEN" ] || [ "$$TOKEN" = "null" ]; then \
  echo "Failed to obtain token — is Polaris up? (make health)" >&2; exit 1; \
fi; \
curl -s "$(1)" \
  -H "Authorization: Bearer $$TOKEN" \
  -H "Polaris-Realm: $(REALM)" | jq .
endef

.PHONY: help token health catalogs catalog catalog-roles spark-hello \
        principals principal-roles namespaces tables snapshots scala-catalogs spark-init-namespaces generate generate-next generate-replay demo-dedup setup \
        spark-smoke-test spark-create-bronze-table spark-create-silver-table spark-build-silver-events \
        spark-create-gold-tables spark-build-gold-aggregates spark-ingest-raw-events \
        spark-maintain-tables \
        ingest clear-landing silver gold maintain \
        query-spread query-imbalance query-liquidity-dry-up query-realized-vol \
        query-parkinson-vol query-vwap query-cancel-add-ratio query-trade-through \
        query-effective-spread

help: ## List available targets
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "} {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

health: ## Check the Polaris health endpoint
	@curl -sf http://localhost:8182/q/health | jq .

token: ## Print an OAuth access token for the root principal
	@$(get_token)

catalogs: ## List all catalogs
	@$(call authed_get,$(MGMT)/catalogs)

catalog: ## Get one catalog (CATALOG=orderbook)
	@$(call authed_get,$(MGMT)/catalogs/$(CATALOG))

catalog-roles: ## List catalog roles for a catalog (CATALOG=orderbook)
	@$(call authed_get,$(MGMT)/catalogs/$(CATALOG)/catalog-roles)

principals: ## List all principals
	@$(call authed_get,$(MGMT)/principals)

principal-roles: ## List all principal roles
	@$(call authed_get,$(MGMT)/principal-roles)

namespaces: ## List namespaces in a catalog (CATALOG=orderbook)
	@$(call authed_get,$(CAT)/$(CATALOG)/namespaces)

tables: ## List tables in a namespace (CATALOG=orderbook NAMESPACE=bronze)
	@if [ -z "$(NAMESPACE)" ]; then \
	  echo "Set NAMESPACE, e.g. make tables NAMESPACE=bronze" >&2; exit 1; \
	fi
	@$(call authed_get,$(CAT)/$(CATALOG)/namespaces/$(NAMESPACE)/tables)

SNAPSHOTS ?= 8

snapshots: ## Last SNAPSHOTS snapshots + pipeline watermarks of a table, from Polaris (TABLE=silver.book_events)
	@if [ -z "$(TABLE)" ]; then \
	  echo "Set TABLE, e.g. make snapshots TABLE=silver.book_events" >&2; exit 1; \
	fi
	@$(call authed_get,$(CAT)/$(CATALOG)/namespaces/$(word 1,$(subst ., ,$(TABLE)))/tables/$(word 2,$(subst ., ,$(TABLE)))) | jq -r '.metadata as $$m \
	  | "$(TABLE)", \
	    ($$m.properties | to_entries[] | select(.key | startswith("pipeline.watermark")) | "  watermark  \(.key | sub("pipeline.watermark."; "")) = \(.value)"), \
	    "  committed (UTC)       snapshot_id           operation  added  total", \
	    ($$m.snapshots | sort_by(."timestamp-ms") | .[-$(SNAPSHOTS):][] \
	      | "  \((."timestamp-ms" / 1000 | floor | todate) | sub("T"; " ") | sub("Z"; ""))  \(."snapshot-id" | tostring | (. + "                    ")[0:20])  \((.summary.operation + "         ")[0:9])  \((.summary["added-records"] // "0") | (. + "      ")[0:5])  \(.summary["total-records"] // "?")")'

scala-catalogs: ## List catalogs via the Scala job (orderbook.ops.ListCatalogs); loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 sbt -batch "runMain orderbook.ops.ListCatalogs"

spark-init-namespaces: ## Create bronze/silver/gold namespaces via Spark (orderbook.ops.CreateNamespaces); loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 sbt -batch "runMain orderbook.ops.CreateNamespaces"

spark-hello: ## Check that local Spark runs, no Docker stack needed (orderbook.ops.SparkHelloWorld)
	@sbt -batch "runMain orderbook.ops.SparkHelloWorld"

spark-smoke-test: ## End-to-end table write/read check via Spark (orderbook.ops.SmokeTest); loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 sbt -batch "runMain orderbook.ops.SmokeTest"

spark-create-bronze-table: ## Create orderbook.bronze.raw_events via Spark (orderbook.bronze.CreateBronzeTable); loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 sbt -batch "runMain orderbook.bronze.CreateBronzeTable"

spark-ingest-raw-events: ## Append Redpanda Connect's landed files (STREAM_LANDING) into bronze (orderbook.bronze.IngestRawEvents); loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 STREAM_LANDING=$(STREAM_LANDING) sbt -batch "runMain orderbook.bronze.IngestRawEvents"

spark-create-silver-table: ## Create orderbook.silver.book_events via Spark (orderbook.silver.CreateSilverTable); loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 sbt -batch "runMain orderbook.silver.CreateSilverTable"

spark-build-silver-events: ## Clean bronze events and merge into silver (orderbook.silver.BuildSilverEvents); loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 sbt -batch "runMain orderbook.silver.BuildSilverEvents"

spark-create-gold-tables: ## Create orderbook.gold.* tables via Spark (orderbook.gold.CreateGoldTables); loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 sbt -batch "runMain orderbook.gold.CreateGoldTables"

spark-build-gold-aggregates: ## Build OHLCV bars + top-of-book snapshots into gold (orderbook.gold.BuildGoldAggregates); loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 sbt -batch "runMain orderbook.gold.BuildGoldAggregates"

spark-maintain-tables: ## Compact, expire snapshots, and remove orphan files on every managed table (orderbook.ops.MaintainTables); loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 sbt -batch "runMain orderbook.ops.MaintainTables"

# ------------------------------------------------------------------
# Phase 6: pipeline-stage aliases. Each wraps the incremental job for
# its stage (ingest -> silver -> gold), so a full run is just:
#   make ingest && make silver && make gold
# ------------------------------------------------------------------

setup: spark-init-namespaces spark-create-bronze-table spark-create-silver-table spark-create-gold-tables clear-landing ## Create namespaces, (re)create all tables empty, and clear the landing prefix
	@rm -f $(GEN_STATE)

# ------------------------------------------------------------------
# Ingest is streaming-only, through Redpanda Connect
# (infra/redpanda-connect/landing.yaml): producers POST events to
# STREAM_URL, Redpanda Connect lands them as NDJSON files under
# STREAM_LANDING, and `make ingest` appends new landed files to bronze.
# ------------------------------------------------------------------

STREAM_URL     ?= http://localhost:4195/events
STREAM_LANDING ?= s3://orderbook-warehouse/landing/raw_events
GEN_COUNT      ?= 5000
GEN_INTERVAL   ?= 1000
GEN_FROM       ?= 0
# Last slice sent ("<from> <count>"), so generate-next/-replay know where the
# feed stands. Removed by make setup, which also empties every table.
GEN_STATE      := .generate-state

generate: ## POST GEN_COUNT synthetic events, starting after the first GEN_FROM of the feed (default 0), to Redpanda Connect
	@sbt -batch "runMain orderbook.tools.GenerateSyntheticEvents $(GEN_COUNT) BTC-USD,ETH-USD $(STREAM_URL) $(GEN_INTERVAL) $(GEN_FROM)"
	@echo "$(GEN_FROM) $(GEN_COUNT)" > $(GEN_STATE)

generate-next: ## POST the next GEN_COUNT events of the feed: new seq_no, later timestamps (first run after setup starts at 0)
	@from=0; \
	 if [ -f $(GEN_STATE) ]; then read f c < $(GEN_STATE); from=$$((f + c)); fi; \
	 $(MAKE) --no-print-directory generate GEN_FROM=$$from

generate-replay: ## POST the last batch again: exact duplicates, for the dedupe demo
	@if [ ! -f $(GEN_STATE) ]; then echo "Nothing sent since make setup: run make generate or make generate-next first" >&2; exit 1; fi; \
	 read f c < $(GEN_STATE); \
	 $(MAKE) --no-print-directory generate GEN_FROM=$$f GEN_COUNT=$$c

demo-dedup: ## Replay the last batch, then ingest + silver: silver should insert 0 and skip them all
	@$(MAKE) --no-print-directory generate-replay
	@$(MAKE) --no-print-directory ingest
	@$(MAKE) --no-print-directory silver

ingest: spark-ingest-raw-events ## Alias for spark-ingest-raw-events

clear-landing: ## Delete landed files under STREAM_LANDING (orderbook.ops.ClearLanding); run by make setup; loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 STREAM_LANDING=$(STREAM_LANDING) sbt -batch "runMain orderbook.ops.ClearLanding"

silver: spark-build-silver-events ## Alias for spark-build-silver-events

gold: spark-build-gold-aggregates ## Alias for spark-build-gold-aggregates

maintain: spark-maintain-tables ## Alias for spark-maintain-tables (Phase 7)

# ------------------------------------------------------------------
# Ad-hoc analytical queries against gold/silver, one Spark job each.
# ------------------------------------------------------------------

query-spread: ## Bid-ask spread over time (orderbook.analytics.SpreadOverTime); loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 sbt -batch "runMain orderbook.analytics.SpreadOverTime"

query-imbalance: ## Book imbalance vs. next bar's return (orderbook.analytics.BookImbalanceVsNextBar); loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 sbt -batch "runMain orderbook.analytics.BookImbalanceVsNextBar"

query-liquidity-dry-up: ## Liquidity dry-up windows (orderbook.analytics.LiquidityDryUpWindows); loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 sbt -batch "runMain orderbook.analytics.LiquidityDryUpWindows"

query-realized-vol: ## Rolling realized volatility (orderbook.analytics.RealizedVolatility); loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 sbt -batch "runMain orderbook.analytics.RealizedVolatility"

query-parkinson-vol: ## Parkinson range volatility (orderbook.analytics.ParkinsonVolatility); loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 sbt -batch "runMain orderbook.analytics.ParkinsonVolatility"

query-vwap: ## Daily VWAP vs. average close (orderbook.analytics.VwapVsClose); loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 sbt -batch "runMain orderbook.analytics.VwapVsClose"

query-cancel-add-ratio: ## Cancel-to-add ratio (orderbook.analytics.CancelToAddRatio); loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 sbt -batch "runMain orderbook.analytics.CancelToAddRatio"

query-trade-through: ## Trade-through detection (orderbook.analytics.TradeThroughDetection); loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 sbt -batch "runMain orderbook.analytics.TradeThroughDetection"

query-effective-spread: ## Effective vs. quoted spread (orderbook.analytics.EffectiveVsQuotedSpread); loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 sbt -batch "runMain orderbook.analytics.EffectiveVsQuotedSpread"
