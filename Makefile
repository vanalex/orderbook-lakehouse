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

.PHONY: help token health catalogs catalog catalog-roles \
        principals principal-roles namespaces tables scala-catalogs spark-init-namespaces \
        spark-smoke-test spark-create-bronze-table spark-create-silver-table spark-build-silver-events \
        spark-create-gold-tables spark-build-gold-aggregates spark-ingest-raw-events \
        ingest silver gold

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

scala-catalogs: ## List catalogs via the Scala job (example.ListCatalogs); loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 sbt -batch "runMain example.ListCatalogs"

spark-init-namespaces: ## Create bronze/silver/gold namespaces via Spark (example.CreateNamespaces); loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 sbt -batch "runMain example.CreateNamespaces"

spark-smoke-test: ## End-to-end table write/read check via Spark (example.SmokeTest); loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 sbt -batch "runMain example.SmokeTest"

spark-create-bronze-table: ## Create orderbook.bronze.raw_events via Spark (example.CreateBronzeTable); loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 sbt -batch "runMain example.CreateBronzeTable"

spark-ingest-raw-events: ## Append SOURCE_PATH's raw feed files into bronze (example.IngestRawEvents); loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 sbt -batch "runMain example.IngestRawEvents"

spark-create-silver-table: ## Create orderbook.silver.book_events via Spark (example.CreateSilverTable); loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 sbt -batch "runMain example.CreateSilverTable"

spark-build-silver-events: ## Clean bronze events and merge into silver (example.BuildSilverEvents); loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 sbt -batch "runMain example.BuildSilverEvents"

spark-create-gold-tables: ## Create orderbook.gold.* tables via Spark (example.CreateGoldTables); loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 sbt -batch "runMain example.CreateGoldTables"

spark-build-gold-aggregates: ## Build OHLCV bars + top-of-book snapshots into gold (example.BuildGoldAggregates); loads .env if present
	@set -a; [ -f .env ] && . ./.env || true; set +a; \
	 sbt -batch "runMain example.BuildGoldAggregates"

# ------------------------------------------------------------------
# Phase 6: pipeline-stage aliases. Each wraps the incremental job for
# its stage (ingest -> silver -> gold), so a full run is just:
#   make ingest && make silver && make gold
# ------------------------------------------------------------------

ingest: spark-ingest-raw-events ## Alias for spark-ingest-raw-events

silver: spark-build-silver-events ## Alias for spark-build-silver-events

gold: spark-build-gold-aggregates ## Alias for spark-build-gold-aggregates
