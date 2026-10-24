#!/bin/sh
# Bootstraps Apache Polaris for the orderbook-lakehouse demo:
#   1. Get an OAuth token as the root principal
#   2. Create the 'orderbook' catalog pointing at SeaweedFS
#   3. Grant the root principal full access to it
#
# Idempotent: Polaris state persists in Postgres across restarts, so on
# every start after the first the catalog already exists (409) and the
# grant PUTs are no-ops.
set -e

POLARIS="http://polaris:8181"
REALM="default-realm"

# Fails the script unless HTTP status $1 is one of the allowed codes ($2...).
expect_status() {
  status="$1"; shift
  for ok in "$@"; do
    [ "$status" = "$ok" ] && return 0
  done
  echo "Unexpected HTTP status $status:" >&2
  cat /tmp/response >&2
  echo >&2
  exit 1
}

echo "Requesting token..."
TOKEN=$(curl -s -X POST "$POLARIS/api/catalog/v1/oauth/tokens" \
  -H "Polaris-Realm: $REALM" \
  -d "grant_type=client_credentials" \
  -d "client_id=root" \
  -d "client_secret=s3cr3t" \
  -d "scope=PRINCIPAL_ROLE:ALL" | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')

if [ -z "$TOKEN" ]; then
  echo "Failed to obtain token" >&2
  exit 1
fi

echo "Creating catalog 'orderbook'..."
STATUS=$(curl -s -o /tmp/response -w '%{http_code}' -X POST "$POLARIS/api/management/v1/catalogs" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: $REALM" \
  -H "Content-Type: application/json" \
  -d '{
    "catalog": {
      "name": "orderbook",
      "type": "INTERNAL",
      "properties": {
        "default-base-location": "s3://orderbook-warehouse"
      },
      "storageConfigInfo": {
        "storageType": "S3",
        "allowedLocations": ["s3://orderbook-warehouse/"],
        "endpoint": "http://seaweedfs:8333",
        "pathStyleAccess": true,
        "stsUnavailable": true
      }
    }
  }')
expect_status "$STATUS" 201 409
[ "$STATUS" = "409" ] && echo "Catalog 'orderbook' already exists"

echo "Granting catalog access to root..."
# Give the auto-created catalog_admin role full content management
STATUS=$(curl -s -o /tmp/response -w '%{http_code}' -X PUT \
  "$POLARIS/api/management/v1/catalogs/orderbook/catalog-roles/catalog_admin/grants" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: $REALM" \
  -H "Content-Type: application/json" \
  -d '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}')
expect_status "$STATUS" 200 201 204

# Attach catalog_admin to the service_admin principal role (root has it)
STATUS=$(curl -s -o /tmp/response -w '%{http_code}' -X PUT \
  "$POLARIS/api/management/v1/principal-roles/service_admin/catalog-roles/orderbook" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: $REALM" \
  -H "Content-Type: application/json" \
  -d '{"catalogRole": {"name": "catalog_admin"}}')
expect_status "$STATUS" 200 201 204

echo ""
echo "Polaris ready: catalog 'orderbook' -> s3://orderbook-warehouse (SeaweedFS)"
