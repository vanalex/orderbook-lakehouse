#!/bin/sh
# Bootstraps Apache Polaris for the orderbook-lakehouse demo:
#   1. Get an OAuth token as the root principal
#   2. Create the 'orderbook' catalog pointing at MinIO
#   3. Grant the root principal full access to it
set -e

POLARIS="http://polaris:8181"
REALM="default-realm"

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
curl -s -X POST "$POLARIS/api/management/v1/catalogs" \
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
        "endpoint": "http://minio:9000",
        "pathStyleAccess": true,
        "stsUnavailable": true
      }
    }
  }'

echo "Granting catalog access to root..."
# Give the auto-created catalog_admin role full content management
curl -s -X PUT \
  "$POLARIS/api/management/v1/catalogs/orderbook/catalog-roles/catalog_admin/grants" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: $REALM" \
  -H "Content-Type: application/json" \
  -d '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}'

# Attach catalog_admin to the service_admin principal role (root has it)
curl -s -X PUT \
  "$POLARIS/api/management/v1/principal-roles/service_admin/catalog-roles/orderbook" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: $REALM" \
  -H "Content-Type: application/json" \
  -d '{"catalogRole": {"name": "catalog_admin"}}'

echo ""
echo "Polaris ready: catalog 'orderbook' -> s3://orderbook-warehouse (MinIO)"