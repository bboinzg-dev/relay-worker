#!/usr/bin/env bash
set -euo pipefail
SITE="${SITE:-https://<YOUR-VERCEL-URL>}"
echo "[1] search"
curl -s "$SITE/api/proxy/parts/searchx?q=g2r" | jq .total

echo "[2] checkout preview"
curl -s -X POST "$SITE/api/proxy/checkout/preview" -H 'content-type: application/json' -H 'X-Actor-Roles: buyer'     -d '{"items":[{"brand":"omron","code":"g2r-1a","qty":5}]}' | jq .plan.totals

echo "[3] order create"
ORDER_JSON=$(curl -s -X POST "$SITE/api/proxy/checkout/create" -H 'content-type: application/json' -H 'X-Actor-Roles: buyer' -H 'X-Actor-Id: u1'     -d '{"items":[{"brand":"omron","code":"g2r-1a","qty":5}]}' )
echo "$ORDER_JSON" | jq .order.id

echo "[4] cover render (if missing)"
curl -s -X POST "$SITE/api/proxy/render/cover" -H 'content-type: application/json'     -d '{"brand":"omron","code":"g2r-1a","gcsPdfUri":"gs://partsplan-docai-us/datasheets/g2r-1a.pdf"}' | jq .

echo "[5] quality scan relay"
curl -s -X POST "$SITE/api/proxy/quality/run" -H 'content-type: application/json' -H 'X-Actor-Roles: admin' -d '{"family":"relay"}' | jq .
