#!/bin/sh
set -eu

: "${INFRAI_API_KEY:?set INFRAI_API_KEY}"
: "${PUBLIC_URL:?set PUBLIC_URL to this service's reachable URL}"

go run .
