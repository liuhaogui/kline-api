#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
GO=${GO:-go}
VERSION=${VERSION:-dev}
case "$VERSION" in *[!a-zA-Z0-9._-]*|'') echo "Invalid VERSION" >&2; exit 1 ;; esac
mkdir -p dist
for arch in amd64 arm64; do
  destination="dist/kline-api_${VERSION}_linux_${arch}"
  mkdir -p "$destination"
  CGO_ENABLED=0 GOOS=linux GOARCH="$arch" "$GO" build -trimpath -ldflags="-s -w -X main.version=$VERSION" -o "$destination/kline-api" ./cmd/kline-api
  cp README.md LICENSE .env.example "$destination/"
  cp -R docs deploy "$destination/"
  tar -C dist -czf "${destination}.tar.gz" "$(basename "$destination")"
done
if command -v sha256sum >/dev/null 2>&1; then
  (cd dist && sha256sum "kline-api_${VERSION}_linux_"*.tar.gz > "SHA256SUMS_${VERSION}")
else
  (cd dist && shasum -a 256 "kline-api_${VERSION}_linux_"*.tar.gz > "SHA256SUMS_${VERSION}")
fi
