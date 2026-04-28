#!/usr/bin/env sh
set -eu

REPO="${QWHYPER_REPO:-your-org/qwhyper}"
INSTALL_DIR="${INSTALL_DIR:-$HOME/.local/bin}"
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

case "$ARCH" in
  x86_64|amd64) ARCH="x86_64" ;;
  arm64|aarch64) ARCH="aarch64" ;;
  *) echo "unsupported arch: $ARCH" >&2; exit 1 ;;
esac

case "$OS" in
  linux) TARGET="unknown-linux-gnu" ;;
  darwin) TARGET="apple-darwin" ;;
  *) echo "unsupported os: $OS" >&2; exit 1 ;;
esac

mkdir -p "$INSTALL_DIR"
URL="https://github.com/$REPO/releases/latest/download/qwhyper-$ARCH-$TARGET"
TMP="$(mktemp)"

curl -fsSL "$URL" -o "$TMP"
chmod +x "$TMP"
mv "$TMP" "$INSTALL_DIR/qwhyper"

echo "qwhyper installed to $INSTALL_DIR/qwhyper"
