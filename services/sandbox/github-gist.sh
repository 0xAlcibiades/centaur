#!/bin/bash
set -euo pipefail

usage() {
  echo "Usage: github-gist <file> [description] [--public]" >&2
  exit 1
}

if [ $# -lt 1 ]; then
  usage
fi

FILE="$1"
shift
DESCRIPTION="${1:-Centaur artifact}"
if [ $# -gt 0 ] && [[ "$1" != --* ]]; then
  shift
fi

PUBLIC_FLAG=""
while [ $# -gt 0 ]; do
  case "$1" in
    --public)
      PUBLIC_FLAG="--public"
      ;;
    *)
      usage
      ;;
  esac
  shift
done

if [ ! -f "$FILE" ]; then
  echo "github-gist: file not found: $FILE" >&2
  exit 1
fi

gh gist create "$FILE" --desc "$DESCRIPTION" $PUBLIC_FLAG
