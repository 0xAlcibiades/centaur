#!/bin/bash
set -euo pipefail

usage() {
  echo "Usage: share-artifact <file> [comment] [--gist] [--public]" >&2
  exit 1
}

if [ $# -lt 1 ]; then
  usage
fi

FILE="$1"
shift
COMMENT="${1:-}"
if [ $# -gt 0 ] && [[ "$1" != --* ]]; then
  shift
fi

USE_GIST=0
PUBLIC=0
while [ $# -gt 0 ]; do
  case "$1" in
    --gist)
      USE_GIST=1
      ;;
    --public)
      PUBLIC=1
      ;;
    *)
      usage
      ;;
  esac
  shift
done

if [ ! -f "$FILE" ]; then
  echo "share-artifact: file not found: $FILE" >&2
  exit 1
fi

if [ "$USE_GIST" -eq 1 ]; then
  if [ "$PUBLIC" -eq 1 ]; then
    github-gist "$FILE" "${COMMENT:-Centaur artifact}" --public
  else
    github-gist "$FILE" "${COMMENT:-Centaur artifact}"
  fi
  exit 0
fi

if [ -n "${SLACK_CHANNEL:-}" ] && [ -n "${SLACK_THREAD_TS:-}" ]; then
  slack-upload "$FILE" "$COMMENT"
  exit 0
fi

github-gist "$FILE" "${COMMENT:-Centaur artifact}"
