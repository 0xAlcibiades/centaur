#!/bin/bash
set -euo pipefail

usage() {
  echo "Usage: slack-upload <file_path> [comment]" >&2
  exit 1
}

if [ $# -lt 1 ]; then
  usage
fi

FILE="$1"
COMMENT="${2:-}"

if [ ! -f "$FILE" ]; then
  echo "slack-upload: file not found: $FILE" >&2
  exit 1
fi

CHANNEL="${SLACK_CHANNEL:?SLACK_CHANNEL not set}"
THREAD="${SLACK_THREAD_TS:?SLACK_THREAD_TS not set}"

if [ -n "$COMMENT" ]; then
  slack upload "$CHANNEL" "$FILE" --thread "$THREAD" --comment "$COMMENT"
else
  slack upload "$CHANNEL" "$FILE" --thread "$THREAD"
fi
