#!/bin/bash
set -euo pipefail

usage() {
  echo "Usage: github-link <path[:line[-end]]> [--ref <ref>]" >&2
  exit 1
}

if [ $# -lt 1 ]; then
  usage
fi

TARGET="$1"
shift
REF_OVERRIDE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --ref)
      shift
      REF_OVERRIDE="${1:-}"
      if [ -z "$REF_OVERRIDE" ]; then
        usage
      fi
      ;;
    *)
      usage
      ;;
  esac
  shift
done

LINE_SPEC=""
PATH_PART="$TARGET"
if [[ "$TARGET" =~ ^(.+):([0-9]+)(-([0-9]+))?$ ]]; then
  PATH_PART="${BASH_REMATCH[1]}"
  if [ -n "${BASH_REMATCH[4]:-}" ]; then
    LINE_SPEC="L${BASH_REMATCH[2]}-L${BASH_REMATCH[4]}"
  else
    LINE_SPEC="L${BASH_REMATCH[2]}"
  fi
fi

if [[ "$PATH_PART" == "~/"* ]]; then
  PATH_PART="$HOME/${PATH_PART#~/}"
fi

if [ -e "$PATH_PART" ]; then
  ABS_PATH="$(cd "$(dirname "$PATH_PART")" && pwd -P)/$(basename "$PATH_PART")"
else
  ABS_PATH="$PATH_PART"
fi

REPO_ROOT="$(git -C "$(dirname "$ABS_PATH")" rev-parse --show-toplevel 2>/dev/null || true)"
if [ -z "$REPO_ROOT" ]; then
  REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
  if [ -n "$REPO_ROOT" ]; then
    ABS_PATH="$REPO_ROOT/$PATH_PART"
  fi
fi
if [ -z "$REPO_ROOT" ]; then
  echo "github-link: not inside a git repository: $TARGET" >&2
  exit 1
fi

REL_PATH="$(git -C "$REPO_ROOT" ls-files --full-name -- "$ABS_PATH" 2>/dev/null | head -n 1)"
if [ -z "$REL_PATH" ] && [[ "$PATH_PART" != /* ]]; then
  SUFFIX_MATCHES="$(git -C "$REPO_ROOT" ls-files -- "$PATH_PART" "*/$PATH_PART" 2>/dev/null | sort -u)"
  SUFFIX_MATCH_COUNT="$(printf '%s\n' "$SUFFIX_MATCHES" | sed '/^$/d' | wc -l | tr -d ' ')"
  if [ "$SUFFIX_MATCH_COUNT" = "1" ]; then
    REL_PATH="$(printf '%s\n' "$SUFFIX_MATCHES" | sed '/^$/d')"
  elif [ "$SUFFIX_MATCH_COUNT" -gt 1 ]; then
    echo "github-link: ambiguous path suffix: $PATH_PART" >&2
    printf '%s\n' "$SUFFIX_MATCHES" >&2
    exit 1
  fi
fi
if [ -z "$REL_PATH" ]; then
  REL_PATH="$(realpath --relative-to="$REPO_ROOT" "$ABS_PATH" 2>/dev/null || true)"
fi
if [ -z "$REL_PATH" ] || [[ "$REL_PATH" == ../* ]]; then
  echo "github-link: path is not inside repo: $TARGET" >&2
  exit 1
fi

REMOTE_URL="$(git -C "$REPO_ROOT" config --get remote.origin.url 2>/dev/null || true)"
if [[ "$REMOTE_URL" =~ ^git@github\.com:([^/]+)/(.+)\.git$ ]]; then
  OWNER="${BASH_REMATCH[1]}"
  REPO="${BASH_REMATCH[2]}"
elif [[ "$REMOTE_URL" =~ ^git@github\.com:([^/]+)/(.+)$ ]]; then
  OWNER="${BASH_REMATCH[1]}"
  REPO="${BASH_REMATCH[2]}"
elif [[ "$REMOTE_URL" =~ ^https://github\.com/([^/]+)/(.+)\.git$ ]]; then
  OWNER="${BASH_REMATCH[1]}"
  REPO="${BASH_REMATCH[2]}"
elif [[ "$REMOTE_URL" =~ ^https://github\.com/([^/]+)/(.+)$ ]]; then
  OWNER="${BASH_REMATCH[1]}"
  REPO="${BASH_REMATCH[2]}"
elif [[ "$REPO_ROOT" =~ /home/agent/(github|branches)/([^/]+)/([^/]+)$ ]]; then
  OWNER="${BASH_REMATCH[2]}"
  REPO="${BASH_REMATCH[3]}"
elif [ -n "${AGENT_REPO:-}" ] && [[ "$AGENT_REPO" == */* ]]; then
  OWNER="${AGENT_REPO%%/*}"
  REPO="${AGENT_REPO#*/}"
else
  echo "github-link: could not infer GitHub repo from origin: $REMOTE_URL" >&2
  exit 1
fi

if [ -n "$REF_OVERRIDE" ]; then
  REF="$REF_OVERRIDE"
else
  BRANCH="$(git -C "$REPO_ROOT" branch --show-current 2>/dev/null || true)"
  DEFAULT_REF="$(git -C "$REPO_ROOT" symbolic-ref --quiet --short refs/remotes/origin/HEAD 2>/dev/null | sed 's#^origin/##' || true)"
  if [ -n "$BRANCH" ] && git -C "$REPO_ROOT" show-ref --verify --quiet "refs/remotes/origin/$BRANCH"; then
    REF="$BRANCH"
  else
    REF="$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || true)"
    if [ -z "$REF" ]; then
      REF="${DEFAULT_REF:-main}"
    fi
  fi
fi

URL_PATH="$(printf '%s' "$REL_PATH" | jq -sRr @uri | sed 's#%2F#/#g')"
URL="https://github.com/$OWNER/$REPO/blob/$REF/$URL_PATH"
if [ -n "$LINE_SPEC" ]; then
  URL="$URL#$LINE_SPEC"
fi
printf '%s\n' "$URL"
