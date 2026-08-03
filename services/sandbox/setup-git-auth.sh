#!/bin/sh
set -eu

# Git encodes HTTPS credentials into Authorization: Basic, so iron-proxy's
# literal placeholder replacement cannot repair the header. Prefer the
# secret-backed token file that is already mounted for private tool sources.
# Placeholder-only sandboxes deliberately remain unauthenticated for public
# Git traffic instead of persisting GITHUB_TOKEN=GITHUB_TOKEN.

HOME_DIR=${HOME:?HOME must be set}
ASKPASS="$HOME_DIR/.git-askpass"
TOKEN_FILE=${CENTAUR_TOOLS_GITHUB_TOKEN_FILE:-}

# Remove auth state created by older images before installing the supported
# askpass path. The token itself is never copied into the Git config or script.
git config --global --unset-all credential.helper 2>/dev/null || true
rm -f "$HOME_DIR/.git-credentials"

if [ -n "$TOKEN_FILE" ] && [ -s "$TOKEN_FILE" ]; then
    cat > "$ASKPASS" <<EOF
#!/bin/sh
case "\$1" in
  *Username*) printf '%s\\n' x-access-token ;;
  *Password*) cat "$TOKEN_FILE" ;;
  *) printf '\\n' ;;
esac
EOF
    chmod 700 "$ASKPASS"
    git config --global core.askPass "$ASKPASS"
    exit 0
fi

# Keep local development usable when a real token is explicitly supplied, but
# never treat the proxy placeholder as a Git credential.
if [ -n "${GITHUB_TOKEN:-}" ] && [ "$GITHUB_TOKEN" != "GITHUB_TOKEN" ]; then
    cat > "$ASKPASS" <<'EOF'
#!/bin/sh
case "$1" in
  *Username*) printf '%s\n' x-access-token ;;
  *Password*) printf '%s\n' "$GITHUB_TOKEN" ;;
  *) printf '\n' ;;
esac
EOF
    chmod 700 "$ASKPASS"
    git config --global core.askPass "$ASKPASS"
else
    rm -f "$ASKPASS"
    git config --global --unset-all core.askPass 2>/dev/null || true
fi
