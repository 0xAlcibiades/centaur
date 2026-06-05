#!/usr/bin/env bun

import {
  chmodSync,
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
} from "node:fs";
import { homedir } from "node:os";
import { dirname, resolve } from "node:path";
import { spawnSync } from "node:child_process";

const CLIENT_ID = "9d1c250a-e61b-44d9-88ed-5944d1962f5e";
const DEFAULT_SCOPES = [
  "user:file_upload",
  "user:inference",
  "user:mcp_servers",
  "user:profile",
  "user:sessions:claude_code",
];

function usage() {
  console.log(`Usage:
  bun run scripts/claude-code-auth-local.js status
  bun run scripts/claude-code-auth-local.js login
  bun run scripts/claude-code-auth-local.js export --out /tmp/claude-code-auth.json
  bun run scripts/claude-code-auth-local.js install --from /tmp/claude-code-auth.json
  bun run scripts/claude-code-auth-local.js test
  CLAUDE_CODE_REFRESH_TOKEN=... bun run scripts/claude-code-auth-local.js install
  CLAUDE_CODE_BLOB='{"refresh_token":"..."}' bun run scripts/claude-code-auth-local.js install

Options:
  --config-dir DIR  Claude config dir to read/write. Defaults to $CLAUDE_CONFIG_DIR or ~/.claude.
  --out FILE        Export destination.
  --from FILE       Install source bundle or Claude credentials JSON.
  --prompt TEXT     Test prompt. Defaults to "Respond exactly: OK".
  --no-keychain     On macOS, skip updating the Claude Code Keychain item.
  --force           Overwrite existing target credentials.
`);
}

function option(name) {
  const index = process.argv.indexOf(name);
  if (index === -1) return "";
  const value = process.argv[index + 1] || "";
  if (!value || value.startsWith("--")) {
    throw new Error(`${name} requires a value`);
  }
  return value;
}

function hasFlag(name) {
  return process.argv.includes(name);
}

function targetConfigDir() {
  return resolve(
    option("--config-dir") ||
      process.env.CLAUDE_CONFIG_DIR ||
      resolve(homedir(), ".claude"),
  );
}

function targetCredentialsPath() {
  return resolve(targetConfigDir(), ".credentials.json");
}

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function writeJson(path, value) {
  mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 });
  chmodSync(path, 0o600);
}

function writeMacosKeychainCredentials(credentials) {
  if (process.platform !== "darwin" || hasFlag("--no-keychain")) return false;
  const result = spawnSync(
    "security",
    [
      "add-generic-password",
      "-U",
      "-s",
      "Claude Code-credentials",
      "-a",
      process.env.USER || "claude-code",
      "-w",
      JSON.stringify(credentials),
    ],
    { encoding: "utf8", stdio: ["ignore", "ignore", "pipe"] },
  );
  if (result.status !== 0) {
    throw new Error(
      `failed to update macOS Keychain item Claude Code-credentials: ${result.stderr.trim()}`,
    );
  }
  return true;
}

function oauthFromCredentials(path, value) {
  const credentials = typeof value === "string" ? JSON.parse(value) : value;
  const oauth = credentials?.claudeAiOauth;
  if (
    typeof oauth !== "object" ||
    oauth === null ||
    typeof oauth.refreshToken !== "string" ||
    !oauth.refreshToken
  ) {
    throw new Error(`${path} does not contain Claude Code OAuth credentials`);
  }
  return { path, credentials, oauth };
}

function credentialsFromRefreshToken(refreshToken) {
  return {
    claudeAiOauth: {
      accessToken: "",
      refreshToken,
      expiresAt: 0,
      scopes: DEFAULT_SCOPES,
      subscriptionType: "claude_max",
    },
  };
}

function readLocalCredentials() {
  const path = targetCredentialsPath();
  if (existsSync(path)) {
    return oauthFromCredentials(path, readJson(path));
  }

  if (process.platform === "darwin") {
    const result = spawnSync(
      "security",
      ["find-generic-password", "-s", "Claude Code-credentials", "-w"],
      { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] },
    );
    if (result.status === 0 && result.stdout.trim()) {
      return oauthFromCredentials(
        "macOS Keychain item Claude Code-credentials",
        result.stdout.trim(),
      );
    }
  }

  const envCredentials = (process.env.CLAUDE_CREDENTIALS_JSON || "").trim();
  if (envCredentials) {
    return oauthFromCredentials("CLAUDE_CREDENTIALS_JSON", envCredentials);
  }

  return null;
}

function credentialsFromBundle(path, value) {
  const parsed = typeof value === "string" ? JSON.parse(value) : value;
  if (parsed?.claudeAiOauth) {
    return oauthFromCredentials(path, parsed).credentials;
  }
  if (parsed?.credentials?.claudeAiOauth) {
    return oauthFromCredentials(path, parsed.credentials).credentials;
  }
  if (typeof parsed?.refresh_token === "string") {
    return credentialsFromRefreshToken(parsed.refresh_token);
  }
  if (typeof parsed?.refreshToken === "string") {
    return credentialsFromRefreshToken(parsed.refreshToken);
  }
  throw new Error(`${path} is not a Claude Code auth bundle`);
}

function credentialsFromEnv() {
  const blob = (process.env.CLAUDE_CODE_BLOB || "").trim();
  if (blob) {
    return credentialsFromBundle("CLAUDE_CODE_BLOB", blob);
  }
  const refreshToken = (
    process.env.CLAUDE_CODE_REFRESH_TOKEN ||
    process.env.CLAUDE_CODE_OAUTH_REFRESH_TOKEN ||
    ""
  ).trim();
  if (refreshToken) {
    return credentialsFromRefreshToken(refreshToken);
  }
  return null;
}

function redactedSummary(found) {
  const scopes = Array.isArray(found.oauth.scopes) ? found.oauth.scopes.join(" ") : "";
  return {
    source: found.path,
    client_id: CLIENT_ID,
    has_access_token:
      typeof found.oauth.accessToken === "string" &&
      found.oauth.accessToken.length > 0,
    has_refresh_token: true,
    expires_at: found.oauth.expiresAt || null,
    scopes,
    subscription_type: found.oauth.subscriptionType || "",
  };
}

function claudeEnv() {
  const env = { ...process.env };
  for (const name of [
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "CLAUDE_CODE_BLOB",
    "CLAUDE_CODE_CLIENT_ID",
    "CLAUDE_CODE_OAUTH_ACCESS_TOKEN",
    "CLAUDE_CODE_OAUTH_CLIENT_ID",
    "CLAUDE_CODE_OAUTH_REFRESH_TOKEN",
    "CLAUDE_CODE_OAUTH_SCOPES",
    "CLAUDE_CODE_OAUTH_TOKEN_SECRET_REF",
    "CLAUDE_CODE_REFRESH_TOKEN",
  ]) {
    delete env[name];
  }
  env.CLAUDE_CONFIG_DIR = targetConfigDir();
  return env;
}

function runStatus() {
  const found = readLocalCredentials();
  if (!found) {
    console.log(`Claude Code auth not found at ${targetCredentialsPath()} or in the macOS Keychain.`);
    process.exitCode = 1;
    return;
  }
  console.log(JSON.stringify(redactedSummary(found), null, 2));
}

function runLogin() {
  const result = spawnSync("claude", ["auth", "login"], {
    env: claudeEnv(),
    stdio: "inherit",
  });
  if (result.error) {
    throw new Error(`failed to run claude: ${result.error.message}`);
  }
  if (result.status !== 0) {
    process.exitCode = result.status ?? 1;
    return;
  }
  runStatus();
}

function runTest() {
  const prompt = option("--prompt") || "Respond exactly: OK";
  const result = spawnSync(
    "claude",
    [
      "-p",
      prompt,
      "--output-format",
      "json",
      "--setting-sources",
      "user",
      "--tools",
      "",
      "--no-session-persistence",
      "--model",
      "haiku",
      "--max-budget-usd",
      "0.01",
    ],
    {
      cwd: "/tmp",
      env: claudeEnv(),
      stdio: "inherit",
    },
  );
  if (result.error) {
    throw new Error(`failed to run claude: ${result.error.message}`);
  }
  if (result.status !== 0) {
    process.exitCode = result.status ?? 1;
  }
}

function runExport() {
  const out = option("--out");
  if (!out) throw new Error("export requires --out FILE");
  const found = readLocalCredentials();
  if (!found) {
    throw new Error("Claude Code auth not found; run `claude auth login` first");
  }
  writeJson(resolve(out), {
    type: "claude_code_auth",
    client_id: CLIENT_ID,
    blob: { refresh_token: found.oauth.refreshToken },
    credentials: found.credentials,
  });
  console.log(`Wrote ${resolve(out)}`);
}

function runInstall() {
  const from = option("--from");
  const target = targetCredentialsPath();
  if (existsSync(target) && !hasFlag("--force")) {
    throw new Error(`${target} already exists; pass --force to overwrite`);
  }

  const credentials = from
    ? credentialsFromBundle(resolve(from), readJson(resolve(from)))
    : credentialsFromEnv();
  if (!credentials) {
    throw new Error("install requires --from FILE, CLAUDE_CODE_BLOB, or CLAUDE_CODE_REFRESH_TOKEN");
  }
  oauthFromCredentials("install payload", credentials);
  writeJson(target, credentials);
  console.log(`Wrote ${target}`);
  if (writeMacosKeychainCredentials(credentials)) {
    console.log("Updated macOS Keychain item Claude Code-credentials");
  }
}

const command = process.argv[2] || "status";

try {
  if (command === "--help" || command === "-h" || command === "help") {
    usage();
  } else if (command === "status") {
    runStatus();
  } else if (command === "login") {
    runLogin();
  } else if (command === "test") {
    runTest();
  } else if (command === "export") {
    runExport();
  } else if (command === "install") {
    runInstall();
  } else {
    usage();
    process.exitCode = 2;
  }
} catch (error) {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
}
