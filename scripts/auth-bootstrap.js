#!/usr/bin/env bun

import { chmodSync, existsSync, readFileSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { resolve } from "node:path";
import { spawnSync } from "node:child_process";

const envFile = resolve(
  process.cwd(),
  process.env.AUTH_BOOTSTRAP_ENV_FILE || ".env.local",
);
const home = homedir();
const loginRequested = process.argv.slice(2).includes("--login");
const CLAUDE_CODE_OAUTH_CLIENT_ID = "9d1c250a-e61b-44d9-88ed-5944d1962f5e";

function readJson(path) {
  if (!existsSync(path)) return null;
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch (error) {
    throw new Error(`${path} is not valid JSON: ${error.message}`);
  }
}

function shellQuote(value) {
  return `'${String(value).replaceAll("'", "'\\''")}'`;
}

function upsertEnvValues(path, values) {
  const lines = existsSync(path) ? readFileSync(path, "utf8").split(/\r?\n/) : [];
  const used = new Set();
  const next = lines.map((line) => {
    const match = /^(export\s+)?([A-Za-z_][A-Za-z0-9_]*)=/.exec(line);
    if (!match || !(match[2] in values)) return line;
    used.add(match[2]);
    return `export ${match[2]}=${shellQuote(values[match[2]])}`;
  });

  for (const [key, value] of Object.entries(values)) {
    if (!used.has(key)) next.push(`export ${key}=${shellQuote(value)}`);
  }

  while (next.length > 0 && next[next.length - 1] === "") next.pop();
  writeFileSync(path, `${next.join("\n")}\n`, { mode: 0o600 });
  chmodSync(path, 0o600);
}

function codexPayload() {
  const path = resolve(home, ".codex", "auth.json");
  const auth = readJson(path);
  if (!auth) return null;
  return { path, value: JSON.stringify(auth) };
}

function claudeCredentialsFromValue(path, value) {
  const credentials = typeof value === "string" ? JSON.parse(value) : value;
  const oauth = credentials?.claudeAiOauth;
  if (
    typeof oauth !== "object" ||
    oauth === null ||
    typeof oauth.accessToken !== "string" ||
    typeof oauth.refreshToken !== "string"
  ) {
    throw new Error(`${path} does not look like Claude Code OAuth credentials`);
  }
  return {
    path,
    value: JSON.stringify(credentials),
    refreshToken: oauth.refreshToken,
  };
}

function claudeCredentialsFromFile(path) {
  if (!existsSync(path)) return null;
  return claudeCredentialsFromValue(path, readFileSync(path, "utf8"));
}

function claudeCredentialsFromKeychain() {
  if (process.platform !== "darwin") return null;
  const result = spawnSync(
    "security",
    ["find-generic-password", "-s", "Claude Code-credentials", "-w"],
    { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] },
  );
  if (result.status !== 0 || !result.stdout.trim()) return null;
  return claudeCredentialsFromValue(
    "macOS Keychain item Claude Code-credentials",
    result.stdout.trim(),
  );
}

function claudeCredentialsPayload() {
  const configDir = process.env.CLAUDE_CONFIG_DIR
    ? resolve(process.env.CLAUDE_CONFIG_DIR)
    : resolve(home, ".claude");
  const localCredentials =
    claudeCredentialsFromFile(resolve(configDir, ".credentials.json")) ||
    claudeCredentialsFromKeychain();
  if (localCredentials) return localCredentials;

  const envPayload = (process.env.CLAUDE_CREDENTIALS_JSON || "").trim();
  if (envPayload) {
    return claudeCredentialsFromValue("CLAUDE_CREDENTIALS_JSON", envPayload);
  }
  return null;
}

const updates = {};
const imported = [];
const missing = [];
const loginCommands = [];

const codex = codexPayload();
if (codex) {
  updates.CODEX_AUTH_JSON = codex.value;
  imported.push(["Codex", "CODEX_AUTH_JSON", codex.path]);
} else {
  loginCommands.push(["Codex", "codex", ["login", "--device-auth"]]);
  missing.push([
    "Codex",
    [
      "Run `codex login --device-auth` on the host,",
      "or `bun run auth:bootstrap -- --login` to stream the device flow.",
      "For SSH sessions, open the device URL printed by the Codex CLI,",
      "then rerun `bun run auth:bootstrap`.",
    ].join(" "),
  ]);
}

const claudeCredentials = claudeCredentialsPayload();
if (claudeCredentials) {
  updates.CLAUDE_CODE_OAUTH_CLIENT_ID = CLAUDE_CODE_OAUTH_CLIENT_ID;
  updates.CLAUDE_CODE_OAUTH_REFRESH_TOKEN = claudeCredentials.refreshToken;
  imported.push([
    "Claude Code OAuth refresh token",
    "CLAUDE_CODE_OAUTH_REFRESH_TOKEN",
    claudeCredentials.path,
  ]);
  imported.push([
    "Claude Code OAuth client id",
    "CLAUDE_CODE_OAUTH_CLIENT_ID",
    claudeCredentials.path,
  ]);
} else {
  loginCommands.push(["Claude", "claude", ["auth", "login"]]);
  missing.push([
    "Claude",
    [
      "Run `claude auth login` on the host,",
      "or `bun run auth:bootstrap -- --login` to run Claude Code login.",
      "On macOS, Centaur imports the Claude Code Keychain credential.",
      "On Linux, Centaur imports $CLAUDE_CONFIG_DIR/.credentials.json or ~/.claude/.credentials.json.",
      "then rerun `bun run auth:bootstrap`.",
    ].join(" "),
  ]);
}

if (Object.keys(updates).length > 0) {
  upsertEnvValues(envFile, updates);
  console.log(`Wrote ${envFile}`);
}

for (const [name, key, path] of imported) {
  console.log(`${name}: imported ${path} into ${key}=[redacted]`);
}

for (const [name, instruction] of missing) {
  console.log(`${name}: local auth not found. ${instruction}`);
}

if (loginRequested && loginCommands.length > 0) {
  for (const [name, command, args] of loginCommands) {
    console.log(`${name}: running ${[command, ...args].join(" ")}`);
    const result = spawnSync(command, args, { encoding: "utf8", stdio: "inherit" });
    if (result.error) {
      console.error(`${name}: failed to run ${command}: ${result.error.message}`);
      process.exitCode = 1;
    } else if (result.status !== 0) {
      console.error(`${name}: ${command} exited with status ${result.status}`);
      process.exitCode = result.status ?? 1;
    } else if (command === "claude" && args[0] === "auth") {
      const credentials = claudeCredentialsPayload();
      if (credentials) {
        upsertEnvValues(envFile, {
          CLAUDE_CODE_OAUTH_CLIENT_ID: CLAUDE_CODE_OAUTH_CLIENT_ID,
          CLAUDE_CODE_OAUTH_REFRESH_TOKEN: credentials.refreshToken,
        });
        console.log(`Wrote ${envFile}`);
        console.log(
          `Claude Code OAuth refresh token: imported ${credentials.path} into CLAUDE_CODE_OAUTH_REFRESH_TOKEN=[redacted]`,
        );
        console.log(
          `Claude Code OAuth client id: imported ${credentials.path} into CLAUDE_CODE_OAUTH_CLIENT_ID=[redacted]`,
        );
      } else {
        console.error("Claude: login completed but Claude Code credentials were not found.");
        process.exitCode = 1;
      }
    }
  }
  console.log("Rerun `bun run auth:bootstrap` after login completes.");
}

if (imported.length > 0) {
  console.log(
    "Enable opt-in use with CODEX_USE_LOCAL_AUTH=true or CLAUDE_USE_LOCAL_AUTH=true in the API deployment env.",
  );
  console.log(
    "For local Kubernetes, source .env.local before running just bootstrap-secrets so Claude refresh-token fields reach centaur-harness-auth.",
  );
  console.log(
    "For Codex proxy auth, store CODEX_AUTH_JSON in the configured 1Password field used by iron-proxy writeback.",
  );
}
