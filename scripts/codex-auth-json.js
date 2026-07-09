#!/usr/bin/env bun

import {
  chmodSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";

function usage() {
  console.log(`Usage:
  bun run scripts/codex-auth-json.js mint --out /tmp/codex-auth.json
  bun run scripts/codex-auth-json.js status --auth /tmp/codex-auth.json
  bun run scripts/codex-auth-json.js test --auth /tmp/codex-auth.json

Options:
  --out FILE     Destination auth.json for mint.
  --auth FILE    Existing auth.json for status/test.
  --home DIR     Existing CODEX_HOME to use instead of a temp directory.
  --force        Overwrite --out if it exists.
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

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function writeJson(path, value) {
  mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 });
  chmodSync(path, 0o600);
}

function codexHome() {
  const requested = option("--home");
  if (requested) {
    const home = resolve(requested);
    mkdirSync(home, { recursive: true, mode: 0o700 });
    return home;
  }
  return mkdtempSync(join(tmpdir(), "codex-auth-home-"));
}

function envForHome(home) {
  const env = { ...process.env, CODEX_HOME: home };
  delete env.CODEX_API_KEY;
  delete env.CODEX_ACCESS_TOKEN;
  delete env.OPENAI_API_KEY;
  delete env.OPENAI_CODEX_ACCOUNT_ID;
  delete env.OPENAI_CODEX_BLOB;
  delete env.OPENAI_CODEX_CLIENT_ID;
  return env;
}

function authSummary(path) {
  const auth = readJson(path);
  const tokens = auth?.tokens || {};
  return {
    path,
    auth_mode: auth?.auth_mode || "",
    has_access_token: typeof tokens.access_token === "string" && tokens.access_token.length > 0,
    has_refresh_token: typeof tokens.refresh_token === "string" && tokens.refresh_token.length > 0,
    account_id: tokens.account_id || "",
    last_refresh: auth?.last_refresh || "",
  };
}

function runCodex(args, home) {
  const result = spawnSync("codex", args, {
    env: envForHome(home),
    stdio: "inherit",
  });
  if (result.error) {
    throw new Error(`failed to run codex: ${result.error.message}`);
  }
  if (result.status !== 0) {
    process.exitCode = result.status ?? 1;
    return false;
  }
  return true;
}

function runMint() {
  const out = option("--out");
  if (!out) throw new Error("mint requires --out FILE");
  const target = resolve(out);
  if (existsSync(target) && !hasFlag("--force")) {
    throw new Error(`${target} already exists; pass --force to overwrite`);
  }

  const home = codexHome();
  if (!runCodex(["login", "--device-auth"], home)) return;

  const authPath = join(home, "auth.json");
  if (!existsSync(authPath)) {
    throw new Error(`codex login completed but ${authPath} was not created`);
  }
  writeJson(target, readJson(authPath));
  console.log(`Wrote ${target}`);
  console.log(JSON.stringify(authSummary(target), null, 2));
}

function materializeAuthHome() {
  const auth = option("--auth");
  if (!auth) throw new Error(`${process.argv[2]} requires --auth FILE`);
  const source = resolve(auth);
  if (!existsSync(source)) throw new Error(`${source} does not exist`);
  const home = codexHome();
  writeJson(join(home, "auth.json"), readJson(source));
  return { home, source };
}

function runStatus() {
  const auth = option("--auth");
  if (!auth) throw new Error("status requires --auth FILE");
  console.log(JSON.stringify(authSummary(resolve(auth)), null, 2));
}

function runTest() {
  const { home } = materializeAuthHome();
  runCodex(
    [
      "exec",
      "--cd",
      "/tmp",
      "--ask-for-approval",
      "never",
      "--sandbox",
      "read-only",
      "Respond exactly: OK",
    ],
    home,
  );
}

const command = process.argv[2] || "help";

try {
  if (command === "--help" || command === "-h" || command === "help") {
    usage();
  } else if (command === "mint") {
    runMint();
  } else if (command === "status") {
    runStatus();
  } else if (command === "test") {
    runTest();
  } else {
    usage();
    process.exitCode = 2;
  }
} catch (error) {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
}
