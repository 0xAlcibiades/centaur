"""Executable contract for the immutable metadata-only trace sidecar."""

import base64
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from urllib.request import Request, urlopen
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import uuid4


IMAGE = (
    "us-west1-docker.pkg.dev/autorotate-iam-proof-442243/"
    "autorotate-trace-agent-public/autorotate-trace-agent"
    "@sha256:0fd1cdb92fed586d4fc3a85509a6b807ffab8ba75cc88e6cfd71eaac692ba25f"
)
GATEWAY_IMAGE = "python@sha256:25976e9d34a0fab1f278cae931f34c8303d97bf0c0d7f85b6b4dcf641d7702a4"
OTLP_FIXTURES = (
    # Each source-shaped OTLP fixture contains hostile raw prompt, provider,
    # tool, output, or identifier sentinels. They must never leave the agent.
    "CrEBCjwKHAoMc2VydmljZS5uYW1lEgwKCmNvZGV4X2V4ZWMKHAoPc2VydmljZS52ZXJzaW9uEgkKBzAuMTQ2LjAScRJvChADAwMDAwMDAwMDAwMDAwMDKgpjb2RleC5leGVjOXgADTEARscYQXjKp2wARscYSiIKCXRocmVhZC5pZBIVChNjb252ZXJzYXRpb24tc2VjcmV0ehkSFXByb3ZpZGVyIGVycm9yIHNlY3JldBgB",
    "CvsCCjwKHAoMc2VydmljZS5uYW1lEgwKCmNvZGV4X2V4ZWMKHAoPc2VydmljZS52ZXJzaW9uEgkKBzAuMTQ2LjASugIStwIKEBISEhISEhISEhISEhISEhISCCIiIiIiIiIiKg5tY3AudG9vbHMuY2FsbDkAAD7x7ideGkFAG50E7ydeGkosCg9jb252ZXJzYXRpb24uaWQSGQoXUkFXX0NPTlZFUlNBVElPTl9JRF9NQ1BKKAoPbWNwLnNlcnZlci5uYW1lEhUKE01DUF9TRVJWRVJfU0VOVElORUxKHAoJdG9vbC5uYW1lEg8KDVRPT0xfU0VOVElORUxKIgoMdG9vbC5jYWxsX2lkEhIKEFJBV19UT09MX0NBTExfSURKIgoKZXJyb3IudHlwZRIUChJFUlJPUl9TRU5USU5FTF9NQ1BKOQoOc2VydmVyLmFkZHJlc3MSJwolaHR0cHM6Ly9FTkRQT0lOVF9TRU5USU5FTF9NQ1AuaW52YWxpZA==",
    "CoUDCjwKHAoMc2VydmljZS5uYW1lEgwKCmNvZGV4X2V4ZWMKHAoPc2VydmljZS52ZXJzaW9uEgkKBzAuMTQ2LjASxAISwQIKEBERERERERERERERERERERESCCEhISEhISEhKgljb21wbGV0ZWQ5AAA+8e4nXhpBgHy/O+8nXhpKLgoPY29udmVyc2F0aW9uLmlkEhsKGVJBV19DT05WRVJTQVRJT05fSURfTU9ERUxKHwoZZ2VuX2FpLnVzYWdlLmlucHV0X3Rva2VucxICGGVKKgokZ2VuX2FpLnVzYWdlLmNhY2hlX3JlYWQuaW5wdXRfdG9rZW5zEgIYHUogChpnZW5fYWkudXNhZ2Uub3V0cHV0X3Rva2VucxICGC9KKQojY29kZXgudXNhZ2UucmVhc29uaW5nX291dHB1dF90b2tlbnMSAhgNSiEKBnByb21wdBIXChVQUk9NUFRfU0VOVElORUxfTU9ERUxKGQoFbW9kZWwSEAoOTU9ERUxfU0VOVElORUw=",
)
SECRET_SENTINELS = (
    "conversation-secret",
    "provider error secret",
    "PROMPT_SENTINEL_MODEL",
    "MODEL_SENTINEL",
    "RAW_CONVERSATION_ID_MCP",
    "MCP_SERVER_SENTINEL",
    "TOOL_SENTINEL",
    "RAW_TOOL_CALL_ID",
    "ERROR_SENTINEL_MCP",
    "ENDPOINT_SENTINEL_MCP",
)


def gateway_main(capture_dir: Path, active: bool) -> None:
    """Run the minimal authenticated gateway surface used by the final binary."""

    class GatewayHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - stdlib callback name
            with (capture_dir / "requests.log").open("a") as file:
                file.write(f"GET {self.path}\n")
            if self.path != "/v1/consent/status":
                self.send_error(404)
                return
            now = datetime.now(timezone.utc)
            body = json.dumps(
                {
                    "active": active,
                    "scope": "metadata",
                    "expires_at": (now + timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
                    "server_time": now.isoformat().replace("+00:00", "Z"),
                    "revision": 1,
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self) -> None:  # noqa: N802 - stdlib callback name
            if self.path != "/v1/events":
                self.send_error(404)
                return
            body = self.rfile.read(int(self.headers["Content-Length"]))
            capture = capture_dir / "events.jsonl"
            with capture.open("ab") as file:
                file.write(body + b"\n")
            self.send_response(202)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, _format: str, *_args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 18080), GatewayHandler)
    server.serve_forever()


def client_main(operation: str, endpoint: str, fixture: str | None = None) -> None:
    if operation == "status":
        request = Request(f"{endpoint}statusz")
    elif operation == "post" and fixture is not None:
        request = Request(
            f"{endpoint}v1/traces",
            data=base64.b64decode(fixture),
            headers={"Content-Type": "application/x-protobuf"},
            method="POST",
        )
    else:
        raise ValueError("invalid client invocation")
    with urlopen(request, timeout=5) as response:  # noqa: S310 - loopback capability
        print(response.status)


class AutorotateTraceAgentContractTests(unittest.TestCase):
    maxDiff = None

    def setUp(self) -> None:
        self.containers: list[str] = []

    def tearDown(self) -> None:
        for container in reversed(self.containers):
            subprocess.run(
                ["docker", "rm", "--force", container],
                check=False,
                capture_output=True,
                text=True,
            )

    def docker_run(self, *args: str) -> str:
        result = subprocess.run(
            ["docker", "run", "--detach", "--platform", "linux/amd64", *args],
            check=True,
            capture_output=True,
            text=True,
        )
        container = result.stdout.strip()
        self.containers.append(container)
        return container

    def start_gateway(self, capture_dir: Path, *, active: bool) -> str:
        self.capture_dir = capture_dir
        gateway = self.docker_run(
            "--volume",
            f"{Path(__file__).resolve()}:/contract.py:ro",
            "--volume",
            f"{capture_dir}:/capture",
            GATEWAY_IMAGE,
            "python",
            "/contract.py",
            "--gateway",
            "/capture",
            "active" if active else "revoked",
        )
        return gateway

    def start_agent(self, gateway: str) -> str:
        startup = """
            set -eu
            umask 077
            mkdir -p /var/trace/credentials /var/trace/spool /var/trace/capability
            printf 12345678901234567890123456789012 > /var/trace/credentials/bearer
            printf '%064d' 0 > /var/trace/credentials/pseudonym-key
            chmod 0600 /var/trace/credentials/bearer /var/trace/credentials/pseudonym-key
            exec /usr/local/bin/autorotate-trace-agent \\
              --listen 127.0.0.1:4318 \\
              --listen-address-file /var/trace/capability/otlp-endpoint \\
              --gateway-url http://127.0.0.1:18080 \\
              --bearer-token-file /var/trace/credentials/bearer \\
              --pseudonym-key-file /var/trace/credentials/pseudonym-key \\
              --spool-dir /var/trace/spool \\
              --source bojack
        """
        return self.docker_run(
            "--network",
            f"container:{gateway}",
            "--tmpfs",
            "/var/trace:uid=65532,gid=65532,mode=700",
            "--entrypoint",
            "/bin/sh",
            IMAGE,
            "-ec",
            startup,
        )

    def client_request(self, gateway: str, operation: str, endpoint: str, fixture: str | None = None) -> subprocess.CompletedProcess[str]:
        command = [
            "docker",
            "run",
            "--rm",
            "--platform",
            "linux/amd64",
            "--network",
            f"container:{gateway}",
            "--volume",
            f"{Path(__file__).resolve()}:/contract.py:ro",
            GATEWAY_IMAGE,
            "python",
            "/contract.py",
            "--client",
            operation,
            endpoint,
        ]
        if fixture is not None:
            command.append(fixture)
        return subprocess.run(command, check=False, capture_output=True, text=True)

    def wait_for_endpoint(self, agent: str) -> str:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            endpoint = subprocess.run(
                [
                    "docker",
                    "exec",
                    agent,
                    "/bin/sh",
                    "-ec",
                    "test -r /var/trace/capability/otlp-endpoint && endpoint=$(cat /var/trace/capability/otlp-endpoint) && test -n \"$endpoint\" && printf %s \"$endpoint\"",
                ],
                check=False,
                capture_output=True,
                text=True,
            ).stdout.strip()
            if endpoint.startswith("http://127.0.0.1:4318/") and endpoint.endswith("/"):
                return endpoint
            time.sleep(0.1)
        logs = subprocess.run(
            ["docker", "logs", self.containers[-1]],
            check=False,
            capture_output=True,
            text=True,
        )
        capability = subprocess.run(
            ["docker", "exec", self.containers[-1], "/bin/sh", "-ec", "ls -la /var/trace/capability; od -An -tc /var/trace/capability/otlp-endpoint 2>/dev/null || true"],
            check=False,
            capture_output=True,
            text=True,
        )
        requests = (self.capture_dir / "requests.log").read_text() if (self.capture_dir / "requests.log").exists() else "none"
        self.fail(f"trace agent did not publish an active loopback capability: {logs.stderr}; capability: {capability.stdout}; gateway requests: {requests}")

    def post_otlp(self, gateway: str, endpoint: str, fixture: str) -> None:
        body = bytearray(base64.b64decode(fixture))
        timestamp = time.time_ns()
        start = timestamp.to_bytes(8, "little")
        end = (timestamp + 1_000_000_000).to_bytes(8, "little")
        # Stock OTLP spans encode start/end timestamps as fixed64 fields 7/8.
        # The fixtures deliberately have stable bytes, so refresh only those
        # values before crossing the live seven-day validation boundary.
        offset = 0
        while (offset := body.find(b"\x39", offset)) >= 0:
            if body[offset + 9 : offset + 10] == b"\x41":
                body[offset + 1 : offset + 9] = start
                body[offset + 10 : offset + 18] = end
                offset += 18
            else:
                offset += 1
        current_fixture = base64.b64encode(body).decode()
        result = self.client_request(gateway, "post", endpoint, current_fixture)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "200")

    def read_gateway_events(self, capture_dir: Path, count: int) -> list[dict]:
        path = capture_dir / "events.jsonl"
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if path.exists():
                events = [json.loads(line) for line in path.read_text().splitlines()]
                if len(events) >= count:
                    return events
            time.sleep(0.1)
        self.fail(f"gateway received fewer than {count} normalized trace batches")

    def test_granted_consent_forwards_only_closed_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            capture_dir = Path(temporary)
            gateway = self.start_gateway(capture_dir, active=True)
            agent = self.start_agent(gateway)
            endpoint = self.wait_for_endpoint(agent)
            for fixture in OTLP_FIXTURES:
                self.post_otlp(gateway, endpoint, fixture)

            batches = self.read_gateway_events(capture_dir, len(OTLP_FIXTURES))
            payload = json.dumps(batches, sort_keys=True)
            for sentinel in SECRET_SENTINELS:
                self.assertNotIn(sentinel, payload)

            self.assertTrue(
                all(set(batch) == {"protocol_version", "events"} and batch["protocol_version"] == 1 for batch in batches),
                batches,
            )
            events = [event for batch in batches for event in batch["events"]]
            self.assertEqual(
                {event["event"]["kind"] for event in events},
                {"execution_started", "model_usage", "tool_use"},
            )
            self.assertTrue(all(set(event) == {"event_id", "execution", "thread", "account", "observed_at", "event"} for event in events))
            self.assertTrue(all(event["account"] is None for event in events))
            self.assertTrue(all(len(event["execution"]) == len(event["thread"]) == 64 for event in events))
            self.assertIn(
                {
                    "kind": "model_usage",
                    "attributes": {
                        "outcome": "succeeded",
                        "duration_ms": 1_000,
                        "input_tokens": 101,
                        "cached_input_tokens": 29,
                        "output_tokens": 47,
                        "reasoning_output_tokens": 13,
                    },
                },
                [event["event"] for event in events],
            )
            self.assertIn(
                {
                    "kind": "tool_use",
                    "attributes": {"category": "connector", "outcome": "failed", "duration_ms": 1_000},
                },
                [event["event"] for event in events],
            )

    def test_revoked_consent_never_opens_intake_or_blocks_the_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            gateway = self.start_gateway(Path(temporary), active=False)
            agent = self.start_agent(gateway)
            time.sleep(1)
            capability = subprocess.run(
                ["docker", "exec", agent, "/bin/sh", "-ec", "test ! -e /var/trace/capability/otlp-endpoint"],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(capability.returncode, 0)
            running = subprocess.run(
                ["docker", "inspect", "--format", "{{.State.Running}}", agent],
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertEqual(running.stdout.strip(), "true")

    def test_pinned_agent_binary_matches_reviewed_sha256(self) -> None:
        binary = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "--platform",
                "linux/amd64",
                "--entrypoint",
                "/usr/bin/sha256sum",
                IMAGE,
                "/usr/local/bin/autorotate-trace-agent",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            binary.stdout.split()[0],
            "a210d58bec0d61f7afc0c0415bda23ef6bd93c1e2a3a90e2559bdce6e2c23495",
        )


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--gateway":
        gateway_main(Path(sys.argv[2]), sys.argv[3] == "active")
    elif len(sys.argv) in (4, 5) and sys.argv[1] == "--client":
        client_main(sys.argv[2], sys.argv[3], sys.argv[4] if len(sys.argv) == 5 else None)
    else:
        unittest.main()
