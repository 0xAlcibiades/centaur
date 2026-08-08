import os
from pathlib import Path
import subprocess
import sys
import tempfile
import tomllib
import unittest


class MetadataTraceEntrypointTests(unittest.TestCase):
    def setUp(self) -> None:
        self.script = (Path(__file__).parent / "entrypoint.sh").read_text()
        self.dockerfile = (Path(__file__).parent / "Dockerfile").read_text()

    def trace_python_heredocs(self) -> list[str]:
        start = self.script.index("configure_codex_metadata_trace()")
        end = self.script.index("# ── Claude Code settings", start)
        launcher = self.script[start:end]
        return [
            heredoc.split("PYEOF", 1)[0]
            for heredoc in launcher.split("<<'PYEOF'\n")[1:]
        ]

    def test_trace_launcher_is_bounded_and_fail_open(self) -> None:
        self.assertIn("configure_codex_metadata_trace()", self.script)
        self.assertIn("CENTAUR_CODEX_METADATA_TRACE_WAIT_SECONDS:-5", self.script)
        self.assertIn("continuing without trace export", self.script)
        self.assertIn("http://127.0.0.1:*|http://[::1]:*", self.script)

    def test_only_codex_gets_metadata_trace_config_without_auth_changes(self) -> None:
        start = self.script.index("configure_codex_metadata_trace()")
        end = self.script.index("# ── Claude Code settings", start)
        launcher = self.script[start:end]
        self.assertIn('[ "${1:-}" = "harness-server" ] && [ "${2:-}" = "codex" ]', launcher)
        self.assertIn('"exporter": "none"', launcher)
        self.assertIn('"log_user_prompt": False', launcher)
        self.assertIn("metrics_exporter", launcher)
        self.assertIn('"trace_exporter": "none"', launcher)
        self.assertIn('case "$name" in OTEL_*) unset "$name" ;; esac', launcher)
        self.assertGreaterEqual(launcher.count("tomllib.loads"), 2)
        self.assertGreaterEqual(launcher.count('config.pop("otel", None)'), 2)
        self.assertIn('CENTAUR_SANDBOX_METADATA_TRACE_ENABLED:-false', launcher)
        self.assertNotIn("auth.json", launcher)
        self.assertNotIn("OTEL_EXPORTER_OTLP_HEADERS", launcher)

    def test_codex_suppresses_generic_otel_before_consent_gate(self) -> None:
        start = self.script.index("configure_codex_metadata_trace()")
        end = self.script.index("# ── Claude Code settings", start)
        launcher = self.script[start:end]
        suppression = launcher.index('"trace_exporter": "none"')
        consent_gate = launcher.index('[ "${CENTAUR_SANDBOX_METADATA_TRACE_ENABLED:-false}" = "true" ] || return 0')
        self.assertLess(suppression, consent_gate)

    def test_trace_config_heredocs_compile_and_rewrite_otel_config(self) -> None:
        default_config, consented_config = self.trace_python_heredocs()
        compile(default_config, "default-trace-config", "exec")
        compile(consented_config, "consented-trace-config", "exec")

        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.toml"
            config_path.write_text('[otel]\nexporter = "otlp"\n\n[otel.extra]\nvalue = true\n')
            env = os.environ | {"CODEX_CONFIG_PATH": str(config_path)}

            subprocess.run([sys.executable, "-c", default_config], check=True, env=env)
            self.assertEqual(
                '[otel]\nexporter = "none"\nlog_user_prompt = false\nmetrics_exporter = "none"\ntrace_exporter = "none"',
                config_path.read_text().strip(),
            )

            for endpoint in (
                "http://127.0.0.1:4318/v1/traces",
                "http://127.0.0.1:4318/v1/traces/",
                "http://127.0.0.1:4318",
            ):
                with self.subTest(endpoint=endpoint):
                    config_path.write_text('[otel]\nexporter = "otlp"\n')
                    subprocess.run(
                        [sys.executable, "-c", consented_config],
                        check=True,
                        env=env | {"CODEX_TRACE_ENDPOINT": endpoint},
                    )
                    # Parse the generated config instead of matching its TOML
                    # spelling: this is the exact Codex 0.146 contract.
                    config = tomllib.loads(config_path.read_text())
                    self.assertEqual(
                        {
                            "exporter": "none",
                            "log_user_prompt": False,
                            "metrics_exporter": "none",
                            "trace_exporter": {
                                "otlp-http": {
                                    "endpoint": "http://127.0.0.1:4318/v1/traces",
                                    "protocol": "binary",
                                }
                            },
                        },
                        config["otel"],
                    )

    def test_trace_capability_gets_one_signal_path(self) -> None:
        _, consented_config = self.trace_python_heredocs()
        self.assertIn('endpoint = os.environ["CODEX_TRACE_ENDPOINT"].rstrip("/")', consented_config)
        self.assertIn('if not endpoint.endswith("/v1/traces"):', consented_config)
        self.assertIn('endpoint = f"{endpoint}/v1/traces"', consented_config)

    def test_trace_config_rewrite_removes_inline_and_commented_otel_forms(self) -> None:
        default_config, _ = self.trace_python_heredocs()
        for source in (
            'model = "gpt-5"\notel = { exporter = "otlp", log_user_prompt = true }\n',
            'model = "gpt-5"\n[otel] # inherited exporter\nexporter = "otlp"\n',
        ):
            with self.subTest(source=source), tempfile.TemporaryDirectory() as temp_dir:
                config_path = Path(temp_dir) / "config.toml"
                config_path.write_text(source)
                subprocess.run(
                    [sys.executable, "-c", default_config],
                    check=True,
                    env=os.environ | {"CODEX_CONFIG_PATH": str(config_path)},
                )
                config = tomllib.loads(config_path.read_text())
                self.assertEqual("gpt-5", config["model"])
                self.assertEqual("none", config["otel"]["exporter"])
                self.assertFalse(config["otel"]["log_user_prompt"])

    def test_codex_0146_keeps_chatgpt_auth_modes_outside_trace_setup(self) -> None:
        self.assertIn("ARG CODEX_VERSION=0.146.0", self.dockerfile)
        self.assertIn('codex --version | grep -F "${CODEX_VERSION}"', self.dockerfile)
        self.assertIn('CODEX_AUTH_MODE="${CODEX_AUTH_MODE:-api_key}"', self.script)
        self.assertIn('[ "$CODEX_AUTH_MODE" = "access_token" ] || [ "$CODEX_AUTH_MODE" = "autorotate" ]', self.script)
        self.assertIn('[ "$CODEX_AUTH_MODE" != "access_token" ] && [ "$CODEX_AUTH_MODE" != "autorotate" ]', self.script)
        self.assertIn('codex login --with-api-key', self.script)
        self.assertNotIn("AUTOROTATE_PROXY_PARENT_TOKEN", self.script)


if __name__ == "__main__":
    unittest.main()
