import httpx
import pytest
from client import (
    SANDBOX_AUTOROTATE_STATUS_PATH,
    SANDBOX_OAUTH_APPS_PATH,
    SANDBOX_PERMISSIONS_PATH,
    ConsoleClient,
)


def json_response(payload, status_code=200):
    return httpx.Response(status_code, json=payload)


def make_client(handler, *, bearer_token=None):
    return ConsoleClient(
        url="http://centaur-console:3000",
        bearer_token=bearer_token,
        transport=httpx.MockTransport(handler),
    )


def test_sandbox_permissions_fetches_and_unwraps_data():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == SANDBOX_PERMISSIONS_PATH
        assert request.headers["Accept"] == "application/json"
        return json_response(
            {
                "data": {
                    "sandbox_id": "sandbox-1",
                    "principal_id": "prn_123",
                    "permissions": {"secrets": []},
                }
            }
        )

    result = make_client(handler).sandbox_permissions()

    assert result["sandbox_id"] == "sandbox-1"
    assert result["principal_id"] == "prn_123"
    assert result["permissions"] == {"secrets": []}


def test_sandbox_permissions_sends_debug_bearer_token_when_provided():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer test-token"
        return json_response({"data": {"sandbox_id": "sandbox-1"}})

    assert (
        make_client(handler, bearer_token="test-token").permissions()["sandbox_id"] == "sandbox-1"
    )


def test_sandbox_permissions_wraps_http_errors():
    def handler(_request: httpx.Request) -> httpx.Response:
        return json_response({"error": {"message": "invalid sandbox token"}}, status_code=401)

    with pytest.raises(RuntimeError, match="HTTP 401"):
        make_client(handler).sandbox_permissions()


def test_sandbox_oauth_apps_fetches_and_unwraps_data():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == SANDBOX_OAUTH_APPS_PATH
        assert request.headers["Accept"] == "application/json"
        return json_response(
            {
                "data": [
                    {
                        "slug": "google",
                        "provider": "google",
                        "start_url": "https://console.example/oauth/google/start",
                    }
                ]
            }
        )

    result = make_client(handler).sandbox_oauth_apps()

    assert result == [
        {
            "slug": "google",
            "provider": "google",
            "start_url": "https://console.example/oauth/google/start",
        }
    ]


def test_sandbox_oauth_apps_wraps_http_errors():
    def handler(_request: httpx.Request) -> httpx.Response:
        return json_response({"error": {"message": "invalid sandbox token"}}, status_code=401)

    with pytest.raises(RuntimeError, match="HTTP 401"):
        make_client(handler).sandbox_oauth_apps()


def test_autorotate_status_fetches_and_unwraps_redacted_pool_data():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == SANDBOX_AUTOROTATE_STATUS_PATH
        return json_response(
            {
                "data": {
                    "generated_at": "2026-07-29T12:00:00Z",
                    "total": 5,
                    "healthy": 4,
                    "available": 3,
                    "limited": 1,
                    "login_required": 0,
                    "disabled": 0,
                    "leased": 1,
                    "removed": 0,
                    "next_available_at": None,
                }
            }
        )

    result = make_client(handler).autorotate_status()

    assert result["total"] == 5
    assert result["available"] == 3
    assert result["limited"] == 1


def test_autorotate_requests_wrap_http_errors():
    def handler(_request: httpx.Request) -> httpx.Response:
        return json_response({"error": {"message": "forbidden"}}, status_code=403)

    with pytest.raises(RuntimeError, match="Autorotate status request failed: HTTP 403"):
        make_client(handler).autorotate_status()


def test_health_returns_identity_details():
    def handler(_request: httpx.Request) -> httpx.Response:
        return json_response(
            {
                "data": {
                    "sandbox_id": "sandbox-1",
                    "proxy_id": "proxy-1",
                    "principal_id": "principal-1",
                }
            }
        )

    result = make_client(handler).health()

    assert result == {
        "ok": True,
        "tool": "centaur-console",
        "error": None,
        "details": {
            "sandbox_id": "sandbox-1",
            "proxy_id": "proxy-1",
            "principal_id": "principal-1",
        },
    }
