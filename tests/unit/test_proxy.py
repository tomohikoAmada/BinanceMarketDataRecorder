from __future__ import annotations

import asyncio
import json
import threading
import urllib.error
import urllib.request
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

from binance_market_data_recorder.binance.spot import exchange_info
from binance_market_data_recorder.binance.spot import websocket as spot_websocket
from binance_market_data_recorder.binance.usdm import rest as usdm_rest
from binance_market_data_recorder.binance.usdm import side_data_rest
from binance_market_data_recorder.binance.usdm import websocket as usdm_websocket
from binance_market_data_recorder.config import ConfigurationError, load_config
from binance_market_data_recorder.network import (
    ProxyConfigurationError,
    ProxyPolicy,
)


@pytest.mark.parametrize(
    ("mode", "url", "expected"),
    [
        ("direct", None, None),
        ("environment", None, True),
        ("explicit", "http://127.0.0.1:7890", "http://127.0.0.1:7890"),
    ],
)
def test_websocket_proxy_modes(
    mode: str,
    url: str | None,
    expected: str | bool | None,
) -> None:
    policy = ProxyPolicy(mode, url)  # type: ignore[arg-type]
    assert policy.websocket_proxy("wss://stream.binance.com:443/ws") == expected


def test_direct_ignores_proxy_environment_for_urllib_and_sdk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[object] = []

    def fake_build_opener(*handlers: object) -> Any:
        observed.extend(handlers)
        return object()

    monkeypatch.setattr(urllib.request, "build_opener", fake_build_opener)
    policy = ProxyPolicy(
        "direct",
        environment={"HTTPS_PROXY": "http://192.0.2.10:8888"},
    )
    assert policy.urllib_proxy_map("https://api.binance.com") == {}
    assert policy.sdk_proxy("https://api.binance.com") is None
    policy.urllib_opener("https://api.binance.com")
    handler = next(
        handler for handler in observed if isinstance(handler, urllib.request.ProxyHandler)
    )
    assert vars(handler)["proxies"] == {}


def test_environment_lowercase_precedence_no_proxy_and_sdk_mapping() -> None:
    policy = ProxyPolicy(
        "environment",
        environment={
            "HTTPS_PROXY": "http://192.0.2.20:9000",
            "https_proxy": "https://127.0.0.1:7890",
            "no_proxy": "api.binance.com",
        },
    )
    assert policy.sdk_proxy("https://api.binance.com") is None
    assert policy.sdk_proxy("https://fapi.binance.com") == {
        "host": "127.0.0.1",
        "port": 7890,
        "protocol": "https",
    }


def test_environment_wss_proxy_is_sdk_fallback_for_public_https() -> None:
    policy = ProxyPolicy(
        "environment",
        environment={"wss_proxy": "http://127.0.0.1:7890"},
    )
    assert policy.websocket_proxy("wss://fstream.binance.com/public/ws") is True
    assert policy.sdk_proxy("https://fapi.binance.com") == {
        "host": "127.0.0.1",
        "port": 7890,
        "protocol": "http",
    }


def test_explicit_urllib_and_sdk_share_one_validated_proxy() -> None:
    policy = ProxyPolicy("explicit", "http://127.0.0.1:7890")
    assert policy.urllib_proxy_map("https://api.binance.com") == {
        "http": "http://127.0.0.1:7890",
        "https": "http://127.0.0.1:7890",
    }
    assert policy.sdk_proxy("https://api.binance.com") == {
        "host": "127.0.0.1",
        "port": 7890,
        "protocol": "http",
    }


@pytest.mark.parametrize(
    "url",
    [
        "socks5://127.0.0.1:7890",
        "http://user:password@127.0.0.1:7890",
        "http://127.0.0.1:99999",
        "http://127.0.0.1:7890/path",
    ],
)
def test_explicit_rejects_unsupported_or_sensitive_urls_without_echo(url: str) -> None:
    with pytest.raises(ProxyConfigurationError) as caught:
        ProxyPolicy("explicit", url)
    assert url not in str(caught.value)
    assert "password" not in str(caught.value)


def test_explicit_requires_url_and_direct_rejects_one() -> None:
    with pytest.raises(ProxyConfigurationError):
        ProxyPolicy("explicit")
    with pytest.raises(ProxyConfigurationError):
        ProxyPolicy("direct", "http://127.0.0.1:7890")


def test_config_and_status_never_render_raw_proxy_url(tmp_path: Any) -> None:
    config_file = tmp_path / "recorder.toml"
    raw_url = "http://proxy-sensitive.invalid:8123"
    config_file.write_text(
        "[recorder]\n"
        'data_root = "/var/lib/bmdr-test"\n'
        'network_proxy_mode = "explicit"\n'
        f'network_proxy_url = "{raw_url}"\n',
        encoding="utf-8",
    )
    loaded = load_config(
        config_file=config_file,
        environ={},
        home=tmp_path / "home",
        repository_root=tmp_path / "workspace" / "repo",
    )
    rendered = json.dumps(loaded.config.public_dict(), sort_keys=True)
    assert raw_url not in rendered
    assert loaded.config.public_dict()["proxy_port"] == 8123
    assert loaded.config.public_dict()["proxy_loopback"] is False


class _FakeWebSocket:
    pass


@pytest.mark.parametrize(
    ("module", "function_name"),
    [
        (spot_websocket, "open_spot_websocket"),
        (usdm_websocket, "open_usdm_websocket"),
    ],
)
def test_websocket_openers_forward_only_policy_value(
    monkeypatch: pytest.MonkeyPatch,
    module: Any,
    function_name: str,
) -> None:
    observed: dict[str, object] = {}

    @asynccontextmanager
    async def fake_connect(url: str, **kwargs: object) -> AsyncIterator[_FakeWebSocket]:
        observed.update({"url": url, **kwargs})
        yield _FakeWebSocket()

    monkeypatch.setattr(module, "connect", fake_connect)

    async def exercise() -> None:
        opener = getattr(module, function_name)
        async with opener(
            "wss://example.invalid/ws",
            proxy="http://127.0.0.1:7890",
        ):
            pass

    asyncio.run(exercise())
    assert observed["proxy"] == "http://127.0.0.1:7890"


@pytest.mark.parametrize(
    "factory_module",
    [exchange_info, usdm_rest, side_data_rest],
)
def test_sdk_factory_receives_policy_proxy(
    monkeypatch: pytest.MonkeyPatch,
    factory_module: Any,
) -> None:
    observed: dict[str, object] = {}

    class FakeConfiguration:
        def __init__(self, **kwargs: object) -> None:
            observed.update(kwargs)

    class FakeSdk:
        def __init__(self, *, config_rest_api: object) -> None:
            class FakeSession:
                trust_env = True

            class FakeRestApi:
                def __init__(self) -> None:
                    self._session = FakeSession()

            self.rest_api = FakeRestApi()

    monkeypatch.setattr(factory_module, "ConfigurationRestAPI", FakeConfiguration)
    sdk_name = (
        "Spot"
        if factory_module is exchange_info
        else "DerivativesTradingUsdsFutures"
    )
    monkeypatch.setattr(factory_module, sdk_name, FakeSdk)
    policy = ProxyPolicy("explicit", "http://127.0.0.1:7890")
    if factory_module is exchange_info:
        factory_module.create_spot_exchange_info_api(
            timeout_ms=10_000,
            proxy_policy=policy,
        )
    elif factory_module is usdm_rest:
        factory_module.create_usdm_rest_api(
            timeout_ms=10_000,
            proxy_policy=policy,
        )
    else:
        factory_module.create_usdm_side_rest_api(
            timeout_ms=10_000,
            proxy_policy=policy,
        )
    assert observed["proxy"] == {
        "host": "127.0.0.1",
        "port": 7890,
        "protocol": "http",
    }


def test_sdk_session_environment_inheritance_matches_mode() -> None:
    class FakeSession:
        trust_env = True

    class FakeRestApi:
        def __init__(self) -> None:
            self._session = FakeSession()

    direct_api = FakeRestApi()
    ProxyPolicy("direct").configure_sdk_rest_api(direct_api)
    assert direct_api._session.trust_env is False

    environment_api = FakeRestApi()
    ProxyPolicy("environment", environment={}).configure_sdk_rest_api(
        environment_api
    )
    assert environment_api._session.trust_env is True

    explicit_api = FakeRestApi()
    ProxyPolicy(
        "explicit",
        "http://127.0.0.1:7890",
    ).configure_sdk_rest_api(explicit_api)
    assert explicit_api._session.trust_env is False


class _ConnectFailureHandler(BaseHTTPRequestHandler):
    observed_connect = threading.Event()

    def do_CONNECT(self) -> None:
        self.observed_connect.set()
        self.send_error(502, "injected CONNECT failure")

    def log_message(self, _format: str, *args: object) -> None:
        pass


def test_local_mock_connect_proxy_failure_is_visible() -> None:
    _ConnectFailureHandler.observed_connect.clear()
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ConnectFailureHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        port = server.server_address[1]
        policy = ProxyPolicy("explicit", f"http://127.0.0.1:{port}")
        request = urllib.request.Request("https://example.invalid/")
        with pytest.raises(urllib.error.URLError):
            policy.urllib_opener(request.full_url).open(request, timeout=1)
        assert _ConnectFailureHandler.observed_connect.wait(timeout=1)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


class _ConnectTimeoutHandler(BaseHTTPRequestHandler):
    observed_connect = threading.Event()
    release = threading.Event()

    def do_CONNECT(self) -> None:
        self.observed_connect.set()
        self.release.wait(timeout=2)

    def log_message(self, _format: str, *args: object) -> None:
        pass


def test_local_mock_connect_proxy_timeout_is_visible_and_bounded() -> None:
    _ConnectTimeoutHandler.observed_connect.clear()
    _ConnectTimeoutHandler.release.clear()
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ConnectTimeoutHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        port = server.server_address[1]
        policy = ProxyPolicy("explicit", f"http://127.0.0.1:{port}")
        request = urllib.request.Request("https://example.invalid/")
        with pytest.raises((TimeoutError, urllib.error.URLError)):
            policy.urllib_opener(request.full_url).open(request, timeout=0.1)
        assert _ConnectTimeoutHandler.observed_connect.wait(timeout=1)
    finally:
        _ConnectTimeoutHandler.release.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_configuration_error_hides_sensitive_proxy_input(tmp_path: Any) -> None:
    raw_url = "http://user:password@127.0.0.1:7890"
    config_file = tmp_path / "recorder.toml"
    config_file.write_text(
        "[recorder]\n"
        'data_root = "/var/lib/bmdr-test"\n'
        'network_proxy_mode = "explicit"\n'
        f'network_proxy_url = "{raw_url}"\n',
        encoding="utf-8",
    )
    with pytest.raises(ConfigurationError) as caught:
        load_config(
            config_file=config_file,
            environ={},
            home=tmp_path / "home",
            repository_root=tmp_path / "workspace" / "repo",
        )
    assert raw_url not in str(caught.value)
    assert "password" not in str(caught.value)
