"""One redaction-safe proxy policy for every production network exit."""

from __future__ import annotations

import ipaddress
import os
import urllib.request
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any, Literal, cast
from urllib.parse import urlsplit

ProxyMode = Literal["direct", "environment", "explicit"]
WebSocketProxy = str | Literal[True] | None
_SUPPORTED_SCHEMES = frozenset({"http", "https"})
_DEFAULT_PORTS = {"http": 80, "https": 443}
_proxy_bypass_environment = cast(
    Any,
    urllib.request.__dict__["proxy_bypass_environment"],
)


class ProxyConfigurationError(ValueError):
    """A proxy setting is unsafe or cannot be represented consistently."""


@dataclass(frozen=True, slots=True)
class ProxyStatus:
    """Only proxy facts that are safe for status, logs, and diagnostics."""

    proxy_mode: ProxyMode
    proxy_scheme: str | None
    proxy_loopback: bool | None
    proxy_port: int | None

    def public_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class _ParsedProxy:
    url: str
    scheme: str
    host: str
    port: int
    loopback: bool


def _loopback(host: str) -> bool:
    normalized = host.rstrip(".").casefold()
    if normalized == "localhost":
        return True
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


def _parse_proxy_url(value: str) -> _ParsedProxy:
    """Validate without ever embedding the supplied value in an exception."""

    if not value or value != value.strip():
        raise ProxyConfigurationError("proxy URL is missing or has surrounding whitespace")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise ProxyConfigurationError("proxy URL has an invalid port") from exc
    scheme = parsed.scheme.casefold()
    if scheme not in _SUPPORTED_SCHEMES:
        raise ProxyConfigurationError("proxy scheme must be http or https")
    if (
        parsed.username is not None
        or parsed.password is not None
        or "@" in parsed.netloc
    ):
        raise ProxyConfigurationError("proxy URL must not contain user information")
    if not parsed.hostname:
        raise ProxyConfigurationError("proxy URL must contain a host")
    if parsed.query or parsed.fragment or parsed.path not in {"", "/"}:
        raise ProxyConfigurationError("proxy URL must not contain path, query, or fragment")
    selected_port = _DEFAULT_PORTS[scheme] if port is None else port
    if not 1 <= selected_port <= 65535:
        raise ProxyConfigurationError("proxy URL has an invalid port")
    host = parsed.hostname
    display_host = f"[{host}]" if ":" in host else host
    normalized_url = f"{scheme}://{display_host}:{selected_port}"
    return _ParsedProxy(
        url=normalized_url,
        scheme=scheme,
        host=host,
        port=selected_port,
        loopback=_loopback(host),
    )


def _environment_proxies(environment: Mapping[str, str]) -> dict[str, str]:
    """Mirror urllib's lower-case-preferred environment convention."""

    proxies: dict[str, str] = {}
    for name, value in environment.items():
        lowered = name.casefold()
        if lowered.endswith("_proxy") and value:
            proxies[lowered.removesuffix("_proxy")] = value
    for name, value in environment.items():
        if name.endswith("_proxy") and value:
            proxies[name.removesuffix("_proxy")] = value
    return proxies


class ProxyPolicy:
    """Resolve WebSocket, urllib, and Binance SDK transports consistently."""

    def __init__(
        self,
        mode: ProxyMode,
        url: str | None = None,
        *,
        environment: Mapping[str, str] | None = None,
    ) -> None:
        if mode not in {"direct", "environment", "explicit"}:
            raise ProxyConfigurationError(
                "proxy mode must be direct, environment, or explicit"
            )
        if mode == "explicit" and url is None:
            raise ProxyConfigurationError("explicit proxy mode requires a proxy URL")
        if mode != "explicit" and url is not None:
            raise ProxyConfigurationError(
                "proxy URL is only valid when proxy mode is explicit"
            )
        self.mode = mode
        self._environment = os.environ if environment is None else environment
        self._explicit = _parse_proxy_url(url) if url is not None else None

    def _environment_proxy(
        self,
        target_url: str,
        *,
        websocket: bool,
    ) -> _ParsedProxy | None:
        target = urlsplit(target_url)
        if target.scheme not in {"http", "https", "ws", "wss"} or not target.hostname:
            raise ProxyConfigurationError("network target URL is invalid")
        proxies = _environment_proxies(self._environment)
        if _proxy_bypass_environment(target.hostname, proxies):
            return None
        names = ("wss", "https", "http") if websocket else ("https", "http", "wss")
        for name in names:
            candidate = proxies.get(name)
            if candidate:
                return _parse_proxy_url(candidate)
        return None

    def websocket_proxy(self, target_url: str) -> WebSocketProxy:
        if self.mode == "direct":
            return None
        if self.mode == "environment":
            # websockets 15 owns getproxies()/no_proxy behavior for this mode.
            return True
        if self._explicit is None:
            raise ProxyConfigurationError("explicit proxy mode is incomplete")
        return self._explicit.url

    def urllib_proxy_map(self, target_url: str) -> dict[str, str]:
        if self.mode == "direct":
            return {}
        selected = (
            self._environment_proxy(target_url, websocket=False)
            if self.mode == "environment"
            else self._explicit
        )
        if selected is None:
            return {}
        return {"http": selected.url, "https": selected.url}

    def urllib_opener(self, target_url: str) -> urllib.request.OpenerDirector:
        return urllib.request.build_opener(
            urllib.request.ProxyHandler(self.urllib_proxy_map(target_url))
        )

    def sdk_proxy(
        self,
        target_url: str,
    ) -> dict[str, str | int | dict[str, str]] | None:
        if self.mode == "direct":
            return None
        selected = (
            self._environment_proxy(target_url, websocket=False)
            if self.mode == "environment"
            else self._explicit
        )
        if selected is None:
            return None
        return {
            "host": selected.host,
            "port": selected.port,
            "protocol": selected.scheme,
        }

    def configure_sdk_rest_api(self, rest_api: object) -> None:
        """Make the SDK's requests Session obey the same environment decision."""

        session = vars(rest_api).get("_session")
        if session is None or not hasattr(session, "trust_env"):
            raise ProxyConfigurationError(
                "official SDK REST client has no controllable proxy session"
            )
        session.trust_env = self.mode == "environment"

    def status(self) -> ProxyStatus:
        selected = self._explicit
        if self.mode == "environment":
            for target, websocket in (
                ("wss://stream.binance.com", True),
                ("https://api.binance.com", False),
            ):
                selected = self._environment_proxy(target, websocket=websocket)
                if selected is not None:
                    break
        return ProxyStatus(
            proxy_mode=self.mode,
            proxy_scheme=selected.scheme if selected is not None else None,
            proxy_loopback=selected.loopback if selected is not None else None,
            proxy_port=selected.port if selected is not None else None,
        )
