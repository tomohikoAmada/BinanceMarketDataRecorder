"""Tests for M5-A Cloudflare templates, private-IP gRPC proxy units, and Access verifier."""

from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import json
from pathlib import Path
import sys
import threading

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "setup_m5a_cloudflare",
    Path(__file__).resolve().parents[1] / "setup_m5a_cloudflare.py",
)
assert _SPEC is not None and _SPEC.loader is not None
setup_cf = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = setup_cf
_SPEC.loader.exec_module(setup_cf)


def test_parse_ss_listeners_classifies_loopback_private_and_public_ssh() -> None:
    sample = "\n".join(
        [
            "Netid State Recv-Q Send-Q Local Address:Port Peer Address:Port Process",
            'tcp LISTEN 0 4096 0.0.0.0:22 0.0.0.0:* users:(("sshd",pid=1,fd=3))',
            'tcp LISTEN 0 511 127.0.0.1:18080 0.0.0.0:* users:(("nginx",pid=2,fd=4))',
            'tcp LISTEN 0 511 10.250.250.10:18080 0.0.0.0:* users:(("nginx",pid=2,fd=5))',
            'tcp LISTEN 0 4096 [::ffff:127.0.0.1]:50051 *:* users:(("bmdg",pid=3,fd=6))',
            'tcp LISTEN 0 4096 10.250.250.10:50051 0.0.0.0:* users:(("systemd",pid=1,fd=7))',
        ]
    )
    records = setup_cf.parse_ss_listeners(sample)
    by_addr = {r.local_addr: r for r in records}
    assert "待 WARP 私网验证后收口" in by_addr["0.0.0.0:22"].exposure
    assert by_addr["127.0.0.1:18080"].exposure == "只本机"
    assert by_addr["[::ffff:127.0.0.1]:50051"].exposure == "只本机"
    assert "10.250.250.10" in by_addr["10.250.250.10:18080"].exposure
    assert "WARP" in by_addr["10.250.250.10:50051"].policy_note


def test_render_private_units_and_cloudflared_config(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        setup_cf.validate_private_ipv4("127.0.0.1")
    with pytest.raises(ValueError):
        setup_cf.validate_private_ipv4("8.8.8.8")

    net_unit = setup_cf.render_private_net_unit("10.250.250.10")
    sock_unit = setup_cf.render_bmdg_proxy_socket_unit("10.250.250.10")
    assert "10.250.250.10/32" in net_unit
    assert "ListenStream=10.250.250.10:50051" in sock_unit

    cfg = setup_cf.render_cloudflared_config(
        tunnel_id="11111111-2222-3333-4444-555555555555",
        export_hostname="recorder-data.example.com",
    )
    assert "warp-routing:\n  enabled: true" in cfg
    assert "hostname: recorder-data.example.com" in cfg
    assert "service: http://127.0.0.1:18080" in cfg


def test_verify_access_http_endpoint_enforces_token_and_range() -> None:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            return

        def _authorized(self) -> bool:
            return (
                self.headers.get("CF-Access-Client-Id") == "id-ok"
                and self.headers.get("CF-Access-Client-Secret") == "sec-ok"
            )

        def do_HEAD(self) -> None:
            if not self._authorized():
                self.send_response(403)
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", "100")
            self.end_headers()

        def do_GET(self) -> None:
            if not self._authorized():
                self.send_response(403)
                self.end_headers()
                self.wfile.write(b"forbidden")
                return
            if self.path == "/export/latest.json":
                payload = json.dumps(
                    {
                        "revision_id": "rev-1",
                        "manifest_url_path": "export/revisions/rev-1/manifest.json",
                    }
                ).encode()
                self.send_response(200)
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(payload)
                return
            if self.path == "/export/revisions/rev-1/manifest.json":
                payload = json.dumps(
                    {
                        "partitions": [
                            {
                                "files": {
                                    "chunks_jsonl": {
                                        "url_path": "export/snapshots/p/chunks.jsonl"
                                    }
                                }
                            }
                        ]
                    }
                ).encode()
                self.send_response(200)
                self.end_headers()
                self.wfile.write(payload)
                return
            if self.path == "/export/snapshots/p/chunks.jsonl":
                payload = (
                    json.dumps(
                        {
                            "chunk_id": "c1",
                            "ready": True,
                            "archive_url_path": "archive/sid1/raw/c1.bmdr.zst",
                        }
                    )
                    + "\n"
                ).encode()
                self.send_response(200)
                self.end_headers()
                self.wfile.write(payload)
                return
            if self.path == "/archive/sid1/raw/c1.bmdr.zst":
                self.send_response(206)
                self.send_header("Content-Range", "bytes 0-63/100")
                self.end_headers()
                self.wfile.write(b"x" * 64)
                return
            self.send_response(404)
            self.end_headers()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        res = setup_cf.verify_access_http_endpoint(
            base_url=f"http://127.0.0.1:{server.server_port}",
            cf_client_id="id-ok",
            cf_client_secret="sec-ok",
        )
        assert res["unauthenticated_http_code"] == 403
        assert res["bad_token_http_code"] == 403
        assert res["valid_token_http_code"] == 200
        assert res["range_verified"] is True
    finally:
        server.shutdown()
        server.server_close()
