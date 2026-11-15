"""M5-A Cloudflare Zero Trust, private-IP gRPC proxy, and host-audit/verification helper."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import ipaddress
import json
from pathlib import Path
import re
import subprocess
import sys
import urllib.error
import urllib.request
from typing import Any

DEPLOYMENT_DIR = Path(__file__).resolve().parent


@dataclass(frozen=True)
class ListenerRecord:
    """Single listening socket observed on the host."""

    proto: str
    local_addr: str
    port: int
    process: str
    exposure: str
    policy_note: str


_LOCAL_ONLY_PORTS = {
    53: ("只本机", "systemd-resolve 本地 DNS stub"),
    323: ("只本机", "chronyd 本地时钟控制"),
    18080: ("只本机 + 私网", "Nginx 只读导出 (Tunnel HTTP / WARP 私网批量)"),
    50051: ("只本机 + 私网转发", "bmdg 保持 127.0.0.1:50051，仅私网 /32 转发接入"),
}


def validate_private_ipv4(raw_ip: str) -> str:
    """Validate a dedicated non-loopback RFC1918/private IPv4 address for WARP routing."""

    addr = ipaddress.ip_address(raw_ip.strip())
    if not isinstance(addr, ipaddress.IPv4Address) or not addr.is_private or addr.is_loopback:
        raise ValueError(f"private_ip must be a non-loopback private IPv4 address: {raw_ip!r}")
    return str(addr)


def parse_ss_listeners(ss_output: str) -> list[ListenerRecord]:
    """Parse `ss -tulpn` output and classify exposure for A4 host audit."""

    records: list[ListenerRecord] = []
    for line in ss_output.splitlines():
        parts = line.split()
        if len(parts) < 5 or parts[0] not in {"tcp", "udp"}:
            continue
        proto = parts[0]
        state = parts[1]
        if proto == "tcp" and state != "LISTEN":
            continue
        local = parts[4]
        host, sep, port_str = local.rpartition(":")
        if not sep or not port_str.isdigit():
            continue
        port = int(port_str)
        clean_host = host.strip("[]")
        process = " ".join(parts[6:]) if len(parts) > 6 else ""
        is_loopback = clean_host.startswith("127.") or clean_host in {"::1", "::ffff:127.0.0.1"} or clean_host.endswith("%lo")
        if is_loopback:
            exposure = "只本机"
            policy = _LOCAL_ONLY_PORTS.get(port, ("只本机", "loopback 内部服务"))[1]
        elif clean_host in {"0.0.0.0", "::", "*"}:
            if port == 22:
                exposure = "公网当前开放 -> 待 WARP 私网验证后收口"
                policy = "SSH 待 Cloudflare WARP 私网与控制台验证可用后关闭公网入站"
            else:
                exposure = "公网通配监听 (需核查/收口)"
                policy = "非预期公网监听，A4 收口阶段禁止直接对公网暴露"
        else:
            exposure = f"指定地址 ({clean_host})"
            policy = (
                "Cloudflare WARP /32 私网专用"
                if ipaddress.ip_address(clean_host).is_private
                else "非私网绑定 (需核查)"
            )
        records.append(
            ListenerRecord(
                proto=proto,
                local_addr=local,
                port=port,
                process=process,
                exposure=exposure,
                policy_note=policy,
            )
        )
    return records


def render_private_net_unit(private_ip: str, *, iface_name: str = "sbmdr0") -> str:
    ip = validate_private_ipv4(private_ip)
    return "\n".join(
        [
            "[Unit]",
            "Description=S-BMDR dedicated private /32 IP for Cloudflare WARP subnet routing",
            "Before=nginx.service sbmdr-bmdg-private-proxy.socket network-online.target",
            "Wants=network-online.target",
            "",
            "[Service]",
            "Type=oneshot",
            "RemainAfterExit=yes",
            f"ExecStart=/bin/sh -c 'ip link show {iface_name} >/dev/null 2>&1 || ip link add {iface_name} type dummy; ip addr replace {ip}/32 dev {iface_name}; ip link set {iface_name} up'",
            f"ExecStop=/bin/sh -c 'ip link del {iface_name} >/dev/null 2>&1 || true'",
            "",
            "[Install]",
            "WantedBy=multi-user.target",
            "",
        ]
    )


def render_bmdg_proxy_socket_unit(private_ip: str, *, port: int = 50051) -> str:
    ip = validate_private_ipv4(private_ip)
    return "\n".join(
        [
            "[Unit]",
            "Description=S-BMDR private-IP gRPC proxy socket (forwards to loopback bmdg)",
            "After=sbmdr-private-net.service",
            "Requires=sbmdr-private-net.service",
            "",
            "[Socket]",
            f"ListenStream={ip}:{port}",
            "NoDelay=true",
            "ReusePort=false",
            "",
            "[Install]",
            "WantedBy=sockets.target",
            "",
        ]
    )


def render_cloudflared_config(
    *,
    tunnel_id: str,
    export_hostname: str,
    credentials_file: Path | None = None,
    template_path: Path | None = None,
) -> str:
    if not re.fullmatch(r"[A-Za-z0-9._-]+", tunnel_id):
        raise ValueError(f"invalid tunnel_id: {tunnel_id!r}")
    if not re.fullmatch(r"[A-Za-z0-9.-]+\.[A-Za-z]{2,}", export_hostname):
        raise ValueError(f"invalid export_hostname: {export_hostname!r}")
    tpl = (template_path or (DEPLOYMENT_DIR / "cloudflared-config.yml.template")).read_text(
        encoding="utf-8"
    )
    creds = credentials_file or Path(f"/etc/cloudflared/{tunnel_id}.json")
    return (
        tpl.replace("{{TUNNEL_ID}}", tunnel_id)
        .replace("{{CREDENTIALS_FILE}}", str(creds))
        .replace("{{EXPORT_HOSTNAME}}", export_hostname)
    )


def audit_host() -> dict[str, Any]:
    """Collect current host listener inventory, firewall status, and cloudflared state."""

    ss_out = subprocess.check_output(["ss", "-tulpn"], text=True)
    listeners = parse_ss_listeners(ss_out)
    ufw_proc = subprocess.run(["ufw", "status", "verbose"], capture_output=True, text=True)
    cf_bin = subprocess.run(["which", "cloudflared"], capture_output=True, text=True)
    cf_ver = ""
    if cf_bin.returncode == 0:
        cf_ver = subprocess.check_output(["cloudflared", "--version"], text=True).strip()
    return {
        "listeners": [
            {
                "proto": r.proto,
                "local_addr": r.local_addr,
                "port": r.port,
                "process": r.process,
                "exposure": r.exposure,
                "policy_note": r.policy_note,
            }
            for r in listeners
        ],
        "ufw_status": ufw_proc.stdout.strip() or ufw_proc.stderr.strip(),
        "cloudflared_installed": cf_bin.returncode == 0,
        "cloudflared_version": cf_ver,
    }


def verify_access_http_endpoint(
    *,
    base_url: str,
    cf_client_id: str,
    cf_client_secret: str,
) -> dict[str, Any]:
    """Verify A3 strict Service Auth Access gate: unauth/bad token rejected; valid token GET/Range ok."""

    clean_base = base_url.rstrip("/")
    latest_url = f"{clean_base}/export/latest.json"

    class _NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> None:
            return None

    opener = urllib.request.build_opener(_NoRedirect())

    def _request(url: str, *, method: str = "GET", headers: dict[str, str] | None = None) -> tuple[int, dict[str, str], bytes]:
        merged = {"User-Agent": "S-BMDR-M5A/1.0", **(headers or {})}
        req = urllib.request.Request(url, method=method, headers=merged)
        try:
            with opener.open(req, timeout=15) as resp:
                return resp.status, {k.lower(): v for k, v in resp.headers.items()}, resp.read()
        except urllib.error.HTTPError as exc:
            return exc.code, {k.lower(): v for k, v in exc.headers.items()}, exc.read()

    # 1. Unauthenticated must NOT return 200 (Access redirects 302 to login or returns 401/403).
    unauth_code, _, _ = _request(latest_url)
    if unauth_code == 200:
        raise RuntimeError(f"Access check failed: unauthenticated GET {latest_url} returned 200")

    # 2. Bad token must be rejected cleanly.
    bad_code, _, _ = _request(
        latest_url,
        headers={
            "CF-Access-Client-Id": "invalid-client-id",
            "CF-Access-Client-Secret": "invalid-client-secret",
        },
    )
    if bad_code == 200:
        raise RuntimeError(f"Access check failed: bad service token returned 200 for {latest_url}")

    # 3. Valid token GET latest.json + Cache-Control: no-store + manifest + Range probe on 1 ready chunk.
    auth_headers = {
        "CF-Access-Client-Id": cf_client_id,
        "CF-Access-Client-Secret": cf_client_secret,
    }
    code, headers, body = _request(latest_url, headers=auth_headers)
    if code != 200:
        raise RuntimeError(f"Access check failed: valid token GET {latest_url} returned HTTP {code}")
    if "no-store" not in headers.get("cache-control", "").lower():
        raise RuntimeError(f"latest.json missing Cache-Control: no-store (got {headers.get('cache-control')!r})")
    latest = json.loads(body.decode("utf-8"))

    m_code, _, m_body = _request(f"{clean_base}/{latest['manifest_url_path']}", headers=auth_headers)
    if m_code != 200:
        raise RuntimeError(f"failed to fetch manifest over Access: HTTP {m_code}")
    manifest = json.loads(m_body.decode("utf-8"))

    sample_chunk_url: str | None = None
    for part in manifest.get("partitions", []):
        c_code, _, c_body = _request(f"{clean_base}/{part['files']['chunks_jsonl']['url_path']}", headers=auth_headers)
        if c_code != 200:
            raise RuntimeError(f"failed to fetch chunks.jsonl: HTTP {c_code}")
        for line in c_body.decode("utf-8").splitlines():
            obj = json.loads(line)
            if obj.get("ready") and obj.get("archive_url_path"):
                sample_chunk_url = f"{clean_base}/{obj['archive_url_path']}"
                break
        if sample_chunk_url:
            break

    range_verified = False
    if sample_chunk_url:
        h_code, h_headers, _ = _request(sample_chunk_url, method="HEAD", headers=auth_headers)
        if h_code != 200 or h_headers.get("accept-ranges") != "bytes":
            raise RuntimeError(f"HEAD chunk failed over Access: HTTP {h_code}, headers={h_headers}")
        r_code, _, r_body = _request(
            sample_chunk_url, headers={**auth_headers, "Range": "bytes=0-63"}
        )
        if r_code != 206 or len(r_body) != 64:
            raise RuntimeError(f"Range chunk failed over Access: HTTP {r_code}, len={len(r_body)}")
        range_verified = True

    return {
        "latest_url": latest_url,
        "unauthenticated_http_code": unauth_code,
        "bad_token_http_code": bad_code,
        "valid_token_http_code": code,
        "revision_id": latest["revision_id"],
        "sample_chunk_url": sample_chunk_url,
        "range_verified": range_verified,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("audit", help="Inspect listening sockets, UFW, and cloudflared state")

    render_p = sub.add_parser("render-private-units", help="Write systemd units for private /32 + bmdg proxy")
    render_p.add_argument("--private-ip", default="10.250.250.10")
    render_p.add_argument("--systemd-dir", type=Path, default=Path("/etc/systemd/system"))

    cf_p = sub.add_parser("render-cloudflared", help="Render /etc/cloudflared/config.yml")
    cf_p.add_argument("--tunnel-id", required=True)
    cf_p.add_argument("--export-hostname", required=True)
    cf_p.add_argument("--out", type=Path, default=Path("/etc/cloudflared/config.yml"))

    ver_p = sub.add_parser("verify-access", help="Verify Cloudflare Access Service Token gating")
    ver_p.add_argument("--base-url", required=True)
    ver_p.add_argument("--cf-client-id", required=True)
    ver_p.add_argument("--cf-client-secret", required=True)

    args = parser.parse_args(argv)
    if args.cmd == "audit":
        print(json.dumps(audit_host(), indent=2, ensure_ascii=False))
        return 0
    if args.cmd == "render-private-units":
        net_unit = render_private_net_unit(args.private_ip)
        sock_unit = render_bmdg_proxy_socket_unit(args.private_ip)
        svc_unit = (DEPLOYMENT_DIR / "sbmdr-bmdg-private-proxy.service").read_text(encoding="utf-8")
        args.systemd_dir.mkdir(parents=True, exist_ok=True)
        (args.systemd_dir / "sbmdr-private-net.service").write_text(net_unit, encoding="utf-8")
        (args.systemd_dir / "sbmdr-bmdg-private-proxy.socket").write_text(sock_unit, encoding="utf-8")
        (args.systemd_dir / "sbmdr-bmdg-private-proxy.service").write_text(svc_unit, encoding="utf-8")
        print(json.dumps({"private_ip": args.private_ip, "systemd_dir": str(args.systemd_dir)}, indent=2))
        return 0
    if args.cmd == "render-cloudflared":
        rendered = render_cloudflared_config(
            tunnel_id=args.tunnel_id, export_hostname=args.export_hostname
        )
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(rendered, encoding="utf-8")
        print(json.dumps({"config": str(args.out), "export_hostname": args.export_hostname}, indent=2))
        return 0
    if args.cmd == "verify-access":
        result = verify_access_http_endpoint(
            base_url=args.base_url,
            cf_client_id=args.cf_client_id,
            cf_client_secret=args.cf_client_secret,
        )
        print(json.dumps(result, indent=2))
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
