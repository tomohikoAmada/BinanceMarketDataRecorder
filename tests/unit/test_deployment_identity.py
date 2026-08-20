from __future__ import annotations

import json
import stat
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from binance_market_data_recorder.service.deployment_identity import (
    DeploymentIdentity,
    DeploymentIdentityError,
    create_deployment_identity,
    load_deployment_identity,
    rollback_compatibility,
    verify_identity_files,
    verify_installed_dependencies,
    verify_retained_rollback_artifacts,
    verify_vps_control_chain,
    verify_vps_identity_permissions,
    write_deployment_identity,
)


def _effective(tmp_path: Path) -> dict[str, object]:
    return {
        "fragment_path": str(tmp_path / "recorder.service"),
        "drop_in_paths": [],
        "exec_start": [
            str(Path(sys.executable).absolute()),
            "-m",
            "binance_market_data_recorder",
            "--config",
            str(tmp_path / "recorder.toml"),
            "_service",
            "run",
        ],
        "user": "recorder",
        "group": "recorder",
        "restart": "on-failure",
        "restart_sec_usec": 10_000_000,
        "timeout_stop_sec_usec": 90_000_000,
        "umask": "0027",
        "no_new_privileges": True,
        "working_directory": str(tmp_path / "data"),
        "wants": ["network-online.target"],
        "requires": [],
        "after": ["network-online.target"],
        "environment": [
            "ALL_PROXY=",
            "HTTPS_PROXY=",
            "HTTP_PROXY=",
            "NO_PROXY=",
            "PYTHONUNBUFFERED=1",
            "all_proxy=",
            "http_proxy=",
            "https_proxy=",
            "no_proxy=",
        ],
        "environment_files": [],
        "pass_environment": [],
        "service_type": "simple",
        "kill_signal": "SIGTERM",
        "standard_output": "journal",
        "standard_error": "journal",
    }


def _identity(tmp_path: Path, *, sidecar: bool = False) -> DeploymentIdentity:
    tmp_path.mkdir(parents=True, exist_ok=True)
    wheel = tmp_path / "recorder.whl"
    lock = tmp_path / "production.lock"
    config = tmp_path / "recorder.toml"
    unit = tmp_path / "recorder.service"
    reconnect = tmp_path / "legacy_reconnect_classifications.json"
    wheel.write_bytes(b"exact wheel")
    lock.write_bytes(b"exact lock")
    config.write_bytes(b'[recorder]\ncapacity_profile="vps-production-v1"\n')
    unit.write_bytes(b"[Service]\nRestart=on-failure\n")
    if sidecar:
        reconnect.write_bytes(b'{"schema":"legacy"}')
    return create_deployment_identity(
        source_git_sha="a" * 40,
        wheel_path=wheel,
        dependency_lock_path=lock,
        config_path=config,
        systemd_unit_path=unit,
        capacity_profile_id="vps-production-v1",
        startup_sidecar_path=reconnect,
        systemd_effective=_effective(tmp_path),
    )


def test_identity_is_canonical_deterministic_and_round_trips(tmp_path: Path) -> None:
    first = _identity(tmp_path)
    second = DeploymentIdentity.from_document(first.document())
    path = tmp_path / "deployment-identity.json"

    write_deployment_identity(path, first)

    assert first.canonical_bytes() == second.canonical_bytes() == path.read_bytes()
    assert first.identity_sha256 == second.identity_sha256
    assert load_deployment_identity(path) == first


def test_vps_identity_permissions_require_root_service_group_and_0640(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "deployment-identity.json"
    identity = _identity(tmp_path)
    write_deployment_identity(path, identity)
    wheel_path = Path(identity.wheel_path)
    lock_path = Path(identity.dependency_lock_path)
    facts = {
        path: SimpleNamespace(
            st_uid=0, st_gid=123, st_mode=stat.S_IFREG | 0o640
        ),
        tmp_path: SimpleNamespace(
            st_uid=0, st_gid=0, st_mode=stat.S_IFDIR | 0o750
        ),
        wheel_path: SimpleNamespace(
            st_uid=0, st_gid=0, st_mode=stat.S_IFREG | 0o640
        ),
        lock_path: SimpleNamespace(
            st_uid=0, st_gid=0, st_mode=stat.S_IFREG | 0o640
        ),
    }
    original_stat = Path.stat

    def fake_stat(selected: Path, *, follow_symlinks: bool = True) -> object:
        return facts.get(
            selected,
            original_stat(selected, follow_symlinks=follow_symlinks),
        )

    monkeypatch.setattr(
        "binance_market_data_recorder.service.deployment_identity.grp.getgrnam",
        lambda _group: SimpleNamespace(gr_gid=123),
    )
    monkeypatch.setattr(Path, "stat", fake_stat)

    assert verify_vps_identity_permissions(
        path, expected_group="recorder"
    )["mode"] == "0640"
    assert verify_retained_rollback_artifacts(identity)["root_controlled"] is True

    facts[wheel_path] = SimpleNamespace(
        st_uid=0, st_gid=123, st_mode=stat.S_IFREG | 0o660
    )
    with pytest.raises(DeploymentIdentityError, match="not service-writable"):
        verify_retained_rollback_artifacts(identity)

    facts[path] = SimpleNamespace(
        st_uid=0, st_gid=123, st_mode=stat.S_IFREG | 0o660
    )
    with pytest.raises(DeploymentIdentityError, match="mode 0640"):
        verify_vps_identity_permissions(path, expected_group="recorder")


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("wheel_path", b"wrong Wheel"),
        ("dependency_lock_path", b"wrong lock"),
        ("config_path", b"wrong config"),
        ("systemd_unit_path", b"wrong unit"),
    ],
)
def test_identity_fails_closed_for_bound_file_mismatch(
    tmp_path: Path,
    field: str,
    replacement: bytes,
) -> None:
    identity = _identity(tmp_path)
    Path(cast(str, getattr(identity, field))).write_bytes(replacement)

    with pytest.raises(DeploymentIdentityError, match="SHA-256 mismatch"):
        verify_identity_files(identity, verify_installed=False)


def test_identity_fails_closed_for_profile_mismatch(tmp_path: Path) -> None:
    document = _identity(tmp_path).document()
    document["capacity_profile_id"] = "not-approved"

    with pytest.raises(DeploymentIdentityError, match="profile"):
        DeploymentIdentity.from_document(document)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("restart", "always"),
        ("drop_in_paths", ["/etc/systemd/system/recorder.d/override.conf"]),
        ("environment", ["BINANCE_MARKET_RECORDER_DATA_ROOT=/wrong"]),
    ],
)
def test_identity_rejects_noncanonical_effective_systemd_authority(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    document = _identity(tmp_path).document()
    effective = cast(dict[str, object], document["systemd_effective"])
    effective[field] = value

    with pytest.raises(DeploymentIdentityError, match="systemd"):
        DeploymentIdentity.from_document(document)


def test_identity_binds_present_and_absent_startup_sidecars(tmp_path: Path) -> None:
    absent = _identity(tmp_path / "absent")
    present_root = tmp_path / "present"
    present = _identity(present_root, sidecar=True)

    assert absent.startup_sidecars["legacy_reconnect_classifications"]["state"] == "ABSENT"
    assert present.startup_sidecars["legacy_reconnect_classifications"]["state"] == "PRESENT"
    Path(
        cast(
            str,
            absent.startup_sidecars["legacy_reconnect_classifications"]["path"],
        )
    ).write_bytes(b"appeared after identity freeze")
    with pytest.raises(DeploymentIdentityError, match="sidecar mismatch"):
        verify_identity_files(absent, verify_installed=False)


def test_noncanonical_identity_bytes_are_rejected(tmp_path: Path) -> None:
    identity = _identity(tmp_path)
    path = tmp_path / "deployment-identity.json"
    path.write_text(json.dumps(identity.document(), indent=2), encoding="utf-8")

    with pytest.raises(DeploymentIdentityError, match="not canonical"):
        load_deployment_identity(path)


def test_rollback_allows_exact_target_that_supports_present_states(tmp_path: Path) -> None:
    identity = _identity(tmp_path)

    class FakeCatalog:
        def remote_archive_transactions(self) -> list[dict[str, object]]:
            return [{"state": "REMOTE_DELETE_PENDING"}, {"state": "REMOTE_DELETED"}]

    result = rollback_compatibility(identity, cast(Any, FakeCatalog()))

    assert result["compatible"] is True
    assert result["data_rollback"] is False


def test_rollback_refuses_target_that_cannot_interpret_remote_states(
    tmp_path: Path,
) -> None:
    document = _identity(tmp_path).document()
    compatibility = cast(dict[str, object], document["catalog_compatibility"])
    compatibility["remote_archive_states"] = []
    incompatible = DeploymentIdentity.from_document(document)

    class FakeCatalog:
        def remote_archive_transactions(self) -> list[dict[str, object]]:
            return [{"state": "REMOTE_DELETE_PENDING"}]

    with pytest.raises(DeploymentIdentityError, match="cannot interpret"):
        rollback_compatibility(incompatible, cast(Any, FakeCatalog()))


def test_pre_m22_document_is_not_accepted_as_a_rollback_identity(tmp_path: Path) -> None:
    path = tmp_path / "old.json"
    path.write_bytes(b'{"wheel_sha256":"' + b"a" * 64 + b'"}')

    with pytest.raises(DeploymentIdentityError):
        load_deployment_identity(path)


def _write_lock(path: Path, distributions: dict[str, str]) -> None:
    body = "".join(
        f"{name}=={version} \\\n"
        f"    --hash=sha256:{index:064x}\n"
        for index, (name, version) in enumerate(distributions.items(), start=1)
    )
    path.write_text(body, encoding="utf-8")


def test_installed_dependency_identity_accepts_only_exact_normalized_set(
    tmp_path: Path,
) -> None:
    lock = tmp_path / "production.lock"
    _write_lock(lock, {"Example_Dependency": "1.2.3", "second.package": "4.5"})

    evidence = verify_installed_dependencies(
        lock,
        installed_distributions={
            "example-dependency": "1.2.3",
            "Second-Package": "4.5",
            "pip": "26.0",
            "binance-market-data-recorder": "0.1.0a1",
        },
    )

    assert evidence["exact_match"] is True
    assert evidence["recorder_distribution_separate"] is True
    assert evidence["bootstrap_allowlist"] == ["pip"]


def test_installed_dependency_identity_rejects_wrong_locked_version(
    tmp_path: Path,
) -> None:
    lock = tmp_path / "production.lock"
    _write_lock(lock, {"example": "1.2.3"})

    with pytest.raises(DeploymentIdentityError, match="versions differ"):
        verify_installed_dependencies(
            lock, installed_distributions={"example": "1.2.4"}
        )


def test_installed_dependency_identity_rejects_missing_locked_package(
    tmp_path: Path,
) -> None:
    lock = tmp_path / "production.lock"
    _write_lock(lock, {"example": "1.2.3"})

    with pytest.raises(DeploymentIdentityError, match="missing"):
        verify_installed_dependencies(lock, installed_distributions={})


def test_installed_dependency_identity_rejects_unexpected_package(
    tmp_path: Path,
) -> None:
    lock = tmp_path / "production.lock"
    _write_lock(lock, {"example": "1.2.3"})

    with pytest.raises(DeploymentIdentityError, match="unexpected"):
        verify_installed_dependencies(
            lock,
            installed_distributions={"example": "1.2.3", "injected": "9.9"},
        )


def test_matching_lock_bytes_do_not_hide_installed_environment_drift(
    tmp_path: Path,
) -> None:
    lock = tmp_path / "production.lock"
    _write_lock(lock, {"example": "1.2.3"})
    identity = _identity(tmp_path / "identity")
    locked_sha = Path(identity.dependency_lock_path).read_bytes()

    with pytest.raises(DeploymentIdentityError, match="versions differ"):
        verify_installed_dependencies(
            lock, installed_distributions={"example": "0.9"}
        )
    assert Path(identity.dependency_lock_path).read_bytes() == locked_sha


def _control_chain_fixture(
    tmp_path: Path,
) -> tuple[DeploymentIdentity, dict[str, object], Path, Path]:
    artifact_root = tmp_path / "artifact"
    release = artifact_root / "release-1"
    venv = artifact_root / "venv"
    site_packages = venv / "lib/python3.12/site-packages"
    dist_info = site_packages / "binance_market_data_recorder-0.1.0a1.dist-info"
    package = site_packages / "binance_market_data_recorder"
    for directory in (release, venv / "bin", dist_info, package):
        directory.mkdir(parents=True, exist_ok=True)
    python = venv / "bin/python"
    wheel = release / "recorder.whl"
    lock = release / "production.lock"
    module = package / "__init__.py"
    direct_url = dist_info / "direct_url.json"
    for path in (python, wheel, lock, module, direct_url):
        path.write_bytes(b"authority")
    identity = replace(
        _identity(tmp_path / "identity"),
        python_executable=str(python),
        wheel_path=str(wheel),
        dependency_lock_path=str(lock),
    )
    installed: dict[str, object] = {
        "module_path": str(module),
        "dist_info_path": str(dist_info),
        "direct_url_path": str(direct_url),
    }
    return identity, installed, artifact_root, release


@pytest.mark.parametrize(
    ("bad_component", "mode", "message"),
    [
        ("venv", stat.S_IFDIR | 0o775, "service-writable"),
        ("release", stat.S_IFDIR | 0o775, "service-writable"),
        ("venv", stat.S_IFLNK | 0o777, "actual directory"),
    ],
)
def test_vps_control_chain_rejects_writable_or_symlinked_namespace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    bad_component: str,
    mode: int,
    message: str,
) -> None:
    identity, installed, artifact_root, release = _control_chain_fixture(tmp_path)
    original_lstat = Path.lstat
    bad_path = artifact_root / "venv" if bad_component == "venv" else release

    def fake_lstat(path: Path) -> object:
        observed = original_lstat(path)
        return SimpleNamespace(
            st_uid=0,
            st_gid=0,
            st_mode=mode if path == bad_path else observed.st_mode,
        )

    monkeypatch.setattr(
        "binance_market_data_recorder.service.deployment_identity.VPS_ARTIFACT_ROOT",
        artifact_root,
    )
    monkeypatch.setattr(
        "binance_market_data_recorder.service.deployment_identity.pwd.getpwnam",
        lambda _user: SimpleNamespace(pw_uid=123),
    )
    monkeypatch.setattr(
        "binance_market_data_recorder.service.deployment_identity.grp.getgrnam",
        lambda _group: SimpleNamespace(gr_gid=456),
    )
    monkeypatch.setattr(Path, "lstat", fake_lstat)

    with pytest.raises(DeploymentIdentityError, match=message):
        verify_vps_control_chain(
            identity,
            installed,
            service_user="recorder",
            service_group="recorder",
        )


def test_vps_control_chain_accepts_protected_symlink_free_namespace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity, installed, artifact_root, _release = _control_chain_fixture(tmp_path)
    original_lstat = Path.lstat

    def fake_lstat(path: Path) -> object:
        observed = original_lstat(path)
        return SimpleNamespace(st_uid=0, st_gid=0, st_mode=observed.st_mode)

    monkeypatch.setattr(
        "binance_market_data_recorder.service.deployment_identity.VPS_ARTIFACT_ROOT",
        artifact_root,
    )
    monkeypatch.setattr(
        "binance_market_data_recorder.service.deployment_identity.pwd.getpwnam",
        lambda _user: SimpleNamespace(pw_uid=123),
    )
    monkeypatch.setattr(
        "binance_market_data_recorder.service.deployment_identity.grp.getgrnam",
        lambda _group: SimpleNamespace(gr_gid=456),
    )
    monkeypatch.setattr(Path, "lstat", fake_lstat)

    evidence = verify_vps_control_chain(
        identity,
        installed,
        service_user="recorder",
        service_group="recorder",
    )

    assert evidence["symlink_free"] is True
    assert evidence["service_writable"] is False
