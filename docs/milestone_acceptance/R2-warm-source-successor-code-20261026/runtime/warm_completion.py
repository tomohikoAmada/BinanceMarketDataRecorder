"""Read-only warm completion child. Never dispatches service control or restoration."""
import base64
import json
import sys
from pathlib import Path
from warm_readiness import (KIND, Q, D, CONFIG, DEPLOYMENT, SOURCE, IDENTITY,
    CONFIG_SHA, protected_read, load_attestation, snapshot, same_guard,
    file_identity, clock_guard, require, sha, clocks)


def evaluate(phase, budget_sha256, attestation_sha256, witness_name, witness_sha256):
    from binance_market_data_recorder.config import load_config
    from binance_market_data_recorder.domain.product import configured_products
    from binance_market_data_recorder.service.deployment_identity import (
        load_deployment_identity, enforce_vps_paths, verify_identity_files,
        verify_vps_identity_permissions, verify_vps_control_chain)
    from binance_market_data_recorder.storage.capacity import VPS_PRODUCTION_V1
    from binance_market_data_recorder.service.readiness import VpsReadinessEvaluator
    from binance_market_data_recorder.cli import _identity_systemd_manager

    require(phase in {'end', 'start'}, 'warm_completion_phase_invalid')
    require(witness_name.startswith(f'warm-completion-{phase}-')
            and witness_name.endswith('-state.json'), 'warm_witness_name_invalid')
    budget, binding, first, att, ready, before = load_attestation(
        budget_sha256, attestation_sha256)
    witness = protected_read(witness_name, witness_sha256)
    raw = base64.b64decode(witness['state_publication_base64'], validate=True)
    require(len(raw) == witness['state_publication_bytes']
            and sha_bytes(raw) == witness['state_sha256'], 'warm_strict_raw_invalid')
    strict = json.loads(raw)
    require(witness['strict_auxiliary_gate']['result'] == 'PASS'
            and witness['recorder_service_instance_id'] == first['recorder_service_instance_id']
            and witness['boot_id'] == budget['boot_id'], 'warm_strict_witness_invalid')
    loaded = load_config(config_file=CONFIG, environ={})
    identity = load_deployment_identity(DEPLOYMENT)
    enforce_vps_paths(identity)
    require(identity.identity_sha256 == IDENTITY and identity.source_git_sha == SOURCE
            and identity.config_sha256 == CONFIG_SHA and loaded.config.data_root == D,
            'warm_loaded_deployment_mismatch')
    manager = _identity_systemd_manager(loaded,
        identity_user=identity.systemd_effective['user'],
        identity_group=identity.systemd_effective['group'])
    reused = {'kind': 'CONDITIONAL_EARLIER_STATIC_SUBPROOFS_NOT_FRESH_FULL_SCAN',
              'attestation_sha256': attestation_sha256,
              'earlier_full_command_before': att['command']['before'],
              'earlier_full_command_after': att['command']['after'],
              'installed_contents_rechecked': False,
              'catalog_integrity_rechecked': False,
              'catalog_default_success_inferred_from_frozen_full_READY_control_flow': True,
              'identity_adapter_used': False, 'catalog_adapter_used': False}

    def identity_adapter(value):
        clock_guard(budget)
        same_guard(before, snapshot(budget, binding, first['recorder_service_instance_id']))
        fresh = dict(verify_identity_files(value, expected_config_path=CONFIG,
            expected_profile_id=VPS_PRODUCTION_V1.profile_id,
            verify_installed=False, require_root_controlled=True))
        fresh['identity_permissions'] = verify_vps_identity_permissions(
            DEPLOYMENT, expected_group=value.systemd_effective['group'])
        fresh['venv_control_chain'] = verify_vps_control_chain(value,
            ready['evidence']['identity']['installed_artifact'],
            service_user=value.systemd_effective['user'],
            service_group=value.systemd_effective['group'])
        # Source leaves installed_artifact/dependencies None: do not relabel old fields fresh.
        fresh['earlier_installed_subproof_reference'] = attestation_sha256
        reused['identity_adapter_used'] = True
        return fresh

    def catalog_adapter(path):
        clock_guard(budget)
        require(path == D / 'state/catalog.sqlite', 'warm_catalog_path_changed')
        same_guard(before, snapshot(budget, binding, first['recorder_service_instance_id']))
        require(file_identity(path) == att['after']['catalog_identity'],
                'warm_catalog_file_identity_changed')
        reused['catalog_adapter_used'] = True
        reused['current_catalog_identity'] = file_identity(path)
        return True  # Conditional on the complete bound guard above, not an unconditional stub.

    result = VpsReadinessEvaluator(
        expected_products=frozenset(configured_products(
            loaded.config.spot_symbols, loaded.config.usdm_symbols)),
        data_root=loaded.config.data_root, identity=identity, systemd_manager=manager,
        identity_verifier=identity_adapter, catalog_ready=catalog_adapter).evaluate().public_dict()
    after = snapshot(budget, binding, first['recorder_service_instance_id'])
    same_guard(before, after)
    clock_guard(budget)
    return {'kind': KIND, 'phase': phase, 'budget_sha256': budget_sha256,
            'attestation_sha256': attestation_sha256, 'witness_sha256': witness_sha256,
            'source_result': result, 'pair_match': result['evidence'].get('service_state') == strict,
            'static_reuse': reused, 'before': before, 'after': after,
            'completed_at': clocks(), 'stock_fresh_full_readiness': False}


def sha_bytes(raw):
    import hashlib
    return hashlib.sha256(raw).hexdigest()


def main():
    try:
        require(len(sys.argv) == 6, 'warm_completion_argument_count')
        print(json.dumps(evaluate(*sys.argv[1:]), sort_keys=True), flush=True)
        return 0
    except BaseException as exc:
        print(json.dumps({'kind': KIND, 'result': 'FAILED', 'error_type': type(exc).__name__,
                          'error': str(exc), 'stock_fresh_full_readiness': False}), flush=True)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
