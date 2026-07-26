from typing import cast

from tools.evaluate_spot_bootstrap import compare


def test_disputed_targets_report_different_bridge_without_claiming_resolution() -> None:
    report = compare(
        100,
        [
            {"U": 100, "u": 100},
            {"U": 101, "u": 110},
        ],
    )
    documented = cast(dict[str, object], report["target_last_update_id"])
    adjacent = cast(dict[str, object], report["target_last_update_id_plus_one"])
    assert documented["bridge_event_index"] == 0
    assert adjacent["bridge_event_index"] == 1
    assert report["different_bridge_event"] is True
    assert report["decision"] == "EVIDENCE_ONLY_R034_REMAINS_OPEN"


def test_observed_adjacent_window_exposes_documented_target_ambiguity() -> None:
    report = compare(97_799_318_619, [{"U": 97_799_318_620, "u": 97_799_318_630}])
    documented = cast(dict[str, object], report["target_last_update_id"])
    adjacent = cast(dict[str, object], report["target_last_update_id_plus_one"])
    assert documented["result"] == "GAP"
    assert adjacent["result"] == "BRIDGED"
