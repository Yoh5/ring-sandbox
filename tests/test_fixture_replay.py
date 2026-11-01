"""Record→replay: `ring-sandbox record` output loaded back into the emulator
must reproduce the real Partner API surface verbatim — devices, included
resources, history entries, and the shapes clients depend on."""

import json
from pathlib import Path

import httpx
import pytest
from test_parity import _emulator

from ring_sandbox.world import default_world, load_fixture_docs

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"
AUTH = {"Authorization": "Bearer sandbox-token"}

DEVICES_DOC = json.loads((FIXTURES / "devices.json").read_text(encoding="utf-8"))
DEVICE_ID = DEVICES_DOC["data"][0]["id"]
INCLUDED = {r["id"]: r for r in DEVICES_DOC["included"]}
RELS = DEVICES_DOC["data"][0]["relationships"]


@pytest.fixture
def replay_world():
    world = default_world()
    counts = load_fixture_docs(world, FIXTURES)
    assert counts["devices"] == 1
    assert counts["history"] == 3
    return world


def _get(base: str, path: str) -> dict:
    resp = httpx.get(f"{base}{path}", headers=AUTH, timeout=5)
    assert resp.status_code == 200, resp.text
    return resp.json()


def test_load_from_dir_and_doc_map_agree(tmp_path):
    world_dir = default_world()
    load_fixture_docs(world_dir, FIXTURES)
    docs = {p.name: json.loads(p.read_text(encoding="utf-8")) for p in FIXTURES.glob("*.json")}
    world_map = default_world()
    load_fixture_docs(world_map, docs)
    assert DEVICE_ID in world_dir.devices and DEVICE_ID in world_map.devices
    assert len(world_map.get(DEVICE_ID).history) == 3


def test_recorded_device_serves_real_id_and_relationships(replay_world):
    _, base = _emulator(replay_world)
    dev = _get(base, f"/v1/devices/{DEVICE_ID}")["data"]
    assert dev["id"] == DEVICE_ID
    assert dev["attributes"] == DEVICES_DOC["data"][0]["attributes"]
    for name in ("status", "capabilities", "configurations", "location"):
        assert dev["relationships"][name]["data"]["id"] == RELS[name]["data"]["id"]


def test_status_capabilities_configurations_verbatim(replay_world):
    _, base = _emulator(replay_world)
    for name, path in (
        ("capabilities", "capabilities"),
        ("configurations", "configurations"),
        ("location", "location"),
    ):
        served = _get(base, f"/v1/devices/{DEVICE_ID}/{path}")["data"]
        recorded = INCLUDED[RELS[name]["data"]["id"]]
        assert served == recorded
    # status is verbatim except the mutable overlays the emulator owns
    served = _get(base, f"/v1/devices/{DEVICE_ID}/status")["data"]
    recorded = INCLUDED[RELS["status"]["data"]["id"]]
    assert served["attributes"]["online"] is True
    for key, value in recorded["attributes"].items():
        if key not in ("online", "reported_at"):
            assert served["attributes"][key] == value


def test_recorded_config_shapes_preserved(replay_world):
    """The real API's "on"/"off" string enums and nested blocks — the parity
    surface canned resources must match."""
    _, base = _emulator(replay_world)
    attrs = _get(base, f"/v1/devices/{DEVICE_ID}/configurations")["data"]["attributes"]
    assert attrs["image_enhancements"]["ir_led_night_vision"] in ("on", "off")
    assert isinstance(attrs["audio"]["volume"], int)
    caps = _get(base, f"/v1/devices/{DEVICE_ID}/capabilities")["data"]["attributes"]
    assert caps  # recorded capability blocks present verbatim


def test_recorded_history_events_replayed(replay_world):
    _, base = _emulator(replay_world)
    events = _get(base, f"/v1/history/devices/{DEVICE_ID}/events")["data"]
    recorded_doc = json.loads(
        (FIXTURES / f"history-on-demand.{DEVICE_ID}.json").read_text(encoding="utf-8")
    )
    assert len(events) == len(recorded_doc["data"]) == 3
    by_id = {e["id"]: e for e in events}
    for recorded in recorded_doc["data"]:
        served = by_id[recorded["id"]]
        assert served["attributes"] == recorded["attributes"]
        assert served["meta"] == recorded.get("meta", {"riid": None})
        assert served["relationships"]["source"]["data"]["id"] == DEVICE_ID


@pytest.mark.skipif(
    not (FIXTURES / "me.json").exists(), reason="me.json is gitignored — local record output only"
)
def test_me_fixture_maps_account(replay_world):
    _, base = _emulator(replay_world)
    me = _get(base, "/v1/users/me")["data"]
    recorded = json.loads((FIXTURES / "me.json").read_text(encoding="utf-8"))["data"]
    assert me["id"] == recorded["id"]


def test_sandbox_load_endpoint_roundtrip():
    _, base = _emulator(default_world())
    docs = {p.name: json.loads(p.read_text(encoding="utf-8")) for p in FIXTURES.glob("*.json")}
    resp = httpx.post(f"{base}/_sandbox/load", json=docs, timeout=5)
    assert resp.status_code == 200
    assert resp.json()["loaded"] == {"devices": 1, "history": 3}
    dev = _get(base, f"/v1/devices/{DEVICE_ID}")["data"]
    assert dev["id"] == DEVICE_ID
