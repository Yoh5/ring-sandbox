"""Live-observed API fidelity: pagination traps, consent boundary, offline
media refusals, WHEP spec shape, OAuth grant surface, and rate-limit headers —
each behavior is something the partner docs or a recorded fixture showed the
real api.amazonvision.com doing."""

from __future__ import annotations

import httpx

from ring_sandbox.emulator import create_app
from ring_sandbox.models import WebhookEventType
from ring_sandbox.world import World, now_ms

AUTH = {"Authorization": "Bearer sandbox-token"}
_OFFER = "v=0\r\no=- 0 0 IN IP4 1.2.3.4\r\ns=-\r\nt=0 0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\n"


def _raw(world: World) -> httpx.Client:
    from ring_sandbox.pytest_plugin import _SyncASGITransport

    return httpx.Client(base_url="http://sandbox", transport=_SyncASGITransport(create_app(world)))


def _history_events(c: httpx.Client, device_id: str) -> dict:
    return c.get(f"/v1/history/devices/{device_id}/events", headers=AUTH).json()


# ------------------------------------------------------------------- history


def test_history_pagination_verified_semantics(ring_world: World):
    """Verified against recorded captures: links.next exists only while a
    further page exists (a final page omits links entirely), a completely
    empty history is the bare {"data": []}, and next drops event_types."""
    dev = ring_world.cameras()[0]
    c = _raw(ring_world)

    doc = _history_events(c, dev.id)
    assert doc["data"] == [] and "links" not in doc  # bare empty doc, no links key

    base = now_ms()
    for i in range(55):
        ring_world.record_event(dev.id, WebhookEventType.MOTION_DETECTED, at_ms=base - i * 60_000)
    doc = c.get(f"/v1/history/devices/{dev.id}/events?event_types=motion", headers=AUTH).json()
    assert len(doc["data"]) == 50
    nxt = doc["links"]["next"]
    assert "page%5Bkey%5D" in nxt or "page[key]" in nxt
    assert "event_types" not in nxt  # the widening trap: filters don't survive paging
    doc2 = c.get(nxt, headers=AUTH).json()
    assert len(doc2["data"]) == 5
    assert "links" not in doc2  # last page — no next link


def test_history_withholds_sub_type(ring_world: World):
    """The real API withholds sub_type from history resources — it is a
    filter-only field (event_types=motion.human still filters)."""
    dev = ring_world.cameras()[0]
    ring_world.record_event(dev.id, WebhookEventType.MOTION_DETECTED, sub_type="human")
    ring_world.record_event(dev.id, WebhookEventType.MOTION_DETECTED, sub_type="package")
    c = _raw(ring_world)
    doc = _history_events(c, dev.id)
    assert all("sub_type" not in e["attributes"] for e in doc["data"])
    doc = c.get(
        f"/v1/history/devices/{dev.id}/events?event_types=motion.human", headers=AUTH
    ).json()
    assert len(doc["data"]) == 1


def test_history_meta_riid_on_motion(ring_world: World):
    dev = ring_world.cameras()[0]
    ring_world.record_event(dev.id, WebhookEventType.MOTION_DETECTED)
    ring_world.record_on_demand(dev.id)
    doc = _history_events(_raw(ring_world), dev.id)
    by_type = {e["attributes"]["event_type"]: e for e in doc["data"]}
    assert by_type["motion"]["meta"]["riid"]
    assert by_type["on_demand"]["meta"]["riid"] is None


# -------------------------------------------------------------- consent gate


def test_consent_boundary_hides_pre_link_history_and_media(ring_world: World):
    """Completing the app-integration is the consent instant: history before it
    is filtered out and media requests for it fail 403."""
    dev = ring_world.cameras()[0]
    c = _raw(ring_world)
    ring_world.record_event(dev.id, WebhookEventType.MOTION_DETECTED, at_ms=now_ms() - 60_000)
    before = now_ms() - 30_000
    c.post("/v1/accounts/me/app-integrations", headers=AUTH)
    c.patch("/v1/accounts/me/app-integrations", headers=AUTH, json={"status": "completed"})
    ring_world.record_event(dev.id, WebhookEventType.MOTION_DETECTED, at_ms=now_ms())

    doc = _history_events(c, dev.id)
    assert len(doc["data"]) == 1  # the pre-consent event is invisible

    r = c.post(
        f"/v1/devices/{dev.id}/media/image/download",
        headers=AUTH,
        json={"type": "at_timestamp", "timestamp": before},
    )
    assert r.status_code == 403
    assert r.json()["errors"][0]["code"] == "TIME_RANGE_NOT_AUTHORIZED"

    r = c.post(
        f"/v1/devices/{dev.id}/media/image/download",
        headers=AUTH,
        json={"type": "at_timestamp", "timestamp": now_ms()},
    )
    assert r.status_code == 303


def test_no_consent_gate_without_integration(ring_world: World):
    """A world that never ran the linking flow exposes its full history —
    consent gating only applies once consent exists."""
    dev = ring_world.cameras()[0]
    ring_world.record_event(dev.id, WebhookEventType.MOTION_DETECTED)
    assert len(_history_events(_raw(ring_world), dev.id)["data"]) == 1


# --------------------------------------------------------------------- media


def test_offline_device_refuses_media_and_whep(ring_world: World):
    dev = ring_world.cameras()[0]
    ring_world.record_event(dev.id, WebhookEventType.DEVICE_OFFLINE)
    c = _raw(ring_world)
    r = c.post(
        f"/v1/devices/{dev.id}/media/image/download",
        headers=AUTH,
        json={"type": "at_timestamp", "timestamp": now_ms()},
    )
    assert r.status_code == 503 and r.json()["errors"][0]["code"] == "device_offline"
    r = c.post(
        f"/v1/devices/{dev.id}/media/streaming/whep/sessions",
        headers={**AUTH, "Content-Type": "application/sdp"},
        content=_OFFER,
    )
    assert r.status_code == 503


def test_latest_in_range_416s_when_nothing_recorded(ring_world: World):
    """The documented trap: no covering recording means a refusal, not a frame
    from the nearest other time."""
    dev = ring_world.cameras()[0]
    c = _raw(ring_world)
    now = now_ms()
    r = c.post(
        f"/v1/devices/{dev.id}/media/image/download",
        headers=AUTH,
        json={
            "type": "latest_in_range",
            "start_timestamp": now - 3600_000,
            "end_timestamp": now - 3500_000,
        },
    )
    assert r.status_code == 416
    assert r.json()["errors"][0]["code"] == "TIMESTAMP_NOT_FOUND"
    ring_world.record_event(dev.id, WebhookEventType.MOTION_DETECTED, at_ms=now - 3550_000)
    r = c.post(
        f"/v1/devices/{dev.id}/media/image/download",
        headers=AUTH,
        json={
            "type": "latest_in_range",
            "start_timestamp": now - 3600_000,
            "end_timestamp": now - 3500_000,
        },
    )
    assert r.status_code == 303


def test_snapshot_logs_one_on_demand_entry(ring_world: World):
    """Regression: the image route logged the on_demand history row twice."""
    dev = ring_world.cameras()[0]
    before = len(dev.history)
    c = _raw(ring_world)
    c.post(
        f"/v1/devices/{dev.id}/media/image/download",
        headers=AUTH,
        json={"type": "at_timestamp", "timestamp": now_ms()},
    )
    added = dev.history[before:]
    assert len(added) == 1 and added[0].event_type == "on_demand"


def test_video_rejects_nonpositive_duration(ring_world: World):
    dev = ring_world.cameras()[0]
    c = _raw(ring_world)
    r = c.post(
        f"/v1/devices/{dev.id}/media/video/download",
        headers=AUTH,
        json={"timestamp": now_ms(), "duration": 0},
    )
    assert r.status_code == 400


# ---------------------------------------------------------------------- WHEP


def test_whep_answer_is_parseable_and_spec_shaped(ring_world: World):
    dev = ring_world.cameras()[0]
    c = _raw(ring_world)
    r = c.post(
        f"/v1/devices/{dev.id}/media/streaming/whep/sessions",
        headers={**AUTH, "Content-Type": "application/sdp"},
        content=_OFFER,
    )
    assert r.status_code == 201
    answer = r.text
    for line in ("a=mid:", "a=ice-ufrag:", "a=ice-pwd:", "a=fingerprint:", "a=setup:"):
        assert line in answer, line
    assert r.headers["Location"].startswith("http://")  # absolute, per real API
    assert r.headers["ETag"] and r.headers["Link"].endswith('rel="ice-server"')


def test_whep_expired_session_close_404s(ring_world: World):
    dev = ring_world.cameras()[0]
    c = _raw(ring_world)
    r = c.post(
        f"/v1/devices/{dev.id}/media/streaming/whep/sessions",
        headers={**AUTH, "Content-Type": "application/sdp"},
        content=_OFFER,
    )
    sid = r.headers["Location"].rsplit("/", 1)[-1]
    ring_world.whep_sessions[sid]["expires_ms"] = now_ms() - 1  # force expiry
    assert (
        c.delete(
            f"/v1/devices/{dev.id}/media/streaming/whep/sessions/{sid}", headers=AUTH
        ).status_code
        == 404
    )


# --------------------------------------------------------------------- oauth


def test_authorization_code_grant_roundtrip(ring_world: World):
    c = _raw(ring_world)
    minted = c.post("/_sandbox/authz-codes", data={"client_id": "sandbox-client"}).json()
    r = c.post(
        "/oauth/token",
        data={"grant_type": "authorization_code", "code": minted["code"]},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["scope"] == "ava.v1" and body["expires_in"] == 14400
    assert body["access_token"] and body["refresh_token"]
    # single-use: a second exchange is invalid_grant, in RFC 6749 shape
    r = c.post(
        "/oauth/token",
        data={"grant_type": "authorization_code", "code": minted["code"]},
    )
    assert r.status_code == 400
    assert r.json()["error"] == "invalid_grant" and "error_description" in r.json()


def test_token_errors_use_rfc6749_shape(ring_world: World):
    c = _raw(ring_world)
    r = c.post("/oauth/token", data={"grant_type": "urn:custom"})
    assert r.status_code == 400 and r.json()["error"] == "unsupported_grant_type"
    assert "errors" not in r.json()  # not the data-plane JSON:API shape
    r = c.post("/oauth/token", data={"grant_type": "refresh_token"})
    assert r.json()["error"] == "invalid_request"
    r = c.post("/oauth/token", data={"grant_type": "refresh_token", "refresh_token": "invalid"})
    assert r.json()["error"] == "invalid_grant"
    # valid refresh grant still works
    r = c.post("/oauth/token", data={"grant_type": "refresh_token", "refresh_token": "rt-ok"})
    assert r.status_code == 200 and r.json()["scope"] == "ava.v1"


# ------------------------------------------------------------------- headers


def test_rate_limit_headers_on_data_plane(ring_world: World):
    c = _raw(ring_world)
    r = c.get("/v1/devices", headers=AUTH)
    assert r.headers["X-RateLimit-Limit"] == "100"
    assert int(r.headers["X-RateLimit-Remaining"]) < 100
    # control plane carries no quota headers
    r = c.get("/_sandbox/state")
    assert "X-RateLimit-Limit" not in r.headers


def test_steady_state_limit_trips_429(ring_world: World):
    c = _raw(ring_world)
    # sit at the quota edge deterministically — the counter resets each second
    ring_world._rl_window = int(__import__("time").time())
    ring_world._rl_used = 100
    r = c.get("/v1/users/me", headers=AUTH)
    assert r.status_code == 429 and r.headers["Retry-After"] == "1"
    assert r.headers["X-RateLimit-Remaining"] == "0"


# ------------------------------------------------------------ error envelope


def test_validation_errors_speak_jsonapi(ring_world: World):
    """Malformed bodies must not leak FastAPI's native {"detail": [...]} shape."""
    c = _raw(ring_world)
    r = c.post(
        f"/v1/devices/{ring_world.cameras()[0].id}/media/image/download",
        headers=AUTH,
        json={"type": 123},  # wrong type for a str field
    )
    assert r.status_code == 422
    assert "errors" in r.json() and "detail" not in r.json()
