"""Partner API parity surface: app-integration lifecycle, subscriptions, WHEP
sessions, device add/remove webhooks, and rate-limit/unavailable chaos."""

import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import httpx
import pytest
import uvicorn

from ring_sandbox import RingAPIError, RingClient, webhooks
from ring_sandbox.emulator import create_app
from ring_sandbox.world import Chaos, default_world

KEY = "parity-hmac-key"
AUTH = {"Authorization": "Bearer sandbox-token"}
OFFER = "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=t\r\nt=0 0\r\nm=video 9 RTP/SAVPF 96\r\n"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _Receiver(BaseHTTPRequestHandler):
    received: list[tuple[str | None, bytes]] = []

    def do_POST(self):  # noqa: N802
        n = int(self.headers.get("Content-Length", 0))
        _Receiver.received.append((self.headers.get("X-Signature"), self.rfile.read(n)))
        self.send_response(200)
        self.end_headers()

    def log_message(self, *_):
        pass


def _serve(app, port: int) -> None:
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    threading.Thread(target=server.run, daemon=True).start()
    for _ in range(50):
        try:
            httpx.get(f"http://127.0.0.1:{port}/_sandbox/health", timeout=0.5)
            return
        except httpx.HTTPError:
            time.sleep(0.1)
    raise RuntimeError("server did not start")


def _emulator(world=None, chaos=None):
    app = create_app(world or default_world(), chaos)
    port = _free_port()
    _serve(app, port)
    return app, f"http://127.0.0.1:{port}"


def _hook(base: str) -> HTTPServer:
    _Receiver.received = []
    port = _free_port()
    hooks = HTTPServer(("127.0.0.1", port), _Receiver)
    threading.Thread(target=hooks.serve_forever, daemon=True).start()
    httpx.post(
        base + "/_sandbox/webhooks",
        json={"url": f"http://127.0.0.1:{port}/hook", "signing_key": KEY},
    )
    return hooks


def _wait_hooks(n: int, timeout: float = 4.0) -> None:
    for _ in range(int(timeout * 10)):
        if len(_Receiver.received) >= n:
            return
        time.sleep(0.1)


def _hook_events():
    return [
        webhooks.parse(body, signing_key=KEY, signature=sig) for sig, body in _Receiver.received
    ]


# ----------------------------------------------------------------- app integrations


def test_app_integration_lifecycle():
    app, base = _emulator()
    world = app.state.world
    hooks = _hook(base)

    assert httpx.get(base + "/v1/accounts/me/app-integrations", headers=AUTH).json()["data"] is None

    # Confirm the link -> awaiting + app_integration_added + device_added per device
    data = httpx.post(base + "/v1/accounts/me/app-integrations", headers=AUTH, json={}).json()[
        "data"
    ]
    assert data["type"] == "app-integrations" and data["attributes"]["status"] == "awaiting"
    _wait_hooks(len(world.devices) + 1)
    events = _hook_events()
    added = events[0]
    assert added.event_type == "app_integration_added"
    assert added.data.attributes.source_type == "accounts"
    assert added.data.attributes.source == world.account_id
    assert not added.data.relationships  # no devices relationship on account events
    assert [e.event_type for e in events[1:]] == ["device_added"] * len(world.devices)
    assert {e.device_id for e in events[1:]} == set(world.devices)

    # Re-confirming is idempotent — no duplicate lifecycle webhooks
    httpx.post(base + "/v1/accounts/me/app-integrations", headers=AUTH, json={})
    time.sleep(0.3)
    assert len(_Receiver.received) == len(world.devices) + 1

    # PATCH only moves forward to completed
    bad = httpx.patch(
        base + "/v1/accounts/me/app-integrations", headers=AUTH, json={"status": "bogus"}
    )
    assert bad.status_code == 400
    done = httpx.patch(
        base + "/v1/accounts/me/app-integrations", headers=AUTH, json={"status": "completed"}
    ).json()["data"]
    assert done["attributes"]["status"] == "completed"
    assert app.state.world.app_integration_status == "completed"
    hooks.shutdown()


def test_app_integration_unlink_fires_removed_events():
    app, base = _emulator()
    world = app.state.world
    cam = world.cameras()[0]
    hooks = _hook(base)
    httpx.post(base + "/v1/accounts/me/app-integrations", headers=AUTH, json={})
    _wait_hooks(len(world.devices) + 1)
    sub = httpx.post(base + "/_sandbox/subscriptions", json={"device_id": cam.id}).json()["data"]
    _wait_hooks(len(world.devices) + 2)
    _Receiver.received = []

    assert httpx.delete(base + "/_sandbox/app-integration").status_code == 200
    _wait_hooks(len(world.devices) + 2)
    events = _hook_events()
    assert events[0].event_type == "app_integration_removed"
    assert events[0].data.attributes.source_type == "accounts"
    assert [e.event_type for e in events[1 : len(world.devices) + 1]] == ["device_removed"] * len(
        world.devices
    )
    last = events[-1]
    assert last.event_type == "subscription_deactivated"
    assert last.data.attributes.plan_id == sub["attributes"]["plan_id"]
    assert world.app_integration_status is None and world.subscriptions == {}
    hooks.shutdown()


def test_unlink_without_link_is_400():
    _, base = _emulator()
    assert httpx.delete(base + "/_sandbox/app-integration").status_code == 400


def test_patch_without_link_is_400():
    _, base = _emulator()
    resp = httpx.patch(
        base + "/v1/accounts/me/app-integrations", headers=AUTH, json={"status": "completed"}
    )
    assert resp.status_code == 400


def test_client_app_integration_roundtrip():
    app, base = _emulator()
    with RingClient("sandbox-token", base_url=base, transport=httpx.HTTPTransport()) as client:
        assert client.app_integration() is None
        link = client.link_app_integration(nonce="test-nonce")
        assert link.attributes.status == "awaiting"
        done = client.update_app_integration("completed")
        assert done.attributes.status == "completed"
        assert client.app_integration().attributes.status == "completed"
        with pytest.raises(RingAPIError) as ei:
            client.update_app_integration("awaiting")
        assert ei.value.status_code == 400


# ----------------------------------------------------------------- subscriptions


def test_subscription_lifecycle():
    app, base = _emulator()
    world = app.state.world
    cam = world.cameras()[0]
    hooks = _hook(base)

    with RingClient("sandbox-token", base_url=base, transport=httpx.HTTPTransport()) as client:
        assert client.subscriptions() == []

    created = httpx.post(
        base + "/_sandbox/subscriptions", json={"device_id": cam.id, "plan_id": "protect.pro"}
    ).json()["data"]
    assert created["type"] == "subscriptions"
    assert created["attributes"]["state"] == "active"
    assert created["attributes"]["plan_id"] == "protect.pro"
    assert created["relationships"]["devices"]["data"]["id"] == cam.id

    _wait_hooks(1)
    (event,) = _hook_events()
    assert event.event_type == "subscription_activated"
    assert event.device_id == cam.id
    assert event.data.attributes.plan_id == "protect.pro"
    assert event.data.attributes.expires_at is not None

    with RingClient("sandbox-token", base_url=base, transport=httpx.HTTPTransport()) as client:
        subs = client.subscriptions()
        assert len(subs) == 1 and subs[0].id == created["id"] and subs[0].device_id == cam.id

    _Receiver.received = []
    httpx.delete(base + f"/_sandbox/subscriptions/{created['id']}")
    _wait_hooks(1)
    (event,) = _hook_events()
    assert event.event_type == "subscription_deactivated"
    assert event.data.attributes.plan_id == "protect.pro"
    assert world.subscriptions == {}
    hooks.shutdown()


def test_subscription_validation():
    app, base = _emulator()
    cam = app.state.world.cameras()[0]
    assert (
        httpx.post(
            base + "/_sandbox/subscriptions", json={"device_id": cam.id, "state": "bogus"}
        ).status_code
        == 400
    )
    assert (
        httpx.post(base + "/_sandbox/subscriptions", json={"device_id": "nope"}).status_code == 404
    )
    assert httpx.delete(base + "/_sandbox/subscriptions/sub_missing").status_code == 404


# ----------------------------------------------------------------- WHEP sessions


def test_whep_session_lifecycle():
    app, base = _emulator()
    world = app.state.world
    cam = world.cameras()[0]
    before = len(cam.history)

    with RingClient("sandbox-token", base_url=base, transport=httpx.HTTPTransport()) as client:
        session = client.whep_session(cam.id, OFFER)
        # the real API returns an absolute session URL (client strips the origin)
        assert session.session_url.endswith(
            f"/v1/devices/{cam.id}/media/streaming/whep/sessions/{session.session_id}"
        )
        assert session.session_url.startswith("http")
        assert session.session_id and "a=recvonly" in session.sdp_answer
        assert session.session_id in world.whep_sessions
        # A live view surfaces in Event History as on_demand
        assert len(cam.history) == before + 1
        assert cam.history[0].event_type == "on_demand"
        client.whep_close(session)
        assert session.session_id not in world.whep_sessions
        with pytest.raises(RingAPIError) as ei:
            client.whep_close(session)
        assert ei.value.status_code == 404


def test_whep_validation():
    app, base = _emulator()
    world = app.state.world
    cam = world.cameras()[0]
    chime = next(d for d in world.devices.values() if d.kind == "chime")
    url = f"{base}/v1/devices/{cam.id}/media/streaming/whep/sessions"

    bad_type = httpx.post(
        url, headers={**AUTH, "Content-Type": "application/json"}, json={"sdp": "v=0"}
    )
    assert bad_type.status_code == 415

    bad_body = httpx.post(
        url, headers={**AUTH, "Content-Type": "application/sdp"}, content=b"garbage"
    )
    assert bad_body.status_code == 400

    no_cam = httpx.post(
        f"{base}/v1/devices/{chime.id}/media/streaming/whep/sessions",
        headers={**AUTH, "Content-Type": "application/sdp"},
        content=OFFER.encode(),
    )
    assert no_cam.status_code == 400

    with pytest.raises(ValueError):
        with RingClient("t", base_url=base, transport=httpx.HTTPTransport()) as client:
            client.whep_session(cam.id, "not an offer")


def test_whep_component_selection():
    app, base = _emulator()
    dev = httpx.post(
        base + "/_sandbox/devices",
        json={"kind": "camera", "name": "Dual Cam", "components": ["front", "rear"]},
    ).json()["data"]
    url = f"{base}/v1/devices/{dev['id']}/media/streaming/whep/sessions"
    ok = httpx.post(
        url + "?component_id=rear",
        headers={**AUTH, "Content-Type": "application/sdp"},
        content=OFFER.encode(),
    )
    assert ok.status_code == 201
    sess = app.state.world.whep_sessions
    assert next(iter(sess.values()))["component_id"] == "rear"
    bad = httpx.post(
        url + "?component_id=bogus",
        headers={**AUTH, "Content-Type": "application/sdp"},
        content=OFFER.encode(),
    )
    assert bad.status_code == 400


# ----------------------------------------------------------------- device add/remove


def test_device_add_remove_fire_webhooks():
    app, base = _emulator()
    hooks = _hook(base)
    dev = httpx.post(
        base + "/_sandbox/devices", json={"kind": "camera", "name": "Side Gate"}
    ).json()["data"]
    _wait_hooks(1)
    (event,) = _hook_events()
    assert event.event_type == "device_added" and event.device_id == dev["id"]

    _Receiver.received = []
    httpx.delete(base + f"/_sandbox/devices/{dev['id']}")
    _wait_hooks(1)
    (event,) = _hook_events()
    assert event.event_type == "device_removed" and event.device_id == dev["id"]
    assert httpx.get(f"{base}/v1/devices/{dev['id']}", headers=AUTH).status_code == 404
    assert httpx.delete(base + f"/_sandbox/devices/{dev['id']}").status_code == 404
    hooks.shutdown()


# ----------------------------------------------------------------- rate limiting


def test_rate_limit_429_retry_after():
    _, base = _emulator(chaos=Chaos(rate_limit=1.0, seed=1))
    resp = httpx.get(base + "/v1/devices", headers=AUTH)
    assert resp.status_code == 429
    assert resp.headers["Retry-After"] == "1"
    assert resp.json()["errors"][0]["code"] == "rate_limited"
    # control plane is not throttled
    assert httpx.get(base + "/_sandbox/health").status_code == 200


def test_unavailable_503():
    _, base = _emulator(chaos=Chaos(unavailable=1.0, seed=1))
    resp = httpx.get(base + "/v1/devices", headers=AUTH)
    assert resp.status_code == 503 and resp.headers["Retry-After"] == "2"


def test_client_retries_through_rate_limit():
    # seed=1, rate=0.5: first roll 0.134<0.5 -> 429, retry roll 0.847 -> pass
    _, base = _emulator(chaos=Chaos(rate_limit=0.5, seed=1))
    with RingClient(
        "sandbox-token",
        base_url=base,
        transport=httpx.HTTPTransport(),
        max_retry_delay=5,
    ) as client:
        bundles = client.devices()
        assert len(bundles) == 4


def test_client_surfaces_exhausted_retries():
    _, base = _emulator(chaos=Chaos(rate_limit=1.0, seed=1))
    with RingClient(
        "sandbox-token",
        base_url=base,
        transport=httpx.HTTPTransport(),
        max_retries=1,
        max_retry_delay=5,
    ) as client:
        with pytest.raises(RingAPIError) as ei:
            client.devices()
        assert ei.value.status_code == 429


# ----------------------------------------------------------------- state


def test_state_reports_account_plane_and_reset_clears():
    app, base = _emulator()
    world = app.state.world
    cam = world.cameras()[0]
    httpx.post(base + "/v1/accounts/me/app-integrations", headers=AUTH, json={})
    httpx.post(base + "/_sandbox/subscriptions", json={"device_id": cam.id})
    httpx.post(
        f"{base}/v1/devices/{cam.id}/media/streaming/whep/sessions",
        headers={**AUTH, "Content-Type": "application/sdp"},
        content=OFFER.encode(),
    )
    state = httpx.get(base + "/_sandbox/state").json()
    assert state["app_integration_status"] == "awaiting"
    assert len(state["subscriptions"]) == 1
    assert len(state["whep_sessions"]) == 1

    httpx.post(base + "/_sandbox/reset")
    state = httpx.get(base + "/_sandbox/state").json()
    assert state["app_integration_status"] is None
    assert state["subscriptions"] == [] and state["whep_sessions"] == []
    assert len(state["devices"]) == len(world.devices)
