"""Token-scope and subscription gating — modeled on observed live API behavior.

Recorded against api.amazonvision.com (2026-09): ``ava.v1:read``-scoped tokens
authenticate but get 403 on app-integration mutations and 422 on the
subscriptions surface. Separately, ``World.enforce_subscriptions`` opts into
plan gating: no active subscription/trial means no retained event history,
no media, no live view, and observation webhooks suppressed (lifecycle events
still post — they're how a partner learns the entitlement changed).
"""

from __future__ import annotations

import httpx

from ring_sandbox.emulator import create_app
from ring_sandbox.world import World

READ_TOKEN = "read-only-token"


def _transport(world: World) -> httpx.BaseTransport:
    from ring_sandbox.pytest_plugin import _SyncASGITransport

    return _SyncASGITransport(create_app(world))


def _raw(world: World) -> httpx.Client:
    return httpx.Client(base_url="http://sandbox", transport=_transport(world))


def authed(client: httpx.Client, token: str) -> httpx.Client:
    client.headers["Authorization"] = f"Bearer {token}"
    return client


# --------------------------------------------------------------------- scopes


def test_read_token_gets_pass(ring_world: World):
    ring_world.token_scopes[READ_TOKEN] = {"ava.v1:read"}
    c = authed(_raw(ring_world), READ_TOKEN)
    assert c.get("/v1/devices").status_code == 200
    assert c.get("/v1/users/me").status_code == 200


def test_read_token_mutations_403(ring_world: World):
    ring_world.token_scopes[READ_TOKEN] = {"ava.v1:read"}
    c = authed(_raw(ring_world), READ_TOKEN)
    r = c.post("/v1/accounts/me/app-integrations")
    assert r.status_code == 403
    assert r.json()["errors"][0]["code"] == "insufficient_scope"


def test_read_token_subscriptions_422(ring_world: World):
    ring_world.token_scopes[READ_TOKEN] = {"ava.v1:read"}
    c = authed(_raw(ring_world), READ_TOKEN)
    assert c.get("/v1/accounts/me/subscriptions").status_code == 422


def test_read_token_media_and_whep_403(ring_world: World):
    ring_world.token_scopes[READ_TOKEN] = {"ava.v1:read"}
    c = authed(_raw(ring_world), READ_TOKEN)
    dev = ring_world.cameras()[0].id
    r = c.post(f"/v1/devices/{dev}/media/image/download", json={"type": "latest"})
    assert r.status_code == 403
    r = c.post(
        f"/v1/devices/{dev}/media/streaming/whep/sessions",
        content=b"v=0\r\n",
        headers={"Content-Type": "application/sdp"},
    )
    assert r.status_code == 403


def test_scoped_token_works_alongside_required_token(ring_world: World):
    ring_world.required_token = "full-token"
    ring_world.token_scopes[READ_TOKEN] = {"ava.v1:read"}
    assert authed(_raw(ring_world), READ_TOKEN).get("/v1/devices").status_code == 200
    assert authed(_raw(ring_world), "full-token").post(
        "/v1/accounts/me/app-integrations"
    ).status_code in (200, 201)
    # unregistered tokens still rejected when a token is required
    assert authed(_raw(ring_world), "bogus").get("/v1/devices").status_code == 401


def test_unregistered_token_unconstrained(ring_world: World):
    ring_world.token_scopes[READ_TOKEN] = {"ava.v1:read"}
    c = authed(_raw(ring_world), "any-full-token")
    assert c.post("/v1/accounts/me/app-integrations").status_code in (200, 201)


# -------------------------------------------------------------- subscriptions


def test_subscription_gates_media_and_history(ring_world: World):
    ring_world.enforce_subscriptions = True
    c = authed(_raw(ring_world), "tok")
    dev = ring_world.cameras()[0]
    ring_world.record_event(dev.id, "motion_detected")

    r = c.get(f"/v1/history/devices/{dev.id}/events")
    assert r.status_code == 200 and r.json()["data"] == []
    r = c.post(f"/v1/devices/{dev.id}/media/image/download", json={"type": "latest"})
    assert r.status_code == 403 and r.json()["errors"][0]["code"] == "subscription_required"

    # activate a plan via the control plane, surfaces open up
    c.post("/_sandbox/subscriptions", json={"device_id": dev.id, "state": "active"})
    r = c.get(f"/v1/history/devices/{dev.id}/events")
    assert r.json()["data"], "subscribed device should see retained history"
    r = c.post(
        f"/v1/devices/{dev.id}/media/image/download",
        json={"type": "at_timestamp", "timestamp": 1},
    )
    assert r.status_code != 403


def test_subscription_gate_off_by_default(ring_world: World):
    c = authed(_raw(ring_world), "tok")
    dev = ring_world.cameras()[0]
    r = c.post(f"/v1/devices/{dev.id}/media/image/download", json={"type": "latest"})
    assert r.status_code != 403


def test_unsubscribed_webhooks_suppressed_lifecycle_passes(ring_world: World):
    ring_world.enforce_subscriptions = True
    c = authed(_raw(ring_world), "tok")
    c.post(
        "/_sandbox/webhooks",
        json={"url": "http://127.0.0.1:1/hook", "signing_key": "k"},
    )
    dev = ring_world.cameras()[0]

    c.post("/_sandbox/events", json={"device_id": dev.id, "type": "motion_detected"})
    kinds = [d["kind"] for d in ring_world.delivered]
    assert "webhook.suppressed" in kinds
    assert "webhook" not in kinds

    # subscribing fires subscription_activated — lifecycle, must never be
    # suppressed (the create_task'd delivery itself needs a live loop, covered
    # by the socket-level scenario tests)
    ring_world.delivered.clear()
    c.post("/_sandbox/subscriptions", json={"device_id": dev.id})
    kinds = [d["kind"] for d in ring_world.delivered]
    assert "webhook.suppressed" not in kinds


def test_control_plane_token_registration(ring_world: World):
    c = authed(_raw(ring_world), "tok")
    c.post("/_sandbox/tokens", json={"token": "scoped-x", "scopes": ["ava.v1:read"]})
    assert "scoped-x" in ring_world.token_scopes
    assert authed(_raw(ring_world), "scoped-x").get("/v1/devices").status_code == 200
    c.delete("/_sandbox/tokens/scoped-x")
    assert "scoped-x" not in ring_world.token_scopes
    # after deregistration the token is unconstrained again
    assert authed(_raw(ring_world), "scoped-x").post(
        "/v1/accounts/me/app-integrations"
    ).status_code in (200, 201)


def test_injected_subscription_events_mutate_entitlement(ring_world: World):
    """An injected subscription_* event isn't theater — it flips the world's
    entitlement so gated surfaces answer accordingly."""
    ring_world.enforce_subscriptions = True
    c = authed(_raw(ring_world), "tok")
    dev = ring_world.cameras()[0]
    assert not ring_world.device_subscribed(dev.id)

    c.post("/_sandbox/events", json={"device_id": dev.id, "type": "subscription_activated"})
    assert ring_world.device_subscribed(dev.id)
    r = c.post(
        f"/v1/devices/{dev.id}/media/image/download",
        json={"type": "at_timestamp", "timestamp": 1},
    )
    assert r.status_code != 403

    c.post("/_sandbox/events", json={"device_id": dev.id, "type": "subscription_deactivated"})
    assert not ring_world.device_subscribed(dev.id)
    r = c.post(
        f"/v1/devices/{dev.id}/media/image/download",
        json={"type": "at_timestamp", "timestamp": 1},
    )
    assert r.status_code == 403


def test_expired_subscription_does_not_entitle(ring_world: World):
    ring_world.enforce_subscriptions = True
    dev = ring_world.cameras()[0]
    sub = {
        "id": "sub_x",
        "device_id": dev.id,
        "plan_id": "p",
        "state": "active",
        "expires_at": "2020-01-01T00:00:00Z",  # already lapsed
        "created_at": "2019-01-01T00:00:00Z",
    }
    ring_world.subscriptions["sub_x"] = sub
    assert not ring_world.device_subscribed(dev.id)
