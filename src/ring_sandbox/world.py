"""In-memory model of a Ring account: devices, history, webhook targets.

The emulator serves this state; scenarios and the control plane mutate it.
"""

from __future__ import annotations

import struct
import uuid
import zlib
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any

from .models import MotionSubType, WebhookEventType

ACCOUNT_ID = "ava1.ring.account.SANDBOX"


class DeviceKind(StrEnum):
    DOORBELL = "doorbell"
    CAMERA = "camera"
    MULTI_CAMERA = "multi_camera"
    CHIME = "chime"
    CONTACT_SENSOR = "contact_sensor"
    FLOOD_FREEZE_SENSOR = "flood_freeze_sensor"
    TEMP_HUMIDITY_SENSOR = "temp_humidity_sensor"
    AIR_QUALITY_MONITOR = "air_quality_monitor"


CAMERA_KINDS = {DeviceKind.DOORBELL, DeviceKind.CAMERA, DeviceKind.MULTI_CAMERA}
SENSOR_KINDS = {
    DeviceKind.CONTACT_SENSOR,
    DeviceKind.FLOOD_FREEZE_SENSOR,
    DeviceKind.TEMP_HUMIDITY_SENSOR,
    DeviceKind.AIR_QUALITY_MONITOR,
}

# webhook type -> history event_type (only camera events are recorded in history)
_HISTORY_TYPE = {
    WebhookEventType.MOTION_DETECTED: "motion",
    WebhookEventType.BUTTON_PRESS: "ding",
}


def now_ms() -> int:
    return int(datetime.now(tz=UTC).timestamp() * 1000)


def new_device_id() -> str:
    return "ava1.ring.device." + uuid.uuid4().hex[:20].upper()


@dataclass
class HistoryRecord:
    id: str
    event_type: str  # motion | ding | on_demand
    sub_type: str | None
    start: int
    end: int
    reviewed: bool = False
    riid: str | None = None
    cv_detections: list[dict[str, Any]] = field(default_factory=list)

    def to_jsonapi(self, device_id: str) -> dict[str, Any]:
        # Real entries carry cv_detections + meta.riid — mirror the shape so
        # clients can rely on the keys existing. `sub_type` is omitted by the
        # real API when absent, not nulled.
        # Recorded shape: attributes are exactly start/end/event_type/
        # is_third_party_reviewed — the API withholds sub_type from history
        # entirely (it's a filter-only field; clients must reconstruct it).
        attrs: dict[str, Any] = {
            "event_type": self.event_type,
            "is_third_party_reviewed": self.reviewed,
            "start": self.start,
            "end": self.end,
        }
        return {
            "type": "history-events",
            "id": self.id,
            "attributes": attrs,
            "relationships": {
                "source": {"data": {"type": "devices", "id": device_id}},
                "cv_detections": {"data": self.cv_detections},
            },
            "meta": {"riid": self.riid},
        }

    def matches(self, filters: list[str]) -> bool:
        if not filters:
            return True
        for f in filters:
            etype, _, sub = f.partition(".")
            if etype == self.event_type and (not sub or sub == self.sub_type):
                return True
        return False


@dataclass
class SandboxDevice:
    kind: DeviceKind
    name: str
    id: str = field(default_factory=new_device_id)
    online: bool = True
    location_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    country: str = "US"
    state: str = "WA"
    battery: int | None = None
    components: list[str] = field(default_factory=list)
    # sensor state
    faulted: bool = False
    tamper: bool = False
    temperature_c: float | None = None
    humidity_pct: float | None = None
    history: list[HistoryRecord] = field(default_factory=list)
    reported_at: int = field(default_factory=now_ms)
    # Recorded JSON:API resources loaded from `record` fixtures — served
    # verbatim (mutable fields overlaid) so a recorded world reproduces the
    # real API's exact payload shape, enums and null keys included.
    recorded: dict[str, dict[str, Any]] = field(default_factory=dict)

    # ------------------------------------------------------------- JSON:API views

    @property
    def is_camera(self) -> bool:
        return self.kind in CAMERA_KINDS

    def to_resource(self) -> dict[str, Any]:
        recorded_dev = self.recorded.get("device") or {}
        recorded_rels = recorded_dev.get("relationships") or {}
        rel = {
            name: {
                "data": {
                    "type": rtype,
                    "id": (recorded_rels.get(name) or {}).get("data", {}).get("id")
                    or (f"{self.id}.{name}" if rtype != "locations" else self.location_id),
                },
                "links": {"related": f"/v1/devices/{self.id}/{name}"},
            }
            for name, rtype in (
                ("status", "device-status"),
                ("capabilities", "device-capabilities"),
                ("configurations", "device-configurations"),
                ("location", "locations"),
            )
        }
        attrs = dict(recorded_dev.get("attributes") or {})
        # mutable bit the emulator owns; everything else verbatim from the
        # recorded resource so a replayed world reproduces the real shapes
        attrs["name"] = self.name
        if not recorded_dev:
            attrs["image_url"] = f"https://sandbox.invalid/images/{self.kind}.png"
        return {
            "type": "devices",
            "id": self.id,
            "attributes": attrs,
            "relationships": rel,
        }

    def status_resource(self) -> dict[str, Any]:
        recorded_attrs = dict(self.recorded.get("status", {}).get("attributes", {}))
        # A recorded world serves the real payload verbatim except the mutable
        # bits the emulator genuinely owns (online state, last-report time).
        attrs: dict[str, Any] = recorded_attrs or {"online": self.online}
        if recorded_attrs:
            attrs["online"] = self.online
            attrs["reported_at"] = datetime.fromtimestamp(
                self.reported_at / 1000, tz=UTC
            ).isoformat()
        elif self.kind in SENSOR_KINDS or self.kind == DeviceKind.CHIME or self.battery is not None:
            attrs["reported_at"] = datetime.fromtimestamp(
                self.reported_at / 1000, tz=UTC
            ).isoformat()
            attrs["signal_strength"] = {"value": "good"}
            attrs["battery_status"] = {
                "percentage": self.battery if self.battery is not None else 255
            }
        if self.is_camera:
            attrs.setdefault("audio", {"snooze": {"active": None, "until": None}})
            attrs.setdefault("state", None)
            # Cameras always report a last-seen timestamp on the real API.
            attrs.setdefault(
                "reported_at",
                datetime.fromtimestamp(self.reported_at / 1000, tz=UTC).isoformat(),
            )
        if self.kind == DeviceKind.CONTACT_SENSOR:
            attrs["contact_detection"] = {"faulted": self.faulted}
            attrs["tamper_detection"] = {"detected": self.tamper}
            attrs["sensor_reporting_state"] = {"value": "active"}
        elif self.kind == DeviceKind.FLOOD_FREEZE_SENSOR:
            attrs["flood_detection"] = {"faulted": self.faulted}
            attrs["freeze_detection"] = {"faulted": False}
            attrs["tamper_detection"] = {"detected": self.tamper}
        elif self.kind in (DeviceKind.TEMP_HUMIDITY_SENSOR, DeviceKind.AIR_QUALITY_MONITOR):
            attrs["temperature"] = {"value": self.temperature_c, "unit": "celsius"}
            attrs["humidity"] = {"value": self.humidity_pct, "unit": "percent"}
        if self.kind == DeviceKind.CHIME:
            attrs["audio"] = {"snooze": {"active": False, "until": None}}
        rid = self.recorded.get("status", {}).get("id", f"{self.id}.status")
        return {"type": "device-status", "id": rid, "attributes": attrs}

    def capabilities_resource(self, component_id: str | None = None) -> dict[str, Any]:
        if "capabilities" in self.recorded:
            return self.recorded["capabilities"]
        null_video = {
            "configurations": None,
            "codecs": None,
            "ratio": None,
            "max_resolution": None,
            "supported_resolutions": None,
        }
        attrs: dict[str, Any]
        if self.is_camera:
            attrs = {
                "video": {
                    "configurations": ["resolution_mode"],
                    "codecs": ["AVC", "HEVC"],
                    "ratio": "16:9",
                    "max_resolution": 1080,
                    "supported_resolutions": [1080, 720],
                },
                "motion_detection": {"configurations": ["enabled", "motion_zones"]},
                "image_enhancements": {
                    "configurations": ["color_night_vision", "hdr", "privacy_zones", "snapshot"]
                },
                # The real camera payload carries these keys nulled, not absent —
                # clients distinguish "unsupported" (null) from "unknown" (missing).
                **{
                    k: None
                    for k in (
                        "audio",
                        "flood_detection",
                        "tamper_detection",
                        "glass_break_detection",
                        "smoke_detection",
                        "freeze_detection",
                        "co_detection_listener",
                        "battery_status",
                        "contact_detection",
                    )
                },
            }
            if self.components:
                attrs["components"] = {
                    "items": [
                        {"component_id": c, "video": {**attrs["video"], "component_id": c}}
                        for c in self.components
                    ]
                }
                if component_id:
                    attrs["video"]["component_id"] = component_id
        elif self.kind == DeviceKind.CHIME:
            attrs = {
                "video": null_video,
                "motion_detection": {"configurations": None},
                "image_enhancements": {"configurations": None},
                "audio": {"customizable_slots": 2, "supported_actions": ["chime.play"]},
            }
        else:
            attrs = {
                "video": null_video,
                "motion_detection": {"configurations": None},
                "image_enhancements": {"configurations": None},
                "battery_status": {"supported": True} if self.battery is not None else None,
            }
        return {
            "type": "device-capabilities",
            "id": f"{self.id}.capabilities",
            "attributes": attrs,
            "relationships": {
                "configurations": {"links": {"related": f"/v1/devices/{self.id}/configurations"}}
            },
        }

    def configurations_resource(self) -> dict[str, Any]:
        if "configurations" in self.recorded:
            return self.recorded["configurations"]
        if self.is_camera:
            # real API uses "on"/"off" string enums, and exposes audio volume
            # plus ir_led_night_vision/auto_zoom_track keys on cameras
            attrs: dict[str, Any] = {
                "motion_detection": {"enabled": "on", "motion_zones": []},
                "image_enhancements": {
                    "color_night_vision": "off",
                    "hdr": "off",
                    "ir_led_night_vision": "off",
                    "auto_zoom_track": "off",
                    "privacy_zones": [],
                },
                "audio": {"customizable_slots": None, "volume": 11},
            }
        elif self.kind == DeviceKind.CHIME:
            attrs = {
                "motion_detection": {"enabled": None},
                "audio": {
                    "volume": 7,
                    "customizable_slots": [
                        {
                            "event": "ring-appstore-event-1",
                            "disabled": False,
                            "audio_ref": str(uuid.uuid4()),
                            "audio_url": "https://sandbox.invalid/audio/1.wav",
                            "audio_name": "Sandbox Tone 1",
                        },
                        {
                            "event": "ring-appstore-event-2",
                            "disabled": False,
                            "audio_ref": str(uuid.uuid4()),
                            "audio_url": "https://sandbox.invalid/audio/2.wav",
                            "audio_name": "Sandbox Tone 2",
                        },
                    ],
                },
            }
        else:
            attrs = {"motion_detection": {"enabled": None}, "image_enhancements": None}
            if self.kind in (DeviceKind.TEMP_HUMIDITY_SENSOR, DeviceKind.AIR_QUALITY_MONITOR):
                attrs["thresholds"] = {
                    "temperature": {"min": 10, "max": 30},
                    "humidity": {"min": 20, "max": 70},
                }
        return {
            "type": "device-configurations",
            "id": f"{self.id}.configurations",
            "attributes": attrs,
        }

    def location_resource(self) -> dict[str, Any]:
        if "location" in self.recorded:
            return self.recorded["location"]
        return {
            "type": "locations",
            "id": self.location_id,
            "attributes": {"country": self.country, "state": self.state},
        }


@dataclass
class WebhookTarget:
    url: str
    signing_key: str


@dataclass
class Chaos:
    """Fault-injection profile: probabilities applied to webhook delivery and
    flaky endpoints. ``seed`` makes the fault stream deterministic so chaos
    runs are reproducible in tests."""

    seed: int = 0
    duplicate: float = 0.0
    drop: float = 0.0
    delay_ms: int = 0
    jitter_ms: int = 0
    flaky_media: float = 0.0
    flaky_history: float = 0.0
    rate_limit: float = 0.0
    unavailable: float = 0.0

    def rng(self):
        import random

        return random.Random(self.seed)


CHAOS_PRESETS: dict[str, Chaos] = {
    "delivery": Chaos(duplicate=0.25, drop=0.10, delay_ms=300, jitter_ms=1500),
    "flaky": Chaos(flaky_media=0.30, flaky_history=0.20),
    "limited": Chaos(rate_limit=0.40, unavailable=0.10),
    "storm": Chaos(
        duplicate=0.30,
        drop=0.15,
        delay_ms=300,
        jitter_ms=2000,
        flaky_media=0.20,
        flaky_history=0.20,
        rate_limit=0.15,
        unavailable=0.05,
    ),
}


@dataclass
class World:
    devices: dict[str, SandboxDevice] = field(default_factory=dict)
    webhooks: list[WebhookTarget] = field(default_factory=list)
    account_id: str = ACCOUNT_ID
    user: dict[str, Any] = field(
        default_factory=lambda: {
            "first_name": "Sandbox",
            "last_name": "User",
            "email": "sandbox@example.com",
            "phone_number": None,
        }
    )
    required_token: str | None = None
    token_scopes: dict[str, set[str]] = field(default_factory=dict)
    enforce_subscriptions: bool = False
    media_dir: Path | None = None
    delivered: list[dict[str, Any]] = field(default_factory=list)
    app_integration_status: str | None = None
    # ms epoch at which the user consented (app-integration completed) — the
    # real API exposes no history/media from before that instant. None = the
    # consent flow never ran on this world (no boundary enforced).
    consent_at: int | None = None
    # authorization_code grant support: minted codes (60 s TTL) → client_id.
    authz_codes: dict[str, dict[str, Any]] = field(default_factory=dict)
    subscriptions: dict[str, dict[str, Any]] = field(default_factory=dict)
    whep_sessions: dict[str, dict[str, Any]] = field(default_factory=dict)
    # steady-state rate-limit accounting (X-RateLimit-* headers, /v1 only)
    _rl_window: int = 0
    _rl_used: int = 0

    # ----------------------------------------------------------- token auth

    def scopes_for(self, token: str) -> set[str] | None:
        """Scopes recorded for a bearer token — ``None`` means unconstrained
        (a token not registered carries full access, matching pre-gating
        behavior for worlds that never touch scope state)."""
        return self.token_scopes.get(token)

    def device_subscribed(self, device_id: str) -> bool:
        """True when the device has a subscription/trial that is both in an
        entitled state and unexpired — what gates event delivery and media."""
        now = datetime.now(tz=UTC)
        for sub in self.subscriptions.values():
            if sub["device_id"] != device_id:
                continue
            if sub["state"] not in ("active", "trialing"):
                continue
            if datetime.fromisoformat(sub["expires_at"].replace("Z", "+00:00")) > now:
                return True
        return False

    # ------------------------------------------------------------- devices

    def add(self, device: SandboxDevice) -> SandboxDevice:
        self.devices[device.id] = device
        return device

    def get(self, device_id: str) -> SandboxDevice | None:
        return self.devices.get(device_id)

    def remove(self, device_id: str) -> SandboxDevice | None:
        return self.devices.pop(device_id, None)

    def cameras(self) -> list[SandboxDevice]:
        return [d for d in self.devices.values() if d.is_camera]

    # ------------------------------------------------------------- events

    def record_event(
        self,
        device_id: str,
        event_type: str,
        *,
        sub_type: str | None = None,
        at_ms: int | None = None,
        duration_ms: int = 20_000,
    ) -> HistoryRecord | None:
        """Apply an event to device state and, for camera events, append to history."""
        dev = self.devices[device_id]
        at_ms = at_ms or now_ms()
        dev.reported_at = at_ms
        if event_type == WebhookEventType.DEVICE_OFFLINE:
            dev.online = False
        elif event_type == WebhookEventType.DEVICE_ONLINE:
            dev.online = True
        elif event_type in (
            WebhookEventType.CONTACT_SENSOR_FAULTED,
            WebhookEventType.FLOOD_DETECTED,
        ):
            dev.faulted = True
        elif event_type in (
            WebhookEventType.CONTACT_SENSOR_CLEARED,
            WebhookEventType.FLOOD_CLEARED,
        ):
            dev.faulted = False
        elif event_type == WebhookEventType.TAMPER_DETECTED:
            dev.tamper = True
        elif event_type == WebhookEventType.TAMPER_CLEARED:
            dev.tamper = False
        elif event_type == WebhookEventType.SUBSCRIPTION_ACTIVATED:
            # Keep entitlement state truthful with the delivered event: an
            # activation creates the plan row it announces.
            if not self.device_subscribed(device_id):
                now = datetime.now(tz=UTC)
                self.subscriptions[f"sub_{device_id}.{at_ms}"] = {
                    "id": f"sub_{device_id}.{at_ms}",
                    "device_id": device_id,
                    "plan_id": "sandbox.protect.basic",
                    "state": "active",
                    "expires_at": (now + timedelta(days=30)).isoformat().replace("+00:00", "Z"),
                    "created_at": now.isoformat().replace("+00:00", "Z"),
                }
        elif event_type == WebhookEventType.SUBSCRIPTION_DEACTIVATED:
            # Lapse voids the entitlement it announces — subsequent gated
            # surfaces (history/media/WHEP/webhook fan-out) reflect it.
            for sub in self.subscriptions.values():
                if sub["device_id"] == device_id and sub["state"] in ("active", "trialing"):
                    sub["state"] = "canceled"
                    sub["expires_at"] = datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")
        hist_type = _HISTORY_TYPE.get(event_type)  # type: ignore[call-overload]
        if hist_type is None:
            return None
        if hist_type == "motion" and sub_type is None:
            sub_type = MotionSubType.OTHER_MOTION
        rec = HistoryRecord(
            id=f"{device_id}.{hist_type}.{at_ms}",
            event_type=hist_type,
            sub_type=sub_type,
            start=at_ms,
            end=at_ms + duration_ms,
            # Motion recordings carry a Ring-internal recording id (meta.riid);
            # on_demand/ding rows stay null like the real API.
            riid=str(uuid.uuid4()) if hist_type == "motion" else None,
        )
        dev.history.append(rec)
        dev.history.sort(key=lambda r: r.start, reverse=True)
        return rec

    def record_on_demand(
        self, device_id: str, at_ms: int | None = None, duration_ms: int = 30_000
    ) -> HistoryRecord:
        """Append an ``on_demand`` history record. The real API creates one whenever
        media is requested from a camera (snapshot/clip downloads, live views, and
        Playground event triggers all surface this way)."""
        dev = self.devices[device_id]
        at_ms = at_ms or now_ms()
        rec = HistoryRecord(
            id=f"{device_id}.on_demand.{at_ms}",
            event_type="on_demand",
            sub_type=None,
            start=at_ms,
            end=at_ms + duration_ms,
            reviewed=True,
        )
        dev.history.append(rec)
        dev.history.sort(key=lambda r: r.start, reverse=True)
        return rec

    def recording_covering(self, device_id: str, ts_ms: int) -> HistoryRecord | None:
        dev = self.devices[device_id]
        for rec in dev.history:
            # on_demand entries mark a media request, not an event recording — they
            # don't extend clip coverage.
            if rec.event_type != "on_demand" and rec.start <= ts_ms <= rec.end:
                return rec
        return None

    # ------------------------------------------------------------- media

    def snapshot_bytes(self, device: SandboxDevice, ts_ms: int, fmt: str) -> tuple[bytes, str]:
        """Return (content, mime). Uses ``media_dir/<device_id>.<ext>`` or ``default.<ext>``
        when present, otherwise a generated placeholder PNG."""
        if self.media_dir:
            for ext in ("jpg", "jpeg", "png"):
                for stem in (device.id, "default"):
                    p = self.media_dir / f"{stem}.{ext}"
                    if p.exists():
                        return p.read_bytes(), "image/jpeg" if ext != "png" else "image/png"
        return _placeholder_png(device, ts_ms), "image/png"

    def clip_bytes(self, device: SandboxDevice) -> bytes:
        if self.media_dir:
            for stem in (device.id, "default"):
                p = self.media_dir / f"{stem}.mp4"
                if p.exists():
                    return p.read_bytes()
        # Minimal MP4 'ftyp' box: enough for content-type sniffing and hashing, not playable.
        return (
            struct.pack(">I", 20)
            + b"ftypisom"
            + struct.pack(">I", 0x200)
            + b"isom"
            + struct.pack(">I", 8)
            + b"mdat"
        )


# --------------------------------------------------------------------------- seeds


def _kind_from_recorded(device: dict[str, Any], caps: dict[str, Any] | None) -> DeviceKind:
    """Best-effort device kind from a recorded JSON:API resource — the Partner
    API doesn't ship a kind field, so sniff the image path and capabilities."""
    url = (device.get("attributes") or {}).get("image_url", "").lower()
    if "doorbell" in url:
        return DeviceKind.DOORBELL
    video = ((caps or {}).get("attributes") or {}).get("video") or {}
    if video.get("configurations"):
        return DeviceKind.CAMERA
    if "chime" in url:
        return DeviceKind.CHIME
    return DeviceKind.CONTACT_SENSOR


def load_fixture_docs(world: World, docs: dict[str, dict[str, Any]]) -> dict[str, int]:
    """Load `ring-sandbox record` output into a world: ``{filename: parsed
    json}`` — devices.json, me.json, history.<device_id>.json. Recorded
    devices keep their real IDs and serve their recorded included resources
    verbatim, so a recorded world reproduces the real API's exact shapes."""
    import json as _json
    from pathlib import Path as _Path

    counts = {"devices": 0, "history": 0}
    # accept either a {filename: doc} map or a directory path
    if isinstance(docs, (str, _Path)):
        root = _Path(docs)
        docs = {p.name: _json.loads(p.read_text(encoding="utf-8")) for p in root.glob("*.json")}
    devices_doc = docs.get("devices.json")
    if devices_doc:
        included = {r["id"]: r for r in devices_doc.get("included", [])}
        for res in devices_doc.get("data", []):
            rels = res.get("relationships", {})
            recorded = {"device": res}
            for name in ("status", "capabilities", "configurations", "location"):
                rid = (rels.get(name) or {}).get("data", {}).get("id")
                if rid and rid in included:
                    recorded[name] = included[rid]
            dev = SandboxDevice(
                kind=_kind_from_recorded(res, recorded.get("capabilities")),
                name=(res.get("attributes") or {}).get("name", "Recorded device"),
                id=res["id"],
                online=(recorded.get("status", {}).get("attributes") or {}).get("online", True),
                recorded=recorded,
            )
            world.add(dev)
            counts["devices"] += 1
    me_doc = docs.get("me.json")
    if me_doc and me_doc.get("data"):
        world.user = me_doc["data"].get("attributes", world.user)
        world.account_id = me_doc["data"].get("id", world.account_id)
    for name, doc in docs.items():
        if not name.startswith("history"):
            continue
        stem = (
            name.removeprefix("history")
            .removeprefix("-on-demand.")
            .lstrip(".")
            .removesuffix(".json")
        )
        dev = world.get(stem)
        if dev is None:
            continue
        for entry in doc.get("data", []):
            attrs = entry.get("attributes", {})
            dev.history.append(
                HistoryRecord(
                    id=entry.get("id", f"{dev.id}.{attrs.get('event_type')}.{attrs.get('start')}"),
                    event_type=attrs.get("event_type", "on_demand"),
                    sub_type=attrs.get("sub_type"),
                    start=attrs.get("start", 0),
                    end=attrs.get("end", 0),
                    reviewed=attrs.get("is_third_party_reviewed", False),
                    riid=(entry.get("meta") or {}).get("riid"),
                    cv_detections=(
                        (entry.get("relationships") or {}).get("cv_detections") or {}
                    ).get("data")
                    or [],
                )
            )
            counts["history"] += 1
        dev.history.sort(key=lambda r: r.start, reverse=True)
    return counts


def default_world() -> World:
    """A plausible household: doorbell, backyard cam, chime, and a front-door contact sensor."""
    w = World()
    w.add(SandboxDevice(DeviceKind.DOORBELL, "Front Door", battery=82))
    w.add(SandboxDevice(DeviceKind.CAMERA, "Backyard"))
    w.add(SandboxDevice(DeviceKind.CHIME, "Kitchen Chime"))
    w.add(SandboxDevice(DeviceKind.CONTACT_SENSOR, "Front Door Sensor", battery=91))
    return w


# --------------------------------------------------------------------------- png


def _placeholder_png(device: SandboxDevice, ts_ms: int, size: int = 64) -> bytes:
    """Deterministic solid-colour PNG derived from device id + timestamp bucket."""
    seed = zlib.crc32(f"{device.id}:{ts_ms // 60_000}".encode())
    r, g, b = (seed >> 16) & 0xFF, (seed >> 8) & 0xFF, seed & 0xFF
    row = b"\x00" + bytes([r, g, b]) * size
    raw = row * size

    def chunk(tag: bytes, data: bytes) -> bytes:
        return (
            struct.pack(">I", len(data))
            + tag
            + data
            + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)
        )

    ihdr = struct.pack(">IIBBBBB", size, size, 8, 2, 0, 0, 0)
    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", ihdr)
        + chunk(b"IDAT", zlib.compress(raw))
        + chunk(b"IEND", b"")
    )
