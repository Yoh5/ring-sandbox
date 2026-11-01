"""Typed synchronous client for the Ring Partner API.

Point ``base_url`` at ``https://api.amazonvision.com`` (default) or at a running
:mod:`ring_sandbox.emulator` instance for offline development.
"""

from __future__ import annotations

import ipaddress
import math
import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import quote

import httpx

from .models import (
    APIError,
    AppIntegration,
    Capabilities,
    Configurations,
    Device,
    DeviceBundle,
    HistoryEvent,
    HistoryPage,
    Location,
    MediaClip,
    Snapshot,
    Status,
    Subscription,
    User,
)

PRODUCTION_BASE_URL = "https://api.amazonvision.com"
DEFAULT_TOKEN_URL = "https://oauth.ring.com/oauth/token"
_INCLUDABLE = ("status", "capabilities", "configurations", "location")


class RingAPIError(Exception):
    def __init__(self, status_code: int, errors: list[APIError], body: str = ""):
        self.status_code = status_code
        self.errors = errors
        self.body = body
        super().__init__(f"Ring API returned HTTP {status_code}")

    @property
    def code(self) -> str | None:
        return self.errors[0].code if self.errors else None


@dataclass(frozen=True)
class WhepSession:
    """A WHEP live-video session: SDP answer + the session URL to DELETE."""

    session_url: str
    sdp_answer: str
    device_id: str

    @property
    def session_id(self) -> str:
        return self.session_url.rsplit("/", 1)[-1]


class RingClient:
    """Bearer-token client. Tokens from the Playground last ~30 minutes; production
    access tokens ~4 hours (refresh via :meth:`RingClient.refresh`)."""

    def __init__(
        self,
        access_token: str,
        *,
        base_url: str = PRODUCTION_BASE_URL,
        timeout: float = 30.0,
        max_retries: int = 3,
        transport: httpx.BaseTransport | None = None,
        media_origins: Iterable[str] = (),
        max_media_bytes: int = 20 * 1024 * 1024,
        max_json_bytes: int = 1024 * 1024,
        max_redirects: int = 3,
        max_retry_delay: float = 30.0,
        refresh_token: str | None = None,
        token_url: str = DEFAULT_TOKEN_URL,
        client_id: str | None = None,
        on_token_refresh: Callable[[dict], None] | None = None,
    ):
        self._has_transport = transport is not None
        origin = _origin_url(base_url)
        self._check_api_origin(origin)
        if (
            type(max_retries) is not int
            or not 0 <= max_retries <= 10
            or type(max_redirects) is not int
            or not 0 <= max_redirects <= 10
            or type(max_media_bytes) is not int
            or max_media_bytes <= 0
            or type(max_json_bytes) is not int
            or max_json_bytes <= 0
            or not math.isfinite(max_retry_delay)
            or max_retry_delay < 0
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            raise ValueError("invalid HTTP limits")
        if isinstance(media_origins, str):
            media_origins = (media_origins,)
        token_origin = _origin_url(token_url)
        if (
            token_origin.scheme != "https"
            and not transport
            and token_origin.host not in ("127.0.0.1", "localhost", "::1")
        ):
            raise ValueError("token endpoint requires HTTPS, except an explicit local emulator")
        self._refresh_token = refresh_token
        self._token_url = token_url
        self._client_id = client_id
        self._on_token_refresh = on_token_refresh
        self._token = access_token
        self.base_url = base_url
        self._media_origins: set[tuple[str, str, int]] = set()
        self._media_suffixes: list[str] = []
        for value in media_origins:
            if value.startswith("*."):
                suffix = value[2:].lower()
                if not suffix or any(c in suffix for c in "/:@?#"):
                    raise ValueError(f"invalid media origin pattern {value!r}")
                self._media_suffixes.append(suffix)
                continue
            allowed = _origin_url(value)
            if allowed.scheme != "https":
                raise ValueError("off-origin media downloads require HTTPS")
            self._media_origins.add(_origin(allowed))
        self.max_retries = max_retries
        self.max_redirects = max_redirects
        self.max_retry_delay = max_retry_delay
        self.max_media_bytes, self.max_json_bytes = max_media_bytes, max_json_bytes
        self._http = httpx.Client(
            base_url=self.base_url,
            timeout=timeout,
            follow_redirects=False,
            transport=transport,
            trust_env=False,
            headers={"Accept": "application/json", "Accept-Encoding": "identity"},
        )

    # ----------------------------------------------------------------- plumbing

    @property
    def base_url(self) -> str:
        return self._base_url

    @base_url.setter
    def base_url(self, value: str) -> None:
        origin = _origin_url(value)
        self._check_api_origin(origin)
        self._base_url = str(origin).rstrip("/")
        self._api_origin = _origin(origin)

    def _check_api_origin(self, origin: httpx.URL) -> None:
        if (
            origin.scheme == "http"
            and not self._has_transport
            and origin.host not in ("127.0.0.1", "localhost", "::1")
        ):
            raise ValueError("API origin requires HTTPS, except an explicit local emulator")

    @property
    def access_token(self) -> str:
        return self._token

    @access_token.setter
    def access_token(self, value: str) -> None:
        self._token = value

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> RingClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _media_allowed(self, url: httpx.URL, origin: tuple[str, str, int]) -> bool:
        if origin in self._media_origins:
            return True
        host = url.host.lower()
        return url.port in (None, 443) and any(
            host == suffix or host.endswith("." + suffix) for suffix in self._media_suffixes
        )

    # ------------------------------------------------------------------ auth

    def refresh(self, refresh_token: str | None = None) -> dict:
        """Exchange a refresh token for a new access token (RFC 6749 refresh grant).

        The token endpoint shape is exercised against the local emulator; verify
        against the official service before relying on it in production.
        ``on_token_refresh`` receives the new token pair so callers can persist it.
        """
        token = refresh_token or self._refresh_token
        if not token:
            raise ValueError("no refresh token available")
        form = {"grant_type": "refresh_token", "refresh_token": token}
        if self._client_id:
            form["client_id"] = self._client_id
        resp = self._post_form(self._token_url, form)
        if resp.status_code >= 400:
            raise RingAPIError(resp.status_code, _parse_errors(resp), resp.text)
        body = resp.json()
        access = body.get("access_token")
        if not isinstance(access, str) or not access:
            raise ValueError("token response missing access_token")
        self._token = access
        if isinstance(body.get("refresh_token"), str) and body["refresh_token"]:
            self._refresh_token = body["refresh_token"]
        if self._on_token_refresh is not None:
            self._on_token_refresh(
                {
                    "access_token": access,
                    "refresh_token": self._refresh_token,
                    "expires_in": body.get("expires_in"),
                    "obtained_at": datetime.now(tz=UTC).isoformat(),
                }
            )
        return body

    def _post_form(self, url: str, data: dict[str, str]) -> httpx.Response:
        request = self._http.build_request("POST", url, data=data)
        resp = self._http.send(request, stream=True, follow_redirects=False, auth=None)
        try:
            content = _read_bounded(resp, self.max_json_bytes)
            return httpx.Response(
                resp.status_code, headers=resp.headers, content=content, request=request
            )
        finally:
            resp.close()

    def _headers(self, **extra: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}", **extra}

    def _request(
        self, method: str, path: str, *, media: bool = False, **kwargs: Any
    ) -> httpx.Response:
        if not path.startswith("/") or path.startswith("//"):
            raise ValueError("API requests require an origin-relative path")
        url = httpx.URL(self.base_url).join(path)
        if _origin(url) != self._api_origin:
            raise ValueError("API origin cannot change after client construction")
        extra_headers = kwargs.pop("headers", {})
        headers = self._headers(**extra_headers)
        attempt = redirects = 0
        credentialed = True
        refreshed = False
        while True:
            request = self._http.build_request(method, url, headers=headers, **kwargs)
            request.headers.pop("Cookie", None)
            if not credentialed:
                request.headers.pop("Authorization", None)
            resp = self._http.send(request, stream=True, follow_redirects=False, auth=None)
            try:
                if (
                    resp.status_code == 401
                    and credentialed
                    and self._refresh_token
                    and not refreshed
                ):
                    self.refresh()
                    headers = self._headers(**extra_headers)
                    refreshed = True
                    continue
                if resp.status_code in (301, 302, 303, 307, 308):
                    location = resp.headers.get("Location")
                    if not media or not location or redirects >= self.max_redirects:
                        raise ValueError("unexpected or excessive redirect")
                    target = request.url.join(location)
                    if target.scheme not in ("https", "http") or not target.host:
                        raise ValueError("unsafe media redirect")
                    if target.username or target.password or target.fragment:
                        raise ValueError("unsafe media redirect")
                    target_origin = _origin(target)
                    if target_origin != self._api_origin:
                        ip = _ip_literal(target.host)
                        if (
                            target.scheme != "https"
                            or not self._media_allowed(target, target_origin)
                            or (ip is not None and not ip.is_global)
                        ):
                            raise ValueError(
                                f"media redirect origin {target.host!r} is not approved; "
                                "allow it via media_origins"
                            )
                        if resp.status_code in (307, 308):
                            raise ValueError("off-origin body-preserving redirect is not supported")
                    if resp.status_code == 303 or (
                        resp.status_code in (301, 302) and method == "POST"
                    ):
                        method, kwargs = "GET", {}
                        headers.pop("Content-Type", None)
                    else:
                        kwargs.pop("params", None)
                    credentialed = target_origin == self._api_origin
                    url = target
                    redirects += 1
                    continue
                if resp.status_code == 429 and attempt < self.max_retries:
                    delay = _retry_delay(resp.headers.get("Retry-After"), attempt)
                    if delay <= self.max_retry_delay:
                        resp.close()
                        time.sleep(delay)
                        attempt += 1
                        continue
                limit = self.max_media_bytes if media and resp.is_success else self.max_json_bytes
                content = _read_bounded(resp, limit)
                result = httpx.Response(
                    resp.status_code, headers=resp.headers, content=content, request=request
                )
                if result.status_code >= 400:
                    raise RingAPIError(result.status_code, _parse_errors(result), result.text)
                return result
            finally:
                resp.close()

    def _get_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._request("GET", path, params=params).json()

    # ----------------------------------------------------------------- account

    def me(self) -> User:
        return User.model_validate(self._get_json("/v1/users/me")["data"])

    # --------------------------------------------------------- app integrations

    def app_integration(self) -> AppIntegration | None:
        """Current partner-integration state, or ``None`` when the account
        link has not been confirmed (or was unlinked)."""
        data = self._get_json("/v1/accounts/me/app-integrations")["data"]
        return AppIntegration.model_validate(data) if data else None

    def link_app_integration(self, nonce: str | None = None) -> AppIntegration:
        """POST the account-link confirmation (nonce verification) — moves the
        integration to ``awaiting``. Idempotent once linked."""
        body = {"nonce": nonce} if nonce is not None else {}
        data = self._request("POST", "/v1/accounts/me/app-integrations", json=body).json()["data"]
        return AppIntegration.model_validate(data)

    def update_app_integration(self, status: str) -> AppIntegration:
        """PATCH the integration status — the real flow only moves forward to
        ``completed``, which finalizes the link in the Ring app UI."""
        data = self._request(
            "PATCH", "/v1/accounts/me/app-integrations", json={"status": status}
        ).json()["data"]
        return AppIntegration.model_validate(data)

    # ------------------------------------------------------------- subscriptions

    def subscriptions(self) -> list[Subscription]:
        """Subscriptions/trials this app holds for the user (per-device)."""
        doc = self._get_json("/v1/accounts/me/subscriptions")
        return [Subscription.model_validate(r) for r in doc["data"]]

    # ----------------------------------------------------------------- devices

    def devices(self, include: Iterable[str] = ()) -> list[DeviceBundle]:
        include = tuple(include)
        unknown = set(include) - set(_INCLUDABLE)
        if unknown:
            raise ValueError(f"unknown include(s) {sorted(unknown)}; allowed: {_INCLUDABLE}")
        params = {"include": ",".join(include)} if include else None
        doc = self._get_json("/v1/devices", params=params)
        return _bundle(doc)

    def device(self, device_id: str) -> Device:
        return Device.model_validate(self._get_json(f"/v1/devices/{_path_id(device_id)}")["data"])

    def status(self, device_id: str) -> Status:
        return Status.model_validate(
            self._get_json(f"/v1/devices/{_path_id(device_id)}/status")["data"]
        )

    def capabilities(self, device_id: str, component_id: str | None = None) -> Capabilities:
        params = {"component_id": component_id} if component_id else None
        doc = self._get_json(f"/v1/devices/{_path_id(device_id)}/capabilities", params=params)
        return Capabilities.model_validate(doc["data"])

    def configurations(self, device_id: str, component_id: str | None = None) -> Configurations:
        params = {"component_id": component_id} if component_id else None
        doc = self._get_json(f"/v1/devices/{_path_id(device_id)}/configurations", params=params)
        return Configurations.model_validate(doc["data"])

    def location(self, device_id: str) -> Location:
        return Location.model_validate(
            self._get_json(f"/v1/devices/{_path_id(device_id)}/location")["data"]
        )

    # ----------------------------------------------------------------- history

    def events_page(
        self,
        device_id: str,
        *,
        event_types: Iterable[str] | None = None,
        page_key: str | None = None,
    ) -> HistoryPage:
        params: dict[str, Any] = {}
        if event_types:
            params["event_types"] = ",".join(event_types)
        if page_key:
            params["page[key]"] = page_key
        doc = self._get_json(
            f"/v1/history/devices/{_path_id(device_id)}/events", params=params or None
        )
        return HistoryPage.model_validate(doc)

    def events(
        self,
        device_id: str,
        *,
        event_types: Iterable[str] | None = None,
        since: datetime | None = None,
        max_pages: int = 20,
    ) -> Iterator[HistoryEvent]:
        """Iterate newest-first through history, following ``links.next``.

        Stops early once events are older than ``since`` (history is newest-first).
        """
        event_types = tuple(event_types) if event_types else None
        if type(max_pages) is not int or max_pages <= 0:
            raise ValueError("max_pages must be a positive int")
        page_key: str | None = None
        since_ms = int(since.timestamp() * 1000) if since else None
        for _ in range(max_pages):
            page = self.events_page(device_id, event_types=event_types, page_key=page_key)
            if not page.data:
                return
            for ev in page.data:
                if since_ms is not None and ev.attributes.start < since_ms:
                    return
                yield ev
            page_key = page.next_key
            if not page_key:
                return

    # ----------------------------------------------------------------- media

    def snapshot_at(
        self,
        device_id: str,
        timestamp: datetime | int,
        *,
        fmt: str = "jpeg",
        width: int | None = None,
        height: int | None = None,
        component_id: str | None = None,
    ) -> Snapshot:
        body: dict[str, Any] = {"type": "at_timestamp", "timestamp": _ms(timestamp)}
        return self._snapshot(device_id, body, fmt, width, height, component_id)

    def snapshot_latest(
        self,
        device_id: str,
        start: datetime | int,
        end: datetime | int | None = None,
        *,
        fmt: str = "jpeg",
        width: int | None = None,
        height: int | None = None,
        component_id: str | None = None,
    ) -> Snapshot:
        body: dict[str, Any] = {"type": "latest_in_range", "start_timestamp": _ms(start)}
        if end is not None:
            body["end_timestamp"] = _ms(end)
        return self._snapshot(device_id, body, fmt, width, height, component_id)

    def _snapshot(
        self,
        device_id: str,
        body: dict[str, Any],
        fmt: str,
        width: int | None,
        height: int | None,
        component_id: str | None,
    ) -> Snapshot:
        if type(fmt) is not str or not fmt:
            raise ValueError("fmt must be a non-empty string")
        opts: dict[str, Any] = {"format": fmt}
        resolution = _resolution(width, height)
        if resolution:
            opts["resolution"] = resolution
        body["image_options"] = opts
        if component_id:
            if type(component_id) is not str:
                raise ValueError("component_id must be a string")
            body["components"] = [{"component_id": component_id}]
        resp = self._request(
            "POST",
            f"/v1/devices/{_path_id(device_id)}/media/image/download",
            json=body,
            media=True,
            headers={"Content-Type": "application/json", "Accept": "*/*"},
        )
        ts = resp.headers.get("X-Media-Timestamp")
        return Snapshot(
            content=resp.content,
            content_type=resp.headers.get("Content-Type", f"image/{fmt}"),
            timestamp=int(ts) if ts else None,
        )

    def clip(
        self,
        device_id: str,
        timestamp: datetime | int,
        duration_ms: int,
        *,
        codec: str | None = None,
        frame_rate: int | None = None,
        width: int | None = None,
        height: int | None = None,
        audio: bool = False,
        component_id: str | None = None,
    ) -> MediaClip:
        """Download an existing recording. Does NOT trigger recording; expect 416 if idle."""
        if type(duration_ms) is not int or not 0 < duration_ms <= 900_000:
            raise ValueError("duration_ms must be an int in (0, 900000] (15 minutes)")
        if codec is not None and (type(codec) is not str or not codec):
            raise ValueError("codec must be a non-empty string")
        if frame_rate is not None and (type(frame_rate) is not int or frame_rate <= 0):
            raise ValueError("frame_rate must be a positive int")
        if component_id is not None and (type(component_id) is not str or not component_id):
            raise ValueError("component_id must be a non-empty string")
        body: dict[str, Any] = {"timestamp": _ms(timestamp), "duration": duration_ms}
        video: dict[str, Any] = {}
        if codec:
            video["codec"] = codec
        if frame_rate:
            video["frame_rate"] = frame_rate
        resolution = _resolution(width, height)
        if resolution:
            video["resolution"] = resolution
        if video:
            body["video_options"] = video
        if audio:
            body["audio_options"] = {"audio_enabled": True}
        if component_id:
            body["components"] = [{"component_id": component_id}]
        resp = self._request(
            "POST",
            f"/v1/devices/{_path_id(device_id)}/media/video/download",
            json=body,
            media=True,
            headers={"Content-Type": "application/json", "Accept": "*/*"},
        )
        ts = resp.headers.get("X-Media-Timestamp")
        ln = resp.headers.get("X-Media-Length")
        return MediaClip(
            content=resp.content,
            content_type=resp.headers.get("Content-Type", "video/mp4"),
            partial=resp.status_code == 206,
            actual_timestamp=int(ts) if ts else None,
            actual_length_ms=int(ln) if ln else None,
        )

    # ----------------------------------------------------------------- chimes

    def chime_play(self, device_id: str, slot_event: str) -> dict[str, Any]:
        """Play one of the app's audio slots (e.g. ``ring-appstore-event-1``) on a chime."""
        if type(slot_event) is not str or not slot_event:
            raise ValueError("slot_event must be a non-empty string")
        resp = self._request(
            "POST",
            f"/v1/devices/{_path_id(device_id)}/media/audio/playback",
            json={"event": slot_event},
            headers={"Content-Type": "application/json"},
        )
        return resp.json() if resp.content else {}

    # ----------------------------------------------------------------- live video (WHEP)

    def whep_session(
        self, device_id: str, sdp_offer: str | bytes, component_id: str | None = None
    ) -> WhepSession:
        """Start a WebRTC/WHEP live-video session (video only, recvonly).

        ``sdp_offer`` is the full SDP from the client's ``RTCPeerConnection``;
        the response carries the SDP answer and a ``Location`` session URL.
        """
        if type(device_id) is not str or not device_id:
            raise ValueError("device_id must be a non-empty string")
        body = sdp_offer.encode() if isinstance(sdp_offer, str) else sdp_offer
        if not body.lstrip().startswith(b"v="):
            raise ValueError("sdp_offer must be an SDP offer (starts with v=)")
        params = {"component_id": component_id} if component_id else None
        resp = self._request(
            "POST",
            f"/v1/devices/{_path_id(device_id)}/media/streaming/whep/sessions",
            params=params,
            content=body,
            headers={"Content-Type": "application/sdp"},
        )
        location = resp.headers.get("Location")
        if resp.status_code != 201 or not location:
            raise ValueError("WHEP session response missing 201/Location")
        return WhepSession(session_url=location, sdp_answer=resp.text, device_id=device_id)

    def whep_close(self, session: WhepSession | str) -> None:
        """Close a WHEP session — pass the session object or its ``Location`` URL.
        Accepts the origin-relative path or an absolute URL on the API origin
        (the real API returns absolute URLs)."""
        url = session.session_url if isinstance(session, WhepSession) else session
        if type(url) is not str or not url:
            raise ValueError("session_url must be a non-empty Location URL")
        if not url.startswith("/"):
            absolute = _origin_url(url)
            url = absolute.path + (f"?{absolute.query.decode()}" if absolute.query else "")
        if not url.startswith("/v1/"):
            raise ValueError("session_url must be a /v1/ session URL")
        self._request("DELETE", url)


# --------------------------------------------------------------------------- helpers


def _path_id(value: str) -> str:
    """Encode an opaque resource id for use as exactly one path segment."""
    if type(value) is not str or not value:
        raise ValueError("id must be a non-empty string")
    return quote(value, safe="")


def _origin_url(value: str) -> httpx.URL:
    try:
        url = httpx.URL(value)
    except Exception as exc:
        raise ValueError(f"invalid URL {value!r}") from exc
    if url.scheme not in ("https", "http") or not url.host:
        raise ValueError(f"invalid URL {value!r}")
    if url.username or url.password:
        raise ValueError("credentials are not allowed in URLs")
    return url


def _origin(url: httpx.URL) -> tuple[str, str, int]:
    scheme = url.scheme.lower()
    port = url.port or (443 if scheme == "https" else 80)
    return (scheme, url.host.lower(), port)


def _ip_literal(host: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
    try:
        return ipaddress.ip_address(host.split("%", 1)[0])
    except ValueError:
        return None


def _retry_delay(header: str | None, attempt: int) -> float:
    delay: float | None = None
    if header:
        try:
            delay = float(header)
        except ValueError:
            try:
                delay = (parsedate_to_datetime(header) - datetime.now(tz=UTC)).total_seconds()
            except Exception:
                delay = None
    if delay is None or not math.isfinite(delay) or delay < 0:
        delay = 2.0**attempt
    return delay


def _read_bounded(resp: httpx.Response, limit: int) -> bytes:
    """Read a response body, aborting the stream as soon as it exceeds ``limit``."""
    try:
        declared = int(resp.headers.get("Content-Length", ""))
    except ValueError:
        declared = -1
    if declared > limit:
        raise ValueError(f"response exceeds the {limit}-byte limit")
    chunks: list[bytes] = []
    total = 0
    for chunk in resp.iter_bytes():
        total += len(chunk)
        if total > limit:
            raise ValueError(f"response exceeds the {limit}-byte limit")
        chunks.append(chunk)
    return b"".join(chunks)


def _ms(value: datetime | int) -> int:
    if type(value) is int:
        if value < 0:
            raise ValueError("timestamp must not be negative")
        return value
    if isinstance(value, datetime) and value.tzinfo is not None and value.utcoffset() is not None:
        return int(value.timestamp() * 1000)
    raise ValueError("timestamp must be epoch milliseconds or a timezone-aware datetime")


def _resolution(width: int | None, height: int | None) -> dict[str, int] | None:
    if width is None and height is None:
        return None
    if type(width) is not int or type(height) is not int or width <= 0 or height <= 0:
        raise ValueError("width and height must both be positive ints")
    return {"width": width, "height": height}


def _parse_errors(resp: httpx.Response) -> list[APIError]:
    try:
        doc = resp.json()
        # The token endpoint answers in RFC 6749 shape ({"error", "error_description"});
        # the data plane uses JSON:API errors[]. Clients must read both.
        if "errors" in doc:
            return [APIError.model_validate(e) for e in doc.get("errors") or []]
        if isinstance(doc.get("error"), str):
            return [
                APIError(
                    code=doc["error"],
                    detail=doc.get("error_description") or doc["error"],
                )
            ]
        return []
    except Exception:
        return []


def _bundle(doc: dict[str, Any]) -> list[DeviceBundle]:
    included: dict[tuple[str, str], dict[str, Any]] = {
        (r["type"], r["id"]): r for r in doc.get("included", [])
    }
    parsers = {
        "status": ("device-status", Status),
        "capabilities": ("device-capabilities", Capabilities),
        "configurations": ("device-configurations", Configurations),
        "location": ("locations", Location),
    }
    out: list[DeviceBundle] = []
    for raw in doc.get("data", []):
        device = Device.model_validate(raw)
        extras: dict[str, Any] = {}
        for rel_name, (rtype, model) in parsers.items():
            rel = device.relationships.get(rel_name)
            if rel and rel.data is not None and not isinstance(rel.data, list):
                hit = included.get((rtype, rel.data.id))
                if hit:
                    extras[rel_name] = model.model_validate(hit)
        out.append(DeviceBundle(device=device, **extras))
    return out
