# ring-sandbox

[![PyPI](https://img.shields.io/pypi/v/ring-sandbox)](https://pypi.org/project/ring-sandbox/)
[![CI](https://github.com/josepha-mayo/ring-sandbox/actions/workflows/ci.yml/badge.svg)](https://github.com/josepha-mayo/ring-sandbox/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Typed Python client **and** offline emulator for the [Ring Partner API](https://developer.amazon.com/docs/ring/api-documentation.html) (`api.amazonvision.com`).

Ring ships no SDK and no local simulator. Testing a partner integration today means a real device, a 30-minute Playground token, or hand-rolled mocks. `ring-sandbox` gives you:

- **`RingClient`** – a small, typed, synchronous client over `httpx` covering users, device discovery (with `?include=` side-loading), status, capabilities, configurations, location, event history (auto-pagination), image snapshots (303 redirect flow), media clips (200/206/416 semantics), chime playback, the app-integration lifecycle (`awaiting` → `completed`), subscription queries, and WHEP live-video sessions.
- **Webhook helpers** – HMAC-SHA256 `X-Signature` signing/verification over raw bytes, v1.1 payload construction, and parsing into a `WebhookEvent`.
- **Emulator** – a FastAPI app that speaks the same JSON:API shapes at `/v1/...`, plus a `/_sandbox` control plane to inject events, register webhook targets (the emulator signs and delivers them), add devices (doorbells, cameras, chimes, Early Access sensors), and reset.
- **Scenarios** – scripted event sequences (`delivery`, `home_aide_visit`, `short_visit`, `no_show`, `device_flap`, or your own YAML), replayable in real time, time-compressed, or back-dated into history.
- **pytest plugin** – `ring_client`, `ring_control`, `ring_world` fixtures that run the emulator in-process with no sockets.
- **Recorder** – `ring-sandbox record --token ...` snapshots real Playground/production responses into JSON fixtures.

## Install

```bash
pip install "ring-sandbox[server]"      # client + emulator + CLI
pip install ring-sandbox                # client only

# development: pinned, CI-tested dependency set
pip install -r requirements-dev.txt -e ".[dev]"
```

## Client

```python
from ring_sandbox import RingClient

with RingClient(token) as ring:                       # production by default
    for d in ring.devices(include=["status", "capabilities"]):
        print(d.name, d.online, d.capabilities.is_camera)

    cam = next(d for d in ring.devices(include=["capabilities"]) if d.capabilities.is_camera)
    for ev in ring.events(cam.id, event_types=["motion.human", "ding"]):
        print(ev.attributes.started_at, ev.attributes.event_type)

    snap = ring.snapshot_latest(cam.id, start=some_datetime)   # follows the 303 to the pre-signed URL
    clip = ring.clip(cam.id, timestamp=some_datetime, duration_ms=10_000)
    if clip.partial: print("only", clip.actual_length_ms, "ms available")
```

Media endpoints 303-redirect to a pre-signed URL on another host. Redirects are followed
manually: bearer credentials and cookies are never forwarded, the response body is capped by
`max_media_bytes`, and off-origin targets must be allowlisted — pass
`media_origins=["https://media-host.example", "*.amazonaws.com"]` when talking to real Ring.
JSON endpoints never follow redirects; device ids are treated as opaque single path segments.

Point it at the emulator with `RingClient(token, base_url="http://127.0.0.1:8787")`.

## Emulator

```bash
ring-sandbox serve --port 8787
ring-sandbox webhook http://localhost:8000/webhooks/ring --key my-hmac-key
ring-sandbox play delivery --speed 5           # courier: vehicle -> human -> ding -> package -> vehicle
ring-sandbox play home_aide_visit --backdate   # 90-minute visit written straight into history
ring-sandbox play late_arrival                 # shipped example — resolves by name, no repo needed
ring-sandbox play examples/late_arrival.yml    # or your own scenario file: name, description, steps
ring-sandbox inject --type motion_detected --sub-type human
```

Custom scenarios are plain YAML — `steps` entries take `offset_s`, `type`, optional `sub_type`, `device` (id or name), and `duration_ms`. Three documented examples ship **inside the wheel** (also in `examples/`): `late_arrival` (aide shows 25 min late), `partial_blackout` (camera dies mid-visit — departure unobserved), `visitor_not_worker` (courier activity in the aide's window). `ring_sandbox.scenarios.resolve()` tries built-ins, then shipped examples, then file paths.

Interactive docs at `http://127.0.0.1:8787/_sandbox/docs`. Drop real `default.jpg` / `default.mp4` (or `<device_id>.jpg`) in a folder and pass `--media-dir` to serve real media instead of placeholders.

### Record → replay

Capture a real API surface once, replay it in CI forever:

```bash
ring-sandbox record --token <playground token> --out fixtures/   # snapshot real responses
ring-sandbox serve --fixtures fixtures/                          # replay them in the emulator
```

`--fixtures` loads `devices.json` (+ included `device-status`/`device-capabilities`/
`device-configurations`/`locations` resources), `me.json`, and
`history[.-on-demand].<device_id>.json`. Recorded devices keep their real IDs and
serve the recorded included resources **verbatim** — only genuinely mutable fields
(`online`, `reported_at`, `name`) are overlaid by the emulator. The same load is
available at runtime via `POST /_sandbox/load` with either `{"path": "fixtures/"}`
or a `{filename: parsed_json}` body, so a test suite can replay a recording
without restarting the server. `fixtures/` in this repo is a real Playground
capture — `tests/test_fixture_replay.py` pins its parity.

### Fidelity notes

The emulator reproduces the parts of the API that bite integrators:

| Behaviour | Emulated |
|---|---|
| JSON:API compound documents via `?include=` | yes |
| History newest-first, `page[key]` cursor, dotted `event_types` filters | yes |
| History traps: `links.next` only while a further page exists, and it **drops `event_types`** (filtered queries silently widen when paged); empty history is the bare `{"data": []}` — no `links` key; `sub_type` is withheld from history (filter-only field — `event_types=motion.human` still filters) | yes |
| `meta.riid` on motion recordings, null elsewhere | yes |
| `is_third_party_reviewed` flips after media access | yes |
| Snapshot `303 See Other` to a pre-signed URL | yes |
| `latest_in_range` 416s when nothing was recorded — never a frame from another time | yes |
| Clip `206 Partial` + `X-Media-Length`, `416 TIMESTAMP_NOT_FOUND` when idle | yes |
| Consent boundary: completing the app-integration sets the consent instant — earlier history is filtered out and pre-consent media requests 403 `TIME_RANGE_NOT_AUTHORIZED` | yes |
| Offline devices 503 `device_offline` on media/WHEP | yes |
| Chime playback restricted to the app's two audio slots | yes |
| Sensor `faulted` semantics, `255` battery sentinel on mains devices | yes |
| Webhook v1.1 payloads with `sub_type`, `component_ids`, HMAC `X-Signature` | yes |
| App-integration lifecycle (`awaiting` → `completed`), `app_integration_*` + `device_added`/`removed` account-linking webhooks | yes |
| `GET /v1/accounts/me/subscriptions` + `subscription_activated`/`deactivated` webhooks (plan_id, expires_at) | yes |
| Token scopes: `--read-token T` marks `T` as `ava.v1:read`-scoped — GETs pass, mutations 403 `insufficient_scope`, the subscriptions surface 422 (all observed live) | yes |
| Plan gating: `--enforce-subscriptions` makes entitlement real — no retained history, 403 `subscription_required` on media/WHEP, and observation webhooks suppressed (journaled as `webhook.suppressed`) while `subscription_*`/`device_*` lifecycle events still deliver; injected `subscription_*` steps mutate the entitlement they announce | yes, opt-in |
| WHEP sessions: SDP offer → 201 + parseable SDP answer (ICE/DTLS/mid lines), **absolute** `Location`, `ETag` + `Link: rel="ice-server"`, 30 s battery / 60 s wired TTL (expired close → 404), `DELETE` to close, live views log `on_demand` history | yes |
| `X-RateLimit-Limit`/`X-RateLimit-Remaining` headers on every `/v1/*` response + a real 100 req/s/token limiter → 429 + `Retry-After` | yes |
| `429`/`503` + `Retry-After` rate-limit faults (`--chaos rate_limit=…`) | yes |
| Failed webhook deliveries retry twice (same `request_id` — receivers must be idempotent); any non-2xx counts as failure | yes |
| OAuth: `refresh_token` grant plus `authorization_code` (codes minted via `POST /_sandbox/authz-codes`, 60 s TTL, single-use); RFC 6749 error shapes; token body carries `scope`/`expires_in` | yes |
| JSON:API `errors[]` envelope on validation failures (no FastAPI `{"detail"}` leaks) | yes |
| Camera capability/status null-keys and `reported_at` parity with recorded payloads | yes |
| OAuth nonce verification (`nonce` body field), 403-vs-404 unconsented device distinction, RTSP live video (`rtsps://`), HEVC offer rejection | no |

## Webhooks

```python
from ring_sandbox import webhooks

@app.post("/webhooks/ring")
async def ring_hook(request: Request):
    raw = await request.body()                                    # raw bytes, never re-serialized JSON
    ev = webhooks.parse(raw, signing_key=KEY, signature=request.headers.get("X-Signature"))
    if ev.event_type == "motion_detected" and ev.sub_type == "human": ...
```

`parse(..., max_age_s=3600)` additionally bounds delivery freshness: `meta.time`
is inside the signed body, so a captured delivery replayed verbatim carries an
authentic-but-stale timestamp — a second replay defense behind `request_id`
dedupe. Omit `max_age_s` when re-verifying stored deliveries (authenticity is
timeless; freshness is an intake property).

## Chaos fault injection

`ring-sandbox serve --chaos storm` runs the emulator with a fault-injection profile:
webhook deliveries can be duplicated, dropped, or delayed with jitter; history/media
endpoints can return transient 500s; and `/v1/*` calls can be throttled with `429`/`503`
+ `Retry-After` so clients exercise backoff. Presets: `delivery` (dup/drop/delay only),
`flaky` (endpoint failures only), `limited` (429/503 only), `storm` (all). A custom
profile is a key=value list: `--chaos drop=0.2,rate_limit=0.3,jitter_ms=1500`.
`--chaos-seed N` makes the fault stream deterministic for reproducible runs.

Every injected fault is recorded — `GET /_sandbox/chaos` returns the active profile plus
the actions taken so far (`webhook.dropped`, `webhook.duplicated`, `webhook.delayed` with
the applied ms). `POST /_sandbox/chaos` adjusts rates live (`{"drop": 0.5}`) without
restarting. The point is proving the *receiver*: a correct consumer dedupes re-delivered
`request_id`s, tolerates out-of-order arrival, and keeps working through flaky polls.

Two things to know when a batch run counts faults:

- **Count from `/_sandbox/chaos`, not from `/_sandbox/state`.** `/chaos` returns the whole
  history of actions; `/state` truncates `delivered` to its last 50 entries, so counting
  faults there undercounts silently on any run of size.
- **`/_sandbox/chaos` reports `pending`**, the deliveries still in flight. Poll it to zero
  rather than sleeping: a delivery is held up to `delay_ms + jitter_ms` (1.8 s on the
  `delivery` preset), so a fixed wait is a guess about someone else's machine, and on a
  loaded one the run ends while deliveries are still out and reports fewer than were made.

`--chaos-seed N` fixes *how many* faults a run injects, for a given number of deliveries.
It does not fix *which* delivery gets which: `meta.request_id` is a fresh uuid4, so a
delivery has no identity that survives a restart, and deliveries run concurrently.

## pytest

```python
def test_visit_detection(ring_client, ring_control):
    ring_control.post("/_sandbox/events", json={"type": "motion_detected", "sub_type": "human"})
    cam = ring_client.devices()[0]
    assert next(ring_client.events(cam.id)).attributes.event_type == "motion"
```

## Status

Built during the Amazon Developer Hackathon 2026. Response shapes follow the public documentation; where the docs are ambiguous the emulator follows what the Playground returns (see `fixtures/`). Sensors and chimes are Early Access upstream and may change.

See [CHANGELOG.md](CHANGELOG.md) for release notes and [CONTRIBUTING.md](CONTRIBUTING.md) for how to help — response-shape corrections and new scenario files are the most valuable contributions.

MIT licensed.
