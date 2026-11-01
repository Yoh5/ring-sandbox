# Changelog

## Unreleased

### Added

- **Consent boundary** — completing the app-integration sets
  `World.consent_at`: earlier history rows are filtered out and pre-consent
  media requests answer `403 TIME_RANGE_NOT_AUTHORIZED` (forum-documented
  upstream semantics).
- **`authorization_code` grant** — `POST /_sandbox/authz-codes` mints
  single-use codes (60 s TTL); `/oauth/token` exchanges them, returns the
  granted `scope` field, and answers RFC 6749 `{"error", "error_description"}`
  shapes. The client parses both error dialects.
- **Steady-state rate limiting** — `X-RateLimit-Limit`/`X-RateLimit-Remaining`
  on every `/v1/*` response plus a real 100 req/s limiter → 429.
- **Webhook retries** — failed deliveries retry twice with the same
  `request_id` (journaled `webhook.retry`); any non-2xx counts as failure.
- **WHEP fidelity** — SDP answers carry `ice-ufrag`/`ice-pwd`/`fingerprint`/
  `mid`/`setup` lines a real `RTCPeerConnection` accepts; `Location` is
  absolute; `ETag` + `Link: rel="ice-server"` headers; 30 s battery / 60 s
  wired session TTL — closing an expired session 404s.
- **Offline-device refusals** — media and WHEP routes 503 `device_offline`
  when the device is down.
- JSON:API `errors[]` envelope for request-validation failures; camera
  capability null-keys + `reported_at` parity; `component_id` accepted on
  configurations; `meta.riid` on generated motion records.

### Changed

- `latest_in_range` image requests now 416 when no recording covers the
  window — previously they silently returned a frame at the window end.
- History `links.next` only while a further page exists (final page omits
  `links` entirely, empty history is bare `{"data": []}`), and `next` drops
  `event_types` — the documented silent-widening trap. `sub_type` is
  withheld from history resources (filter-only upstream).
- Webhook `request_id` reuse across retries makes receivers' idempotency
  load-bearing — matching upstream's documented retry semantics.

### Fixed

- Image downloads logged the `on_demand` history row twice.
- Video downloads accepted `duration <= 0`.

## 0.5.0 — 2026-10-04

### Added

- **Record → replay** — `load_fixture_docs(world, docs)` plus
  `ring-sandbox serve --fixtures DIR` and `POST /_sandbox/load` replay
  `record` output back through the emulator: real device resources are
  served with their original ids and compound `included` members
  (`device-status`/`device-capabilities`/`device-configurations`/`locations`)
  verbatim, and recorded history replays with `meta.riid`,
  `cv_detections`, and absent `sub_type` preserved. Capture a real API
  surface once, replay it deterministically in CI forever.
- **Token scopes** — `World.token_scopes`, `POST|DELETE
  /_sandbox/tokens`, and `serve --read-token TOKEN` register an
  `ava.v1:read`-scoped bearer token: GETs pass, mutations answer
  `403 insufficient_scope`, and the subscriptions surface answers `422` —
  the statuses recorded from live `api.amazonvision.com` probing.
  Unregistered tokens stay unconstrained.
- **Plan enforcement** — `serve --enforce-subscriptions` (or
  `World.enforce_subscriptions`) makes entitlement real: no active
  subscription/trial means empty Event History, `403
  subscription_required` on media downloads and WHEP sessions, and
  observation webhooks suppressed (journaled as `webhook.suppressed`).
  `subscription_*`/`device_*` lifecycle events still deliver — they carry
  the entitlement signal itself.
- **Coherent subscription lifecycle** — injected
  `subscription_activated`/`subscription_deactivated` events now mutate
  the subscription rows they announce, so gated surfaces answer
  truthfully after the event; scenarios can script plan lapses that
  actually lapse.

## 0.4.1 — 2026-09-30

### Added

- **Webhook freshness verification** — `webhooks.parse()` accepts
  `max_age_s`: the sender's `meta.time` lives inside the HMAC-signed body,
  so a captured delivery replayed verbatim carries an authentic but stale
  timestamp and cannot be refreshed without breaking the signature. A
  bounded replay window that complements `request_id` dedupe — dedupe
  survives restarts, freshness survives a purged tombstone. Requires
  `signing_key` (an unsigned `meta.time` proves nothing); stale or
  future-dated deliveries raise `SignatureError`. Consumers re-verifying
  *stored* deliveries should omit `max_age_s` — authenticity is timeless,
  freshness is an intake property.

## 0.4.0 — 2026-09-29

### Added

- **App-integration lifecycle** — `POST|GET|PATCH
  /v1/accounts/me/app-integrations` mirrors the real two-step link flow:
  POST confirms the link (status `awaiting`, fires `app_integration_added`
  plus `device_added` for every consented device), PATCH
  `{"status": "completed"}` finalizes. `DELETE /_sandbox/app-integration`
  simulates the user unlinking in the Ring app (`app_integration_removed` +
  `device_removed` + `subscription_deactivated`).
- **Subscriptions** — `GET /v1/accounts/me/subscriptions` lists per-device
  plans/trials; `POST|DELETE /_sandbox/subscriptions` activate/deactivate
  them and fire the lifecycle webhooks that gate event delivery
  (`plan_id` + `expires_at` attributes).
- **WHEP live-video sessions** — `POST
  /v1/devices/{id}/media/streaming/whep/sessions` accepts an
  `application/sdp` offer (`?component_id=` supported) and returns 201 + an
  SDP answer + a `Location` session URL; `DELETE` closes it. Live views log
  an `on_demand` history entry like the real API.
- **Rate-limit / availability chaos** — new `Chaos` fields `rate_limit` and
  `unavailable` short-circuit `/v1/*` calls with 429/503 + `Retry-After`;
  new `limited` preset. The client's existing 429 retry path is exercised
  end-to-end (seeded rolls are deterministic).
- **Device lifecycle webhooks** — `POST /_sandbox/devices` now fires
  `device_added`; new `DELETE /_sandbox/devices/{id}` fires `device_removed`.
- **Client** — `app_integration()`, `link_app_integration()`,
  `update_app_integration()`, `subscriptions()`, `whep_session()`,
  `whep_close()`, and the `WhepSession` return type. New `AppIntegration`
  and `Subscription` models. `/_sandbox/state` reports the account plane.
- `webhooks.build_event()` accepts `source_type="accounts"` for
  account-scoped lifecycle events (no `devices` relationship).

### Fixed

- `__version__` was stale at `0.1.0`; now tracks the release (`0.4.0`) and
  the emulator reports it.

## 0.3.0 — 2026-09-23

### Added

- **Example scenarios ship inside the wheel** — `late_arrival`,
  `partial_blackout`, and `visitor_not_worker` resolve by name everywhere:
  `ring-sandbox play partial_blackout` works with a plain PyPI install, no
  repo checkout needed. `ring_sandbox.scenarios.resolve(name_or_path)`
  tries built-in → shipped example → YAML path; `examples()` returns the
  shipped set as name → YAML text.

## 0.2.0 — 2026-09-20

### Added

- **Chaos fault injection** — `ring-sandbox serve --chaos` injects duplicated,
  dropped, and delayed webhook deliveries plus flaky Event History / media
  responses. `GET|POST /_sandbox/chaos` inspects and adjusts the fault stream
  live. Every injected fault is recorded for assertions.
- **Scenario DSL** — load YAML visit scenarios (`ring-sandbox scenario FILE`,
  used by Attest's `attest replay path/to/scenario.yml`) with validated
  devices, event sequences, and clock control. Ships documented examples:
  `late_arrival.yml`, `partial_blackout.yml`, `visitor_not_worker.yml`.
- **OAuth refresh grant** — RFC 6749 `refresh_token` flow: on 401 the client
  refreshes once and retries, persisting rotated tokens. Exercised against the
  emulator; production endpoints unverified.
- **CI** — GitHub Actions matrix: Windows + Linux, Python 3.11 + 3.14.

### Fixed

- Media redirect handling hardened: JSON endpoints never follow redirects;
  media redirects only reach same-origin or allowlisted HTTPS hosts, never
  carry credentials, and response bodies are byte-capped.
- Event History mirrors live-observed behavior: Playground only surfaces
  `on_demand` events, and the emulator now matches.

## 0.1.0 — 2026-09-15

First public release on PyPI.

- Typed Python client for the Ring Partner API: users/me, devices, Event
  History, media download, webhook registration.
- Offline emulator (`ring-sandbox serve`) implementing the same surfaces with
  signed webhook delivery to the consumer's endpoint.
- pytest plugin with in-process `ring_client` / `ring_server` / `ring_control`
  fixtures.
- `ring-sandbox record` captures real Playground responses into sanitized JSON
  fixtures — several real recordings included under `fixtures/`.
