"""Seeded chaos under concurrent delivery, and knowing when the queue is empty.

`Chaos` promises that "``seed`` makes the fault stream deterministic so chaos
runs are reproducible in tests". `test_seeded_fault_streams_are_reproducible`
checks that two fresh `Chaos.rng()` objects yield the same sequence — which is
a property of `random.Random`, not of the emulator. Nothing covered a run.

It does not hold for a run, because deliveries are concurrent. `_fire` ends in
`asyncio.create_task(deliver(payload))`, and `deliver()` awaits
`asyncio.sleep(delay)` in the middle, between two draws from a single `rng`
shared by every delivery. Which coroutine draws next therefore depends on the
event loop's scheduling, so the same seed produces a different fault stream
from one run to the next as soon as more than one event is in flight.

Drawing each delivery's faults in one chunk closes that race, and the third
test closes the other half of it: the data plane draws from the same profile
(`rate_limit` and `unavailable` on every `/v1/*` request, `flaky_*` in the
handlers), so a run that served traffic while deliveries were in flight
consumed the stream in an order set by the event loop even with the draws
chunked. Measured on the `storm` rates: with no `/v1` traffic the same seed
gave identical counts 3 times out of 3, with twelve concurrent `/v1` calls
0 out of 3.

The second test is the practical consequence for a consumer. Nothing reports
in-flight deliveries: `create_task` results are not tracked, and the
`delivery` preset holds each delivery for up to `delay_ms + jitter_ms` (1.8 s).
A batch run cannot tell when the queue has drained, so it waits a fixed number
of seconds and hopes. On a loaded machine that guess is short, and the run
reports fewer deliveries than were made — a measurement that moves with the
load of the machine rather than with the code under test.

Found while running 20 doors at once against the `delivery` preset: the same
seed gave 8 drops on one run and 9 on the next, and a batch reported 58 of 60
receivers notified where a longer wait gave 60.
"""

import contextlib
import socket
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import uvicorn

from ring_sandbox.emulator import create_app
from ring_sandbox.world import Chaos, default_world

KEY = "chaos-hmac-key"
# A control-plane call does almost nothing, but it queues behind the deliveries
# and data-plane calls this module fires at once, and httpx's 5 s default is
# not slack on a two-core runner: a timed-out inject fails the test for the
# load of the machine rather than for the behaviour under test.
SLACK = 30.0


@contextlib.contextmanager
def _client(base: str):
    """One client for the whole run, not one per call.

    A bare `httpx.post(...)` builds a throwaway `Client` each time, and that
    builds a verification context from the CA bundle: ~300 ms per call here,
    which is 100x the work being measured. It also used to decide the result —
    six injects cost 6 s that way, so deliveries meant to be in flight had
    already landed before the first poll could see them.
    """
    with httpx.Client(base_url=base, timeout=SLACK) as client:
        yield client


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _Receiver(BaseHTTPRequestHandler):
    received: list[bytes] = []

    def do_POST(self):  # noqa: N802
        n = int(self.headers.get("Content-Length", 0))
        _Receiver.received.append(self.rfile.read(n))
        self.send_response(200)
        self.end_headers()

    def log_message(self, *_):
        pass


def _serve(app, port: int) -> tuple[uvicorn.Server, threading.Thread]:
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{port}"
    for _ in range(50):
        try:
            httpx.get(base + "/_sandbox/health", timeout=0.5)
            return server, thread
        except httpx.HTTPError:
            time.sleep(0.1)
    raise RuntimeError("emulator did not start")


def _stop(server: uvicorn.Server, thread: threading.Thread) -> None:
    """Stop the emulator and wait for the port to actually be released.

    Each run starts a server and used to leave it bound with its thread alive.
    Joining matters as much as stopping: `_free_port` picks a port by binding
    and closing one, so returning while the old socket is still in teardown
    lets the next run be handed a port the previous server still answers on,
    and its assertions then read the wrong emulator.
    """
    server.should_exit = True
    thread.join(timeout=10)


def _drained(api: httpx.Client, timeout_s: float = 30.0) -> None:
    """Wait for the queue itself, using the `pending` this PR adds, rather than
    sleeping for a guessed number of seconds — which is the habit the PR argues
    against, so the test should not keep it. It is also what makes these tests
    safe on a loaded CI runner: a fixed sleep either wastes the slack or is too
    short, and too short here means comparing two truncated runs."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if api.get("/_sandbox/chaos").json()["pending"] == 0:
            return
        time.sleep(0.05)
    raise AssertionError(f"deliveries still in flight after {timeout_s}s")


def _run(
    seed: int, events: int = 12, v1_calls: int = 0, serial: bool = False, **rates: float
) -> Counter:
    """One chaos run, and the fault stream it gave.

    `serial` drains each delivery before injecting the next, so no two ever
    overlap — the reference the concurrent run has to match. `v1_calls` adds
    that many concurrent data-plane requests, which draw from the profile too.
    """
    chaos = Chaos(seed=seed, drop=0.3, duplicate=0.3, delay_ms=10, jitter_ms=60, **rates)
    app = create_app(default_world(), chaos)
    api_port, hook_port = _free_port(), _free_port()
    server, thread = _serve(app, api_port)
    base = f"http://127.0.0.1:{api_port}"
    _Receiver.received = []
    hooks = ThreadingHTTPServer(("127.0.0.1", hook_port), _Receiver)
    threading.Thread(target=hooks.serve_forever, daemon=True).start()

    try:
        with _client(base) as api:
            api.post(
                "/_sandbox/webhooks",
                json={"url": f"http://127.0.0.1:{hook_port}/hook", "signing_key": KEY},
            )

            def _inject() -> None:
                api.post("/_sandbox/events", json={"type": "motion_detected"})

            def _read() -> None:
                # A throttled call answers 429/503, which is a fault drawn from
                # the profile and the whole point of sending it; not an error.
                with contextlib.suppress(httpx.HTTPError):
                    api.get("/v1/devices")

            if serial:
                for _ in range(events):
                    _inject()
                    _drained(api)
                for _ in range(v1_calls):
                    _read()
            else:
                # At once, which is the point: one at a time never interleaves.
                with ThreadPoolExecutor(max_workers=events + v1_calls) as pool:
                    jobs = [pool.submit(_inject) for _ in range(events)]
                    jobs += [pool.submit(_read) for _ in range(v1_calls)]
                    for job in jobs:
                        job.result()
            _drained(api)
            actions = api.get("/_sandbox/chaos").json()["actions"]
    finally:
        hooks.shutdown()
        _stop(server, thread)

    # Counts, not identities. `meta.request_id` is a fresh uuid4, so a delivery
    # has no name that survives a restart and "which one was dropped" cannot be
    # compared across runs — an earlier version of this test tried, and could
    # never have passed. How MANY were dropped is the part a measurement rests
    # on, and it is the part the seed can honestly promise.
    return Counter(a["kind"] for a in actions)


# The `storm` preset's rates, so that every consumer of the profile is drawing:
# delivery in its task, `_chaos_throttle` on each `/v1/*` request.
STORM = {"flaky_media": 0.2, "flaky_history": 0.2, "rate_limit": 0.15, "unavailable": 0.05}


def test_a_seeded_run_is_reproducible_under_concurrent_delivery():
    """The promise in `Chaos`'s own docstring, applied to a run rather than to
    `random.Random`.

    Against the serial run, not against a second concurrent one. Two concurrent
    runs are only *likely* to disagree when the bug is present — they disagree
    when the machine's timing happens to vary between them, so the test passes
    with the bug in place whenever the runs are fast and uniform, which is
    exactly what a green CI looks like. Measured: this assertion as
    `_run(7) == _run(7)` stopped failing the moment the suite got faster.
    The serial run is the fixed point the concurrent one owes an answer to, and
    needs no luck to disagree: overlapping deliveries must consume the stream
    in the same per-delivery chunks that non-overlapping ones do.
    """
    assert _run(seed=7, serial=True) == _run(seed=7)


def test_the_seed_still_decides_something():
    """The guard on the guard: a fix that made every seed behave alike would
    pass the test above and quietly destroy the point of seeding. Several
    seeds, because two of them landing on the same counts is a coincidence and
    not a failure."""
    comptes = [_run(seed=s) for s in (7, 23, 99)]

    assert len({tuple(sorted(c.items())) for c in comptes}) > 1


def test_the_queue_says_when_it_is_empty():
    """So a batch run can wait for the thing itself instead of sleeping for a
    guessed number of seconds.

    `delay_ms` is a flat 2 s with no jitter so that "still in flight" is a fact
    for the whole check rather than a race: with 200 ms + up to 400 ms of
    jitter the six deliveries could all have landed before the first poll —
    which is the same guess-the-timing habit this test exists to remove, and it
    failed that way on a loaded machine.
    """
    app = create_app(default_world(), Chaos(seed=3, delay_ms=2000, jitter_ms=0))
    api_port, hook_port = _free_port(), _free_port()
    server, thread = _serve(app, api_port)
    base = f"http://127.0.0.1:{api_port}"
    _Receiver.received = []
    hooks = ThreadingHTTPServer(("127.0.0.1", hook_port), _Receiver)
    threading.Thread(target=hooks.serve_forever, daemon=True).start()

    try:
        with _client(base) as api:
            api.post(
                "/_sandbox/webhooks",
                json={"url": f"http://127.0.0.1:{hook_port}/hook", "signing_key": KEY},
            )
            for _ in range(6):
                api.post("/_sandbox/events", json={"type": "motion_detected"})

            # One task per event, every one of them still held by its 2 s delay.
            assert api.get("/_sandbox/chaos").json()["pending"] == 6

            _drained(api)
            assert len(_Receiver.received) == 6, "nothing was still in flight when it said zero"
    finally:
        hooks.shutdown()
        _stop(server, thread)


def test_the_seed_survives_traffic_on_the_data_plane():
    """The delivery counts a seeded run reports must not depend on how busy the
    data plane happened to be.

    `_chaos_throttle` draws on every `/v1/*` request. While that shared one
    stream with delivery, those draws shifted the delivery stream, so a run
    that served traffic reported different faults from a quiet run on the same
    seed — the fix the test above makes is undone from the other side. Busy
    against quiet, so the comparison fails on the shift itself rather than on
    two busy runs happening to be scheduled differently.
    """
    assert _run(seed=11, v1_calls=12, **STORM) == _run(seed=11, v1_calls=0, **STORM)
