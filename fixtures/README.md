Real responses recorded from api.amazonvision.com with a Playground token via `ring-sandbox record` on 2026-09-15. Device ids are the Playground's synthetic Doorbell Pro. `me.json` is not committed.

Replay them locally:

```bash
ring-sandbox serve --fixtures fixtures/
# or into a running emulator:
curl -X POST http://127.0.0.1:8787/_sandbox/load \
  -H 'content-type: application/json' -d '{"path": "fixtures/"}'
```

`tests/test_fixture_replay.py` asserts the replayed world reproduces these
responses verbatim (device resource, included resources, history entries).
