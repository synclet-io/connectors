# Proof: source-revenuecat as a manifest

`manifest.yaml` re-expresses [source-revenuecat](../../../source-revenuecat) for source-declarative. `proof_test.go`
runs it against the Go connector's own `testdata/` (route adapter in `proof_test.go`, clock
2026-10-18 12:00 UTC) and compares, per stream, the catalog entry and every RECORD and STATE (parsed
JSON, exact numbers, emission order, `sourceStats` included) with `expected/`.

Compared: all ten streams, full refresh (the Go connector has no incremental stream). Streams not expressed: none.

`expected/` and `schemas/` were produced from source-revenuecat 0.1.0 at commit 7ab98c8 by running the Go
connector on its fixtures. To refresh them after the Go connector or its fixtures change:

```bash
cd connectors/source-revenuecat
cp ../source-declarative/testdata/source-revenuecat/golden_test.go.txt golden_test.go
GOLDEN_OUT=$PWD/../source-declarative/testdata/source-revenuecat go test -run TestGolden -count=1 .
rm golden_test.go
```

The proof runs only when source-declarative's tests run (CI runs them for changed connectors only), so a
change to the Go connector's fixtures is caught when source-declarative next changes.

Behaviour outside records and state that the manifest does not reproduce:

- `project_id` is required: the Go connector resolves the only project the key can see when it is empty.
- No per-domain request pacing; the manifest waits on 429 (`Retry-After`, then `backoff_ms`, then 60s).
- A child 404 skips that child stream for that customer (`on_404: skip`); the Go connector skips the customer's remaining child streams.
- A child record whose `customer_id` names another customer passes through; the Go connector fails the read.
- 401 wording ("v1 key") and a `check` that probes every permission are the Go connector's own.
