# Proof: source-openexchangerates as a manifest

`manifest.yaml` re-expresses [source-openexchangerates](../../../source-openexchangerates) for source-declarative. `proof_test.go`
runs it against the Go connector's own `testdata/` (route adapter in `proof_test.go`, clock
2026-10-18 12:00 UTC) and compares, per stream, the catalog entry and every RECORD and STATE (parsed
JSON, exact numbers, emission order, `sourceStats` included) with `expected/`.

Compared: currencies, latest_rates, historical_rates (full refresh; historical_rates also incremental from an empty state and from `{"date":"2026-10-15"}`). Streams not expressed: none.

`expected/` and `schemas/` were produced from source-openexchangerates 0.2.1 at commit 7ab98c8 by running the Go
connector on its fixtures. To refresh them after the Go connector or its fixtures change:

```bash
cd connectors/source-openexchangerates
cp ../source-declarative/testdata/source-openexchangerates/golden_test.go.txt golden_test.go
GOLDEN_OUT=$PWD/../source-declarative/testdata/source-openexchangerates go test -run TestGolden -count=1 .
rm golden_test.go
```

The proof runs only when source-declarative's tests run (CI runs them for changed connectors only), so a
change to the Go connector's fixtures is caught when source-declarative next changes.

Behaviour outside records and state that the manifest does not reproduce:

- The Go connector treats a 429 as "the plan lacks the route" (a config error); the manifest waits on it per `retry` and fails after five waits with the API's message. No fixture exercises it.
