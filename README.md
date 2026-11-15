# PodSleuth

A **read-only Kubernetes Pod failure investigator**. Point it at a failing Pod and it detects the
failure (CrashLoopBackOff, OOMKilled, image pull, scheduling, eviction, …), collects forensic
evidence, runs deterministic RCA rules, and renders a Markdown or JSON report.

Read-only by design: no cluster mutations, no Secret values, and every hypothesis is linked to
the evidence that supports it — missing evidence is reported as an explicit gap, never a guess.

## Build

Requires Go 1.27+.

```bash
go build -o podsleuth ./cmd/podsleuth
```

## Quickstart

```bash
# Verify kubeconfig, context and API server connectivity
podsleuth doctor

# Investigate a failing Pod on demand
podsleuth investigate pod <pod-name> -n <namespace>
podsleuth investigate pod <pod-name> -n <namespace> --output json

# Re-render a persisted report
podsleuth report <incident-id>
```

Global flags: `--kubeconfig`, `--context`. Storage lives at
`~/.local/share/podsleuth/podsleuth.db` (user-only permissions).

Exit codes: `0` completed (root cause may remain unknown) · `1` internal error · `2` invalid
input/config · `3` report produced but collection was partial · `4` Kubernetes
auth/connectivity failure.

## Security

- Read-only: no write verbs, no Secret contents (references only).
- Credentials in logs, events, env values and annotations are redacted before persistence.
- Bounded work: 256 KiB logs per container, 2 MiB per evidence item, 200 events max.

## Development

```bash
go test ./...        # all tests run offline (fakes, temp SQLite, injected clocks)
go test -race ./...
go vet ./...
```

## Limitations

- `watch`, `incidents list/show` and `config validate` are specified but not wired yet.
- Node, workload and PVC collectors are pending, so the `NODE-001`/`DEPLOY-001` rules cannot fire.
- Retention/purge and in-cluster RBAC manifests are not implemented yet.

See [docs/PodSleuth_AGENT_SPEC.md](docs/PodSleuth_AGENT_SPEC.md) for the full implementation
contract.
