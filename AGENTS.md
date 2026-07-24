# Project Rules

This is a cross-language event bus SDK built around RabbitMQ.

Priorities:

- Keep the public API small and boring.
- Prefer explicit message envelopes: { meta, data }.
- Preserve correlationId, causationId, streamId, source, timestamp, kind, type, version.
- The canonical protocol spec (schema, transport docs, conformance fixtures) lives in [conduit-protocol](https://github.com/Conduit-Events/conduit-protocol). This repo's `protocol/` directory is a duplicated copy, not the source of truth — check conduit-protocol before treating anything under `protocol/` here as authoritative, and don't let the two silently drift.
- Do not introduce framework-specific DI requirements.
- Tests should be readable and focused.
- Use plain JavaScript unless TypeScript is already present.
- Do not hide RabbitMQ semantics when they matter: exchanges, queues, bindings, ack/nack, retry, DLQ.

Before editing:

- Explain the intended change.
- Prefer small diffs.
- Run relevant tests after changes.

Git workflow:

- `main` is branch-protected: direct pushes are rejected. Always branch off `origin/main`, push the branch, and open a PR (`gh pr create`).
- Use the `gh` CLI for push/PR/merge operations rather than the GitHub web UI.
- Keep unrelated changes on separate branches/PRs rather than bundling them.
- Squash-merge is this repo's convention (`gh pr merge --squash --delete-branch`).
