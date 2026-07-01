# Project Rules

This is a cross-language event bus SDK built around RabbitMQ.

Priorities:

- Keep the public API small and boring.
- Prefer explicit message envelopes: { meta, data }.
- Preserve correlationId, causationId, streamId, source, timestamp, kind, type, version.
- Do not introduce framework-specific DI requirements.
- Tests should be readable and focused.
- Use plain JavaScript unless TypeScript is already present.
- Do not hide RabbitMQ semantics when they matter: exchanges, queues, bindings, ack/nack, retry, DLQ.

Before editing:

- Explain the intended change.
- Prefer small diffs.
- Run relevant tests after changes.
