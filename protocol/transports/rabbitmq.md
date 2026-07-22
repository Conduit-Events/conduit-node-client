# Conduit RabbitMQ Transport Conventions

> This document is experimental and may change before the first stable release.

The [message-envelope protocol](../README.md) is transport-independent. This document defines the separate, transport-specific contract for the RabbitMQ transport: exchange and queue naming, bindings, dead-letter topology, how envelope fields map onto AMQP message properties, and acknowledgement behaviour.

A client implementation in any language that wants to interoperate with other Conduit clients over RabbitMQ must follow these conventions. Producing a schema-valid envelope is necessary but not sufficient for interoperability — a message published to the wrong exchange, under the wrong routing key, or without the expected AMQP properties will not reach another client's queue, or will not carry the metadata that client expects.

This document currently describes the conventions implemented by the [Conduit Node Client](../../README.md). It is derived from that implementation rather than an independent design; where the two disagree, treat that as a bug in one of them, not an ambiguity to resolve locally.

## Scope

This document covers:

- exchange naming, type, and durability;
- queue naming, durability, and other declaration options;
- binding and routing-key conventions;
- dead-letter exchange and queue topology;
- how envelope fields map onto AMQP message properties and headers;
- acknowledgement, negative-acknowledgement, and requeue behaviour.

It does not cover:

- the message envelope itself — see [`protocol/README.md`](../README.md);
- connection sharing within a single process — this is a process-local implementation detail, not part of the wire contract between clients;
- retry delay, attempt limits, backoff, or poison-message handling — not yet defined by any Conduit client. See "Known limitations" in the main README.

## Naming conventions

Given a client configured with a `namespace` and `service`, for example:

```text
namespace: studio
service:   email-service
```

### Event exchange

```text
conduit.<namespace>.events
```

Example: `conduit.studio.events`

- Type: `topic`
- Durable: `true`
- Shared by both events and commands published by any service in the namespace. There is no separate exchange for commands; `meta.kind` is the only thing distinguishing them.

### Service queue

```text
<namespace>.<service>
```

Example: `studio.email-service`

Defaults:

- Durable: `true`
- Exclusive: `false`
- Auto-delete: `false`

A caller may declare a queue under an explicit name instead of the default (for example, a shared read-model queue consumed by more than one logical subscription). Choosing an explicit name is a deliberate way to share a queue; avoiding accidental collisions with other services' queue names is the caller's responsibility.

A given queue name must be declared with consistent options for the lifetime of a transport instance — redeclaring the same queue name with different durability/exclusivity/arguments is a configuration error, not a merge.

### Dead-letter exchange and queue

Enabled by default, and derived from the service queue name and event exchange name:

```text
Dead-letter exchange: <event-exchange>.dlx     (type: direct, durable: true)
Dead-letter queue:    <queue-name>.dlq         (durable: true)
Dead-letter key:      <queue-name>.dead
```

Example, for the queue `studio.email-service` on exchange `conduit.studio.events`:

```text
conduit.studio.events.dlx
studio.email-service.dlq
studio.email-service.dead
```

The dead-letter exchange and queue are declared, and the dead-letter queue is bound to the dead-letter exchange using the dead-letter key, before the service queue itself is declared. The service queue is then declared with:

```text
x-dead-letter-exchange:    <dead-letter-exchange>
x-dead-letter-routing-key: <dead-letter-key>
```

Dead-lettering can be disabled per queue. A queue declared without dead-lettering has neither argument set, and RabbitMQ's default behaviour (drop on reject/expiry) applies instead.

## Routing keys and bindings

The routing key used to publish a message defaults to `meta.type` (for example, `user.created`). A caller may supply an explicit routing key instead; doing so decouples the message's routing from its declared type and should be used deliberately, since it can make the wire contract diverge from the envelope's own `type` field.

A subscription binds its queue to the event exchange using the subscribed message type as the binding pattern:

```text
bindQueue(queue, exchange, pattern)
```

Supported patterns:

- an exact message type, e.g. `user.created`;
- the catch-all pattern `#`.

RabbitMQ's topic wildcards `*` and an embedded `#` (e.g. `user.*`, `user.#`, `*.created`) are **not currently part of this contract**. A conforming client must reject them rather than accept them with whatever partial-matching behaviour its own language's tooling happens to provide — silently-different wildcard semantics between two client implementations would be worse than rejecting the pattern outright.

## Message representation on the wire

- Body: UTF-8 JSON, the complete Conduit envelope (`{ meta, data }`), unmodified.
- `contentType`: `application/json`
- `persistent`: `true` by default; a publisher may override this per message.
- `messageId`: copied from `meta.id`
- `correlationId`: copied from `meta.correlationId`
- `timestamp`: set to the time of publication as a transport-level AMQP property. This does not replace `meta.timestamp`, which remains authoritative for the envelope.
- `headers`:
  - `kind`: copied from `meta.kind`
  - `type`: copied from `meta.type`
  - `version`: copied from `meta.version`
  - `source`: copied from `meta.source`
  - `namespace`: the publishing client's configured namespace
  - additional caller-supplied headers are merged in, and take precedence over the built-in headers above if a key collides

These AMQP-level fields are duplicates for broker-side introspection (e.g. filtering in the RabbitMQ management UI) and are not used for routing — routing is governed entirely by the topic exchange and routing key described above. Per [`protocol/README.md`](../README.md), the JSON envelope remains authoritative; transport-specific values must never be written into `meta` itself.

## Consumption and acknowledgement

- Consumers use manual acknowledgement.
- A single queue may carry more than one logical subscription (e.g. separate handlers for different message types), multiplexed onto one AMQP consumer for that queue.

For each delivered message:

1. The body is parsed as JSON. Malformed JSON is negatively acknowledged without requeue.
2. Locally-registered subscriptions whose pattern matches the delivery's routing key are selected.
3. If none match, the message is acknowledged and discarded. The queue received the message because of its own bindings; the absence of a locally-matching subscription is not itself an error.
4. Matching handlers run sequentially, in registration order. The message is acknowledged only once every matching handler has resolved successfully.
5. If a handler throws, its subscription's `onError` callback (if any) is awaited, and the message is negatively acknowledged. It is requeued only if that subscription was configured with `requeueOnError: true`; otherwise RabbitMQ dead-letters or discards it according to the queue's configuration.

There is currently no delay, attempt counter, or backoff applied to a requeue. A message that fails every time it is delivered can therefore loop between delivery and requeue as fast as the broker and consumer allow.

## Compatibility

This document tracks the RabbitMQ transport as currently implemented. Until a stable release is declared:

- naming derivations, defaults, and dead-letter behaviour may change;
- additional transport-level headers may be introduced;
- wildcard-pattern support may be added, changing what a conforming client must accept.

A second-language client aiming for interoperability should implement against this document rather than reverse-engineering the Node source directly, but should treat the Node implementation as the tie-breaker if the two ever disagree, and report the discrepancy so one of them can be corrected.
