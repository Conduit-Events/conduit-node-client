# Conduit Node Client

> **Experimental:** this client is under active development and is not yet production-ready. APIs, defaults, routing rules, and lifecycle behaviour may change before the first stable release.

The Conduit Node Client is the first SDK for **Conduit Events**, a cross-language event-driven architecture toolkit built around a shared message protocol.

The current implementation provides a small Node.js API for:

- publishing events and commands;
- consuming RabbitMQ messages through service-owned queues;
- validating message envelopes and typed payloads with JSON Schema;
- preserving stream, correlation, and causation metadata across message chains;
- sharing RabbitMQ connections between clients in the same Node.js process;
- acknowledging or rejecting messages according to handler outcomes.

RabbitMQ is the first supported transport. The next planned milestone is a minimal Python client and end-to-end cross-language protocol testing. Broader production hardening will follow after the interoperability contract has been exercised by more than one implementation.

## Current status

The client currently supports the core publish-and-subscribe workflow and has both unit tests and RabbitMQ integration tests.

It should presently be treated as:

- a working architectural prototype;
- a reference implementation of the Conduit message protocol;
- a foundation for further development;
- a project for evaluation, testing, and contribution.

It should **not** presently be treated as a production messaging framework.

There is no stable release contract yet, and the package version should not be interpreted as a guarantee of API stability.

## What currently works

- Event publishing with `client.emit()`
- Command publishing with `client.command()`
- Pre-start handler registration with `client.on()`
- Post-start subscription with `client.subscribe()`
- Runtime unsubscribe handles
- RabbitMQ topic exchange publishing
- Durable service queues by default
- Publisher confirms
- Manual consumer acknowledgements
- Default dead-letter exchange and queue creation
- JSON message-envelope validation
- Optional JSON Schema validation by message type
- Multiple handlers for the same event type
- Child event and command publishing from handler context
- Automatic propagation of `streamId`, `correlationId`, and `causationId`
- Process-local RabbitMQ connection sharing
- Dependency injection for transport, envelope, and validator testing
- Unit and RabbitMQ integration test suites

## Requirements

- A current Node.js runtime with ES module support
- RabbitMQ
- npm

## Installation

The package is not yet presented as a stable npm release. For development, clone the repository and install its dependencies:

```bash
git clone https://github.com/Conduit-Events/conduit-node-client.git
cd conduit-node-client
npm install
```

When installed as a local or Git dependency, the current root export is:

```js
import { Client } from "conduit-node-client";
```

Only `Client` should currently be treated as part of the public package API. Internal transport, schema, envelope, and connection classes may still change or be reorganised.

## Quick start

The example below registers the consumer before startup, starts both services, and publishes an event.

```js
import { Client } from "conduit-node-client";

const userService = Client.create({
  namespace: "studio",
  service: "user-service",
  rabbitmq: {
    url: process.env.RABBITMQ_URL ?? "amqp://localhost",
  },
});

const emailService = Client.create({
  namespace: "studio",
  service: "email-service",
  rabbitmq: {
    url: process.env.RABBITMQ_URL ?? "amqp://localhost",
  },
});

emailService.on("user.created", async (message, ctx) => {
  console.log("New user:", message.data);
  console.log("Routing key:", ctx.routingKey);
});

await Promise.all([userService.start(), emailService.start()]);

await userService.emit(
  "user.created",
  {
    userId: "user_123",
    email: "user@example.com",
  },
  {
    streamId: "user_123",
  },
);

await Promise.all([userService.stop(), emailService.stop()]);
```

`start()` waits for all handlers registered with `on()` to be activated before it resolves.

## Client configuration

```js
const client = Client.create({
  namespace: "studio",
  service: "email-service",
  source: "email-service",

  schemas: {
    // Optional payload schemas keyed by message type.
  },

  rabbitmq: {
    url: "amqp://localhost",
    connectionName: "main",
    exchange: "conduit.studio.events",
    exchangeType: "topic",
    prefetch: 10,
    queue: {
      durable: true,
      exclusive: false,
      autoDelete: false,
      deadLetter: true,
    },
  },
});
```

### Top-level options

| Option           | Default                      | Description                                                                               |
| ---------------- | ---------------------------- | ----------------------------------------------------------------------------------------- |
| `namespace`      | `"default"`                  | Application or system boundary used when deriving RabbitMQ names.                         |
| `service`        | none                         | Required logical service name used for client identity and default RabbitMQ queue naming. |
| `source`         | `service`                    | Source written to outgoing message metadata.                                              |
| `defaultVersion` | `"1.0.0"`                    | Default envelope version.                                                                 |
| `schemas`        | `{}`                         | Payload schemas or schema aliases keyed by message type.                                  |
| `rabbitmq`       | `{}`                         | RabbitMQ transport configuration.                                                         |
| `transport`      | generated RabbitMQ transport | Advanced dependency-injection override.                                                   |
| `envelopes`      | generated envelope factory   | Advanced dependency-injection override.                                                   |
| `validator`      | generated schema validator   | Advanced dependency-injection override.                                                   |

### RabbitMQ options

| Option             | Default                      | Description                                       |
| ------------------ | ---------------------------- | ------------------------------------------------- |
| `url`              | `"amqp://localhost"`         | RabbitMQ connection URL.                          |
| `connectionName`   | `"main"`                     | Process-local connection-registry key.            |
| `exchange`         | `conduit.<namespace>.events` | Exchange used for both events and commands.       |
| `exchangeType`     | `"topic"`                    | RabbitMQ exchange type.                           |
| `prefetch`         | `10`                         | Consumer prefetch applied to the consume channel. |
| `queue.name`       | `<namespace>.<service>`      | Default service queue name.                       |
| `queue.durable`    | `true`                       | Whether the queue survives broker restarts.       |
| `queue.exclusive`  | `false`                      | Whether the queue is exclusive to its connection. |
| `queue.autoDelete` | `false`                      | Whether RabbitMQ automatically deletes the queue. |
| `queue.arguments`  | `{}`                         | Additional RabbitMQ queue arguments.              |
| `queue.deadLetter` | `true`                       | Enables default dead-letter infrastructure.       |

## Lifecycle

### Register before startup

Use `on()` when handlers are known during service initialisation:

```js
const subscription = client.on("user.created", async (message, ctx) => {
  console.log(message.data);
});

await client.start();
```

`on()` returns immediately with a subscription handle:

```js
{
  id,
  type,
  ready,
  unsubscribe,
}
```

For a pre-start registration, `start()` activates the subscription. The handle's `ready` promise reflects its current activation promise.

### Subscribe after startup

Use `subscribe()` when the client is already running and the caller needs to wait until the RabbitMQ subscription is active:

```js
await client.start();

const subscription = await client.subscribe("user.created", async (message) => {
  console.log(message.data);
});
```

Calling `subscribe()` before `start()` rejects. Use `on()` for deterministic startup registration.

### Stop and restart

```js
await client.stop();
await client.start();
```

Logical subscriptions registered with `on()` or `subscribe()` are retained across a normal stop and restart unless explicitly unsubscribed.

## Publishing events

```js
await client.emit(
  "user.created",
  {
    userId: "user_123",
    email: "user@example.com",
  },
  {
    streamId: "user_123",
    correlationId: "signup_456",
  },
);
```

## Publishing commands

```js
await client.command(
  "welcome-email.send",
  {
    userId: "user_123",
    email: "user@example.com",
  },
  {
    streamId: "user_123",
    correlationId: "signup_456",
  },
);
```

Events and commands currently use the same RabbitMQ exchange and routing model. Their semantic difference is represented by `message.meta.kind`.

## Message envelope

The canonical language-neutral envelope specification and JSON Schema are documented in the [`conduit-protocol`](https://github.com/Conduit-Events/conduit-protocol) repository, which this client depends on directly.
Every published message has the following shape:

```json
{
  "meta": {
    "id": "91afd38e-8e46-4822-919f-af37d0813ef5",
    "kind": "event",
    "type": "user.created",
    "version": "1.0.0",
    "streamId": "user_123",
    "correlationId": "signup_456",
    "timestamp": "2026-07-07T12:00:00.000Z",
    "source": "user-service"
  },
  "data": {
    "userId": "user_123",
    "email": "user@example.com"
  }
}
```

### Metadata behaviour

- `id` is generated for every new message.
- `version` defaults to `1.0.0`.
- `source` defaults to the client service.
- `streamId` is generated when not supplied.
- `correlationId` defaults to the new message's `id` when not supplied.
- `causationId` is optional and normally appears on a message produced while handling another message.
- `timestamp` is generated as an ISO 8601 date-time string.

For meaningful domain traces, callers should normally supply a stable `streamId` rather than relying on an automatically generated value.

## Typed payload validation

Envelope validation is always enabled. Payload validation is enabled for message types that have a registered JSON Schema.

```js
const schemas = {
  "user.created": {
    type: "object",
    required: ["userId", "email"],
    additionalProperties: false,
    properties: {
      userId: {
        type: "string",
        minLength: 1,
      },
      email: {
        type: "string",
        format: "email",
      },
    },
  },
};

const client = Client.create({
  namespace: "studio",
  service: "user-service",
  schemas,
  rabbitmq: {
    url: process.env.RABBITMQ_URL ?? "amqp://localhost",
  },
});
```

Outgoing messages are validated before publication. Incoming messages are validated before their application handler runs.

A client that needs typed validation for an incoming event must register the corresponding schema itself. Schemas are not currently distributed through RabbitMQ or a central registry.

### Schema aliases

A schema entry may alias another registered schema:

```js
const schemas = {
  "user.record": {
    type: "object",
    required: ["userId"],
    properties: {
      userId: {
        type: "string",
      },
    },
  },

  "user.created": "user.record",
  "user.updated": "user.record",
};
```

Alias targets must exist.

## Handler context

Handlers receive the validated message and a context object:

```js
client.on("user.created", async (message, ctx) => {
  console.log(ctx.routingKey);
  console.log(ctx.exchange);
  console.log(ctx.redelivered);
  console.log(ctx.properties);
  console.log(ctx.client);
});
```

The context also provides `emit()` and `command()` helpers.

### Publishing a child event

```js
client.on("user.created", async (message, ctx) => {
  await ctx.emit("welcome-email.requested", {
    userId: message.data.userId,
    email: message.data.email,
  });
});
```

The child message inherits:

- `streamId` from the parent;
- `correlationId` from the parent, or the parent message ID when no correlation ID exists;
- `causationId` from the parent message ID.

Explicit `streamId` and `correlationId` child options override the inherited values:

```js
await ctx.emit(
  "welcome-email.requested",
  {
    userId: message.data.userId,
  },
  {
    streamId: "custom-stream",
    correlationId: "custom-correlation",
  },
);
```

`causationId` cannot be overridden this way. It always identifies the message that directly caused the child to be produced, so it is set to the parent message's ID regardless of any `causationId` passed in options.

The same inheritance rules apply to `ctx.command()`.

## Subscription options

```js
const subscription = client.on(
  "user.created",
  async (message) => {
    await updateProjection(message);
  },
  {
    queue: {
      name: "studio.user-projection",
      durable: true,
      deadLetter: true,
    },

    requeueOnError: false,

    onError: async (error, message, ctx) => {
      console.error("Handler failed", {
        error,
        messageId: message.meta.id,
        routingKey: ctx.routingKey,
      });
    },
  },
);
```

## Routing

The current RabbitMQ transport supports:

- exact routing keys, such as `user.created`;
- the catch-all pattern `#`.

```js
client.on("user.created", handler);
client.on("#", auditHandler);
```

Partial topic wildcards are not currently supported by the client:

```text
user.*
user.#
*.created
```

Although RabbitMQ supports those patterns, the current transport deliberately rejects them until local matching and subscription behaviour are expanded.

A custom routing key can currently be supplied when publishing:

```js
await client.emit(
  "user.created",
  {
    userId: "user_123",
  },
  {
    routingKey: "identity.user.created",
  },
);
```

Overriding the routing key independently of the message type should be used cautiously because it can make contracts and routing behaviour diverge.

## RabbitMQ topology

For this configuration:

```js
Client.create({
  namespace: "studio",
  service: "email-service",
});
```

the defaults are:

```text
Exchange: conduit.studio.events
Queue:    studio.email-service
```

With dead-lettering enabled, the default dead-letter topology is derived from those names:

```text
Dead-letter exchange: conduit.studio.events.dlx
Dead-letter queue:    studio.email-service.dlq
Dead-letter key:      studio.email-service.dead
```

Queues are durable by default. The event exchange and dead-letter exchange are also durable.

## Acknowledgement and failure behaviour

The transport uses manual acknowledgements.

For each delivered RabbitMQ message:

1. The JSON body is parsed.
2. Matching local subscriptions are selected.
3. Each matching handler is awaited.
4. The RabbitMQ message is acknowledged only after every matching handler succeeds.

When a handler throws:

1. Its optional `onError` callback is awaited.
2. The RabbitMQ message is negatively acknowledged.
3. The message is requeued only when `requeueOnError` is `true`.
4. Otherwise, RabbitMQ dead-letters or discards the message according to queue configuration.

Malformed JSON is negatively acknowledged without requeueing.

No built-in delay, retry count, exponential backoff, or poison-message policy is implemented yet.

## Multiple handlers in one service

A service may register multiple handlers for the same event:

```js
client.on("user.created", updateReadModel);
client.on("user.created", sendInternalNotification);
```

The current transport uses one RabbitMQ delivery for the service queue and executes all matching local handlers **sequentially in registration order**.

This means:

- the second handler waits for the first;
- the message is acknowledged only after both succeed;
- if the first handler fails, the second handler is not run;
- retries and dead-lettering apply to the RabbitMQ message as a whole, not to individual handlers.

This is a valid execution model, but it is not yet a final architectural commitment. Future versions may introduce configurable sequential, parallel, or independently isolated handler execution.

## Shared RabbitMQ connections

Clients in the same Node.js process can share a RabbitMQ TCP connection by using the same `connectionName` and URL:

```js
const serviceA = Client.create({
  namespace: "studio",
  service: "service-a",
  rabbitmq: {
    url: "amqp://localhost",
    connectionName: "studio-main",
  },
});

const serviceB = Client.create({
  namespace: "studio",
  service: "service-b",
  rabbitmq: {
    url: "amqp://localhost",
    connectionName: "studio-main",
  },
});
```

Each transport creates its own publish and consume channels, while the underlying connection is reference-counted.

Stopping one client releases its lease. The connection is closed when the final client using that registry entry disconnects.

Connection sharing is process-local. It does not coordinate connections across Node.js processes, containers, or hosts.

Reusing a connection name with a different RabbitMQ URL is rejected.

## Unsubscribing

```js
const subscription = client.on("user.created", handler);

await client.start();
await subscription.unsubscribe();
```

Unsubscribing removes the logical handler and, when no handlers remain for that queue, cancels the local consumer.

See the limitations section for the current RabbitMQ binding-cleanup behaviour.

## Testing

Run the unit suite:

```bash
npm test
```

or explicitly:

```bash
npm run test:unit
```

Run linting:

```bash
npm run lint
```

Run the RabbitMQ integration suite:

```bash
RABBITMQ_URL=amqp://localhost npm run test:integration
```

This needs a running RabbitMQ broker. If you don't already have one, start one with Docker:

```bash
docker run -d --name conduit-rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

`amqp://localhost` is the default `RABBITMQ_URL` for `test:integration`, so no further configuration is needed once the container is running. The management UI, if you want it, is at `http://localhost:15672` (default login `guest`/`guest`).

The integration suite currently covers:

- communication between multiple clients;
- typed payload validation;
- multiple handlers within one service;
- fan-out to multiple service queues;
- child event metadata propagation;
- shared connection lifecycle behaviour.

CI runs this suite against a RabbitMQ service container on every PR (see `.github/workflows/ci.yml`).

## Known limitations

### 1. Experimental API and packaging

The client has no stable release contract. The root package currently exports only `Client`, while several internal classes are imported directly by tests and implementation code.

Public exports, package versioning, release automation, and compatibility guarantees still need to be finalised.

### 2. Sequential local handler execution

Multiple handlers matching the same delivered message run sequentially.

A slow handler delays every later handler. A failing handler prevents later handlers from running and causes the entire RabbitMQ delivery to be rejected.

There is no current option for parallel execution, per-handler acknowledgement, failure isolation, or separate handler queues.

### 3. Unsubscribe does not remove RabbitMQ bindings

The current unsubscribe path removes local subscriptions and may cancel the consumer, but it does not call `unbindQueue()`.

Consequences can include:

- a durable queue remaining bound after the handler is removed;
- messages for a removed pattern continuing to reach the queue;
- unmatched messages being acknowledged and discarded while another consumer remains active;
- messages accumulating when the durable queue has no consumer.

Binding ownership and cleanup need to be defined before unsubscribe can be considered complete.

### 4. Connection recovery is incomplete

The RabbitMQ connection and channels track close events, but the transport does not currently rebuild its complete topology after an unexpected connection or channel loss.

It does not automatically restore:

- queue declarations;
- dead-letter declarations;
- queue bindings;
- active consumers;
- subscription-to-consumer state.

Applications should not yet rely on transparent recovery from broker restarts or network interruptions.

### 5. No strict message ordering guarantee

Handlers for one delivered message are sequential, but separate RabbitMQ deliveries may be processed concurrently up to the configured prefetch.

The client does not currently provide:

- per-stream serialisation;
- partition ownership;
- ordered replay;
- sequence numbers;
- gap detection.

Consumers that require ordering must enforce it at the application or topology level.

### 6. Limited topic-pattern support

Only exact routing keys and `#` are supported. RabbitMQ-style partial wildcard patterns using `*` or embedded `#` are rejected.

### 7. Retry behaviour is minimal

`requeueOnError: true` performs a RabbitMQ requeue without delay or attempt tracking. A repeatedly failing message can therefore enter a rapid redelivery loop.

The current implementation has no built-in:

- delayed retries;
- retry limits;
- backoff;
- retry headers;
- poison-message detection;
- dead-letter replay tooling.

### 8. Schemas are local to each client

There is no schema registry, schema distribution, compatibility checking, or contract publication workflow.

Payloads for message types without a registered schema pass payload validation as long as the base envelope is valid.

### 9. Publishing options are limited

Envelope metadata and transport-only publishing options are explicitly separated. A custom RabbitMQ `routingKey` is currently supported. Other transport-specific options, such as AMQP headers and persistence overrides, are not yet part of the stable client API.

### 10. Commands have no specialised delivery semantics

Commands currently use the same exchange, routing, subscription, acknowledgement, and queue behaviour as events.

The client does not yet provide:

- command ownership enforcement;
- single-handler guarantees;
- request/reply;
- RPC timeouts;
- response envelopes;
- command result correlation.

### 11. No idempotency or deduplication

The envelope contains a unique message ID, but the client does not store processed IDs or prevent duplicate processing.

Consumers must currently implement idempotency themselves.

### 12. No replay or event store

Conduit currently routes messages through RabbitMQ queues. It is not an event store and does not provide historical replay, stream persistence, snapshots, or event-sourcing projections.

### 13. Limited observability

Connection and channel errors are currently written directly to `console.error`.

There is no stable logging abstraction, metrics interface, OpenTelemetry integration, health check, or tracing exporter.

### 14. RabbitMQ is the only implemented transport

The transport contract exists, but no alternative transport is currently implemented.

Cross-language compatibility is a design goal, not a completed feature yet. A [Python client](https://github.com/Conduit-Events/conduit-python-client) exists as an early scaffold but has no implementation yet; Elixir and other clients don't exist at all.

## Likely next steps

The most important work before a stable release is:

1. Remove queue bindings during explicit unsubscribe.
2. Restore topology and subscriptions after connection loss.
3. Decide whether local handlers are sequential, parallel, or configurable.
4. Add a deliberate retry and dead-letter strategy.
5. Expand topic-pattern support.
6. Finalise public exports and package versioning.
7. Build end-to-end cross-language protocol tests (under way — see `conduit-python-client`).

## Project structure

```text
src/
├── client/
│   └── client.js
├── envelope/
│   └── envelope-factory.js
├── schema/
│   └── schema-validator.js
└── transports/
    ├── transport.js
    └── rabbitmq/
        ├── rabbitmq-connection.js
        ├── rabbitmq-connection-registry.js
        ├── rabbitmq-transport-config.js
        └── rabbitmq-transport.js

test/
├── helpers/
├── unit/
└── integration/
```

The message-envelope protocol itself — schema, transport-binding docs, conformance fixtures — is not part of this repo. It's consumed as a dependency from [`conduit-protocol`](https://github.com/Conduit-Events/conduit-protocol).

## Design principles

Conduit aims to remain:

- explicit rather than magical;
- language-neutral at the protocol level;
- small enough to understand;
- strict enough to reduce integration drift;
- transport-aware without permanently coupling the public API to RabbitMQ;
- usable without requiring a particular dependency-injection framework or application architecture.

## Licence

This project is licensed under the [MIT License](./LICENSE).
