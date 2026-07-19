# Conduit Message Protocol

> This protocol is experimental and may change before the first stable release.

This directory defines the language-neutral Conduit message-envelope format.

It currently lives in the Conduit Node Client repository, which acts as the reference implementation. The protocol may later move into a dedicated repository so that Node, Python, and other clients can depend on the same versioned specification.

The protocol defines the JSON message exchanged between Conduit clients. It does not define RabbitMQ exchanges, queues, routing keys, acknowledgements, retries, or other transport-specific behaviour.

## Schema

The canonical message schema is located at:

```text
schemas/conduit-message.schema.json
```

The schema uses JSON Schema draft-07 and has the identifier:

```text
urn:conduit:schema:message:1.0.0
```

The schema identifier and version describe the Conduit message-envelope contract. They are independent of the version of any Node, Python, or other client package.

## Message structure

Every Conduit message contains two top-level properties:

```json
{
  "meta": {},
  "data": {}
}
```

- `meta` contains protocol metadata used to identify and trace the message.
- `data` contains the domain-specific message payload.

Additional top-level properties are not permitted.

## Example

```json
{
  "meta": {
    "id": "01JZM8FNDNGXARX13W5FQG9B3Z",
    "kind": "event",
    "type": "user.created",
    "version": "1.0.0",
    "streamId": "user_123",
    "correlationId": "01JZM8FNDNGXARX13W5FQG9B3Z",
    "timestamp": "2026-07-19T18:30:00.000Z",
    "source": "user-service"
  },
  "data": {
    "userId": "user_123",
    "email": "user@example.com"
  }
}
```

## Metadata fields

### `id`

Required non-empty string.

Uniquely identifies this specific message.

A client creating a new message generates its `id`. The normal publishing API does not permit application code to supply or override it.

Consumers must treat IDs as opaque strings. The protocol does not require UUIDs or any other particular identifier format.

### `kind`

Required string with one of these values:

```text
event
command
```

An `event` communicates something that has happened.

A `command` communicates a request or instruction.

Events and commands use the same envelope schema. The `kind` field expresses their semantic distinction.

The protocol does not currently define different routing, ownership, request/reply, or delivery guarantees for commands.

### `type`

Required non-empty string.

Identifies the domain-specific message type, for example:

```text
user.created
welcome-email.send
invoice.payment-failed
```

Dotted type names are a recommended convention but are not currently enforced by the schema.

### `version`

Required non-empty string.

Identifies the version of the Conduit message-envelope protocol used by the message.

This is not the version of the client package and does not identify the version of the domain payload.

The current default envelope version is:

```text
1.0.0
```

Payload-versioning rules may be added separately in a future protocol revision.

### `streamId`

Required non-empty string.

Identifies the logical domain entity, aggregate, workflow, or stream associated with the message.

Examples include:

```text
user_123
order_456
signup_789
```

Callers should provide a stable domain-relevant stream ID where possible. A client may generate one when it is not supplied.

Messages produced while handling another message normally inherit the parent message’s `streamId`, unless explicitly overridden.

### `correlationId`

Required non-empty string.

Identifies a related chain of messages belonging to the same operation or workflow.

For a root message, the default `correlationId` is the message’s own `id`:

```text
correlationId = id
```

Messages produced while handling another message normally inherit the parent message’s `correlationId`.

Requiring a correlation ID on every message means consumers do not need separate logic for uncorrelated root messages.

### `causationId`

Optional non-empty string.

Identifies the message that directly caused this message to be produced.

A root message normally omits `causationId`.

A child message normally uses the parent message’s `id`:

```text
child.causationId = parent.id
```

When no cause exists, the field should be omitted rather than set to `null` or an empty string.

### `timestamp`

Required string using the JSON Schema `date-time` format.

Represents the time at which the envelope was created.

Example:

```text
2026-07-19T18:30:00.000Z
```

Clients should generate timestamps in UTC using an ISO 8601 representation.

Transport timestamps, such as an AMQP timestamp property, do not replace this field.

### `source`

Required non-empty string.

Identifies the logical service or component that created the message.

Examples include:

```text
user-service
billing-service
notification-worker
```

The source is normally taken from the publishing client’s configuration.

### `extensions`

Optional object.

Provides an explicit location for additional transport-independent message metadata that is not part of the base Conduit envelope.

Example:

```json
{
  "extensions": {
    "com.example.audit": {
      "actorId": "admin_123"
    }
  }
}
```

Extension keys should be namespaced to reduce collisions between organisations and libraries.

The contents of `extensions` are not restricted by the base schema. Applications or extensions may define additional schemas for their own values.

Transport configuration must not be placed in `extensions`.

Examples of values that do not belong in `extensions` include:

- RabbitMQ routing keys;
- exchange or queue names;
- persistence settings;
- acknowledgement settings;
- requeue settings;
- consumer tags;
- delivery or redelivery state.

## Payload

The `data` property is a required JSON object.

Valid examples include:

```json
{}
```

```json
{
  "userId": "user_123"
}
```

```json
{
  "order": {
    "id": "order_456",
    "items": [
      {
        "sku": "sku_1",
        "quantity": 2
      }
    ]
  }
}
```

The base envelope schema does not define domain-specific payload fields.

Applications may register additional schemas keyed by message type, such as a schema specifically for `user.created`.

The base schema currently requires `data` to be an object. A top-level array, primitive value, or `null` is not a valid Conduit payload.

## Additional properties

Unknown properties are not allowed directly inside the message or its `meta` object.

For example, this is invalid:

```json
{
  "meta": {
    "id": "message_123",
    "kind": "event",
    "type": "user.created",
    "version": "1.0.0",
    "streamId": "user_123",
    "correlationId": "message_123",
    "timestamp": "2026-07-19T18:30:00.000Z",
    "source": "user-service",
    "routingKey": "identity.user.created"
  },
  "data": {}
}
```

The `routingKey` is a transport concern and is not part of the language-neutral envelope.

Additional message metadata must use the explicit `extensions` object.

## Parent and child messages

Given this parent message:

```json
{
  "meta": {
    "id": "message_1",
    "kind": "event",
    "type": "user.created",
    "version": "1.0.0",
    "streamId": "user_123",
    "correlationId": "message_1",
    "timestamp": "2026-07-19T18:30:00.000Z",
    "source": "user-service"
  },
  "data": {
    "userId": "user_123"
  }
}
```

A child message would normally contain:

```json
{
  "meta": {
    "id": "message_2",
    "kind": "command",
    "type": "welcome-email.send",
    "version": "1.0.0",
    "streamId": "user_123",
    "correlationId": "message_1",
    "causationId": "message_1",
    "timestamp": "2026-07-19T18:30:01.000Z",
    "source": "user-service"
  },
  "data": {
    "userId": "user_123"
  }
}
```

The child:

- receives a new `id`;
- inherits the parent’s `streamId`;
- inherits the parent’s `correlationId`;
- sets `causationId` to the parent’s `id`;
- receives a new timestamp;
- uses the source of the client that creates it.

These relationships cannot be fully enforced by JSON Schema. Client implementations must enforce them through message-creation behaviour and tests.

## Relationship to transports

The JSON envelope is transport-independent.

A transport may duplicate selected envelope values in native transport properties. For example, a RabbitMQ implementation may copy:

- `meta.id` into the AMQP `messageId` property;
- `meta.correlationId` into the AMQP `correlationId` property;
- `meta.timestamp` into an AMQP timestamp representation.

The values in the JSON envelope remain authoritative.

Transport-specific values must not be inserted directly into `meta`.

The following are outside the scope of this schema:

- exchanges;
- queues;
- bindings;
- routing keys;
- message persistence;
- acknowledgements;
- negative acknowledgements;
- retries;
- dead-lettering;
- consumer state;
- transport connection details.

Transport bindings and conventions should be documented separately from the message-envelope schema.

## Validation requirements

Conduit implementations should validate:

1. outgoing messages before publication;
2. incoming messages before application handlers run.

An implementation must reject envelopes that do not conform to the canonical schema.

Domain-specific payload validation is separate from base-envelope validation.

## Compatibility

The protocol is currently experimental.

Until a stable release is declared:

- fields may be added, removed, or changed;
- default behaviour may change;
- compatibility between protocol versions is not guaranteed.

Once the protocol is stabilised, breaking envelope changes should require a new protocol version and schema identifier.

Client package versions and protocol versions may advance independently.
