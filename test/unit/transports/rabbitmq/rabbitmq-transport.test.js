import { expect } from "chai";
import { RabbitMqTransport } from "../../../../src/transports/rabbitmq/rabbitmq-transport.js";
import {
  createFakeAmqpConnection,
  createRawMessage,
} from "../../../helpers/fake-rabbitmq.js";

function nextTick() {
  return new Promise((resolve) => setImmediate(resolve));
}

async function expectAsyncError(promise, expectedMessage) {
  let error;

  try {
    await promise;
  } catch (err) {
    error = err;
  }

  expect(error).to.be.instanceOf(Error);

  if (expectedMessage) {
    expect(error.message).to.equal(expectedMessage);
  }

  return error;
}

describe("RabbitMqTransport", function () {
  function createTransport(options = {}) {
    const fakeConnection = createFakeAmqpConnection(options.channelOptions);

    const transport = new RabbitMqTransport({
      namespace: options.namespace ?? "studio",
      service: options.service ?? "email-service",
      connection: fakeConnection,
      exchange: options.exchange,
      exchangeType: options.exchangeType,
      prefetch: options.prefetch,
      queue: options.queue,
    });

    return {
      transport,
      fakeConnection,
    };
  }

  const message = {
    meta: {
      id: "evt_123",
      kind: "event",
      type: "user.created",
      version: "1.0.0",
      streamId: "user_123",
      correlationId: "corr_123",
      timestamp: "2026-06-30T12:00:00.000Z",
      source: "test-suite",
    },
    data: {
      userId: "user_123",
      email: "janx@example.com",
    },
  };

  it("connects by creating publish and consume channels", async function () {
    const { transport, fakeConnection } = createTransport({
      prefetch: 5,
    });

    await transport.connect();

    expect(fakeConnection.confirmChannels).to.have.length(1);
    expect(fakeConnection.normalChannels).to.have.length(1);

    const publishChannel = fakeConnection.confirmChannels[0];
    const consumeChannel = fakeConnection.normalChannels[0];

    expect(publishChannel.assertExchangeCalls).to.deep.equal([
      ["conduit.studio.events", "topic", { durable: true }],
    ]);

    expect(consumeChannel.assertExchangeCalls).to.deep.equal([
      ["conduit.studio.events", "topic", { durable: true }],
    ]);

    expect(consumeChannel.prefetchCalls).to.deep.equal([5]);
  });

  it("does not create duplicate channels when connect is called twice", async function () {
    const { transport, fakeConnection } = createTransport();

    await transport.connect();
    await transport.connect();

    expect(fakeConnection.confirmChannels).to.have.length(1);
    expect(fakeConnection.normalChannels).to.have.length(1);
  });

  it("publishes to the namespace exchange using message.meta.type as routing key", async function () {
    const { transport, fakeConnection } = createTransport();

    await transport.publish(message);

    const publishChannel = fakeConnection.confirmChannels[0];

    expect(publishChannel.publishCalls).to.have.length(1);

    const [exchange, routingKey, body, properties] =
      publishChannel.publishCalls[0];

    expect(exchange).to.equal("conduit.studio.events");
    expect(routingKey).to.equal("user.created");
    expect(JSON.parse(body.toString("utf8"))).to.deep.equal(message);

    expect(properties.persistent).to.equal(true);
    expect(properties.contentType).to.equal("application/json");
    expect(properties.messageId).to.equal("evt_123");
    expect(properties.correlationId).to.equal("corr_123");

    expect(properties.headers).to.deep.include({
      kind: "event",
      type: "user.created",
      version: "1.0.0",
      source: "test-suite",
      namespace: "studio",
    });

    expect(publishChannel.waitForConfirmsCalls).to.equal(1);
  });

  it("publishes using an explicit routing key", async function () {
    const { transport, fakeConnection } = createTransport();

    await transport.publish(message, {
      routingKey: "custom.user.created",
      headers: {
        tenant: "studio",
      },
    });

    const publishChannel = fakeConnection.confirmChannels[0];
    const [, routingKey, , properties] = publishChannel.publishCalls[0];

    expect(routingKey).to.equal("custom.user.created");
    expect(properties.headers.tenant).to.equal("studio");
  });

  it("throws when publishing without a routing key or message.meta.type", async function () {
    const { transport } = createTransport();

    await expectAsyncError(
      transport.publish({
        meta: {},
        data: {},
      }),
      "RabbitMQ publish requires options.routingKey or message.meta.type",
    );
  });

  it("waits for drain when publish returns false", async function () {
    const { transport, fakeConnection } = createTransport({
      channelOptions: {
        publishAccepted: false,
      },
    });

    await transport.connect();

    const publishChannel = fakeConnection.confirmChannels[0];
    const publishPromise = transport.publish(message);

    await nextTick();

    expect(publishChannel.onceHandlers.drain).to.be.a("function");

    publishChannel.emit("drain");

    await publishPromise;

    expect(publishChannel.waitForConfirmsCalls).to.equal(1);
  });

  it("subscribes using the default service queue and creates a DLQ by default", async function () {
    const { transport, fakeConnection } = createTransport();

    await transport.subscribe("user.created", async () => {}, {
      consumerTag: "email-consumer",
    });

    const consumeChannel = fakeConnection.normalChannels[0];

    expect(consumeChannel.assertExchangeCalls).to.deep.equal([
      ["conduit.studio.events", "topic", { durable: true }],
      ["conduit.studio.events.dlx", "direct", { durable: true }],
    ]);

    expect(consumeChannel.assertQueueCalls).to.deep.equal([
      [
        "studio.email-service.dlq",
        {
          durable: true,
        },
      ],
      [
        "studio.email-service",
        {
          durable: true,
          exclusive: false,
          autoDelete: false,
          arguments: {
            "x-dead-letter-exchange": "conduit.studio.events.dlx",
            "x-dead-letter-routing-key": "studio.email-service.dead",
          },
        },
      ],
    ]);

    expect(consumeChannel.bindQueueCalls).to.deep.equal([
      [
        "studio.email-service.dlq",
        "conduit.studio.events.dlx",
        "studio.email-service.dead",
      ],
      ["studio.email-service", "conduit.studio.events", "user.created"],
    ]);

    expect(consumeChannel.consumeCalls).to.have.length(1);
    expect(consumeChannel.consumeCalls[0].queue).to.equal(
      "studio.email-service",
    );
    expect(consumeChannel.consumeCalls[0].options.consumerTag).to.equal(
      "email-consumer",
    );
  });

  it("can disable DLQ on a custom queue", async function () {
    const { transport, fakeConnection } = createTransport();

    await transport.subscribe("random.gossip", async () => {}, {
      queue: {
        name: "studio.email-service.low-priority",
        deadLetter: false,
      },
    });

    const consumeChannel = fakeConnection.normalChannels[0];

    expect(consumeChannel.assertExchangeCalls).to.deep.equal([
      ["conduit.studio.events", "topic", { durable: true }],
    ]);

    expect(consumeChannel.assertQueueCalls).to.deep.equal([
      [
        "studio.email-service.low-priority",
        {
          durable: true,
          exclusive: false,
          autoDelete: false,
          arguments: {},
        },
      ],
    ]);

    expect(consumeChannel.bindQueueCalls).to.deep.equal([
      [
        "studio.email-service.low-priority",
        "conduit.studio.events",
        "random.gossip",
      ],
    ]);
  });

  it("uses only one RabbitMQ consumer for multiple subscriptions on the same queue", async function () {
    const { transport, fakeConnection } = createTransport();

    await transport.subscribe("user.created", async () => {});
    await transport.subscribe("random.gossip", async () => {});

    const consumeChannel = fakeConnection.normalChannels[0];

    expect(consumeChannel.consumeCalls).to.have.length(1);
    expect(consumeChannel.consumeCalls[0].queue).to.equal(
      "studio.email-service",
    );
  });

  it("dispatches exact routing-key matches to the correct handler", async function () {
    const { transport, fakeConnection } = createTransport();

    const handled = [];

    await transport.subscribe("user.created", async (event) => {
      handled.push(["user.created", event]);
    });

    await transport.subscribe("random.gossip", async (event) => {
      handled.push(["random.gossip", event]);
    });

    const consumeChannel = fakeConnection.normalChannels[0];
    const consumeHandler = consumeChannel.consumeCalls[0].handler;

    const rawMessage = createRawMessage(JSON.stringify(message), {
      fields: {
        routingKey: "user.created",
      },
    });

    await consumeHandler(rawMessage);

    expect(handled).to.deep.equal([["user.created", message]]);
    expect(consumeChannel.ackCalls).to.deep.equal([rawMessage]);
    expect(consumeChannel.nackCalls).to.have.length(0);
  });

  it('dispatches "#" subscriptions for all events on the exchange', async function () {
    const { transport, fakeConnection } = createTransport();

    const handled = [];

    await transport.subscribe("#", async (event, ctx) => {
      handled.push([ctx.routingKey, event]);
    });

    const consumeChannel = fakeConnection.normalChannels[0];
    const consumeHandler = consumeChannel.consumeCalls[0].handler;

    const rawMessage = createRawMessage(JSON.stringify(message), {
      fields: {
        routingKey: "payment.received",
      },
    });

    await consumeHandler(rawMessage);

    expect(handled).to.deep.equal([["payment.received", message]]);
    expect(consumeChannel.ackCalls).to.deep.equal([rawMessage]);
  });

  it("throws for unsupported wildcard patterns", async function () {
    const { transport } = createTransport();

    await expectAsyncError(
      transport.subscribe("user.*", async () => {}),
      'RabbitMQ transport currently supports exact event types or "#" only',
    );
  });

  it("nacks invalid JSON without requeueing", async function () {
    const { transport, fakeConnection } = createTransport();

    await transport.subscribe("user.created", async () => {
      throw new Error("Should not be called");
    });

    const consumeChannel = fakeConnection.normalChannels[0];
    const consumeHandler = consumeChannel.consumeCalls[0].handler;

    const rawMessage = createRawMessage("{ invalid json");

    await consumeHandler(rawMessage);

    expect(consumeChannel.ackCalls).to.have.length(0);
    expect(consumeChannel.nackCalls).to.deep.equal([
      {
        rawMessage,
        allUpTo: false,
        requeue: false,
      },
    ]);
  });

  it("nacks when a handler throws", async function () {
    const { transport, fakeConnection } = createTransport();

    let capturedError;
    let capturedMessage;
    let capturedContext;

    await transport.subscribe(
      "user.created",
      async () => {
        throw new Error("Handler failed");
      },
      {
        onError: async (err, event, ctx) => {
          capturedError = err;
          capturedMessage = event;
          capturedContext = ctx;
        },
      },
    );

    const consumeChannel = fakeConnection.normalChannels[0];
    const consumeHandler = consumeChannel.consumeCalls[0].handler;
    const rawMessage = createRawMessage(JSON.stringify(message));

    await consumeHandler(rawMessage);

    expect(capturedError).to.be.instanceOf(Error);
    expect(capturedError.message).to.equal("Handler failed");
    expect(capturedMessage).to.deep.equal(message);
    expect(capturedContext.routingKey).to.equal("user.created");

    expect(consumeChannel.ackCalls).to.have.length(0);
    expect(consumeChannel.nackCalls).to.deep.equal([
      {
        rawMessage,
        allUpTo: false,
        requeue: false,
      },
    ]);
  });

  it("can requeue when a handler throws and requeueOnError is true", async function () {
    const { transport, fakeConnection } = createTransport();

    await transport.subscribe(
      "user.created",
      async () => {
        throw new Error("Handler failed");
      },
      {
        requeueOnError: true,
      },
    );

    const consumeChannel = fakeConnection.normalChannels[0];
    const consumeHandler = consumeChannel.consumeCalls[0].handler;
    const rawMessage = createRawMessage(JSON.stringify(message));

    await consumeHandler(rawMessage);

    expect(consumeChannel.nackCalls).to.deep.equal([
      {
        rawMessage,
        allUpTo: false,
        requeue: true,
      },
    ]);
  });

  it("acks messages that reach the queue but have no matching local subscription", async function () {
    const { transport, fakeConnection } = createTransport();

    await transport.subscribe("user.created", async () => {});

    const consumeChannel = fakeConnection.normalChannels[0];
    const consumeHandler = consumeChannel.consumeCalls[0].handler;

    const rawMessage = createRawMessage(JSON.stringify(message), {
      fields: {
        routingKey: "unhandled.event",
      },
    });

    await consumeHandler(rawMessage);

    expect(consumeChannel.ackCalls).to.deep.equal([rawMessage]);
    expect(consumeChannel.nackCalls).to.have.length(0);
  });

  it("unsubscribe removes a subscription without cancelling the queue consumer when other subscriptions remain", async function () {
    const { transport, fakeConnection } = createTransport();

    const handled = [];

    const subscription = await transport.subscribe("user.created", async () => {
      handled.push("user.created");
    });

    await transport.subscribe("random.gossip", async () => {
      handled.push("random.gossip");
    });

    await subscription.unsubscribe();

    const consumeChannel = fakeConnection.normalChannels[0];

    expect(consumeChannel.cancelCalls).to.have.length(0);

    const consumeHandler = consumeChannel.consumeCalls[0].handler;

    await consumeHandler(
      createRawMessage(JSON.stringify(message), {
        fields: {
          routingKey: "user.created",
        },
      }),
    );

    await consumeHandler(
      createRawMessage(JSON.stringify(message), {
        fields: {
          routingKey: "random.gossip",
        },
      }),
    );

    expect(handled).to.deep.equal(["random.gossip"]);
  });

  it("unsubscribe cancels the queue consumer when the last subscription is removed", async function () {
    const { transport, fakeConnection } = createTransport();

    const subscription = await transport.subscribe(
      "user.created",
      async () => {},
      {
        consumerTag: "email-consumer",
      },
    );

    const consumeChannel = fakeConnection.normalChannels[0];

    await subscription.unsubscribe();

    expect(consumeChannel.cancelCalls).to.deep.equal(["email-consumer"]);
  });

  it("throws when the same queue is declared with incompatible options", async function () {
    const { transport } = createTransport();

    await transport.subscribe("user.created", async () => {});

    await expectAsyncError(
      transport.subscribe("random.gossip", async () => {}, {
        queue: {
          name: "studio.email-service",
          deadLetter: false,
        },
      }),
      'RabbitMQ queue "studio.email-service" has already been declared with different options',
    );
  });

  it("disconnect cancels consumers and closes this transport's channels", async function () {
    const { transport, fakeConnection } = createTransport();

    await transport.subscribe("user.created", async () => {}, {
      consumerTag: "email-consumer",
    });

    const publishChannel = fakeConnection.confirmChannels[0];
    const consumeChannel = fakeConnection.normalChannels[0];

    await transport.disconnect();

    expect(consumeChannel.cancelCalls).to.deep.equal(["email-consumer"]);
    expect(publishChannel.closeCalls).to.equal(1);
    expect(consumeChannel.closeCalls).to.equal(1);

    // Because the connection was injected, transport.disconnect()
    // should not close the shared/injected broker connection.
    expect(fakeConnection.closeCalls).to.equal(0);
  });
});
