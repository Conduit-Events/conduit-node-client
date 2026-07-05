import { expect } from "chai";
import amqp from "amqplib";

import { RabbitMqTransport } from "../../../../src/transports/rabbitMQ/rabbitmq-transport.js";
import { RabbitMqConnectionRegistry } from "../../../../src/transports/rabbitMQ/rabbitmq-connection-registry.js";

const RUN_INTEGRATION_TESTS = process.env.RUN_RABBITMQ_INTEGRATION === "true";

const RABBITMQ_URL = process.env.RABBITMQ_URL ?? "amqp://localhost";

function createTestId() {
  return `test_${Date.now()}_${Math.random().toString(16).slice(2)}`;
}

function createMessage(type = "user.created") {
  return {
    meta: {
      id: `evt_${Date.now()}`,
      kind: "event",
      type,
      version: "1.0.0",
      streamId: "user_123",
      correlationId: `corr_${Date.now()}`,
      timestamp: new Date().toISOString(),
      source: "integration-test",
    },
    data: {
      userId: "user_123",
      email: "janx@example.com",
    },
  };
}

function waitForMessage(label, timeoutMs = 5000) {
  let resolve;
  let reject;

  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });

  const timer = setTimeout(() => {
    reject(new Error(`Timed out waiting for ${label}`));
  }, timeoutMs);

  return {
    promise,
    resolve(value) {
      clearTimeout(timer);
      resolve(value);
    },
    reject(error) {
      clearTimeout(timer);
      reject(error);
    },
  };
}

async function eventually(fn, options = {}) {
  const timeoutMs = options.timeoutMs ?? 5000;
  const intervalMs = options.intervalMs ?? 100;
  const startedAt = Date.now();

  let lastResult;

  while (Date.now() - startedAt < timeoutMs) {
    lastResult = await fn();

    if (lastResult) {
      return lastResult;
    }

    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }

  throw new Error(options.message ?? "Condition was not met in time");
}

async function cleanupRabbitMq({
  url,
  queue,
  deadLetterQueue,
  exchange,
  deadLetterExchange,
}) {
  const connection = await amqp.connect(url);
  const channel = await connection.createChannel();

  try {
    await channel.deleteQueue(queue).catch(() => {});
    await channel.deleteQueue(deadLetterQueue).catch(() => {});
    await channel.deleteExchange(exchange).catch(() => {});
    await channel.deleteExchange(deadLetterExchange).catch(() => {});
  } finally {
    await channel.close().catch(() => {});
    await connection.close().catch(() => {});
  }
}

describe("RabbitMqTransport integration", function () {
  this.timeout(10000);

  let transport;
  let names;

  before(function () {
    if (!RUN_INTEGRATION_TESTS) {
      this.skip();
    }
  });

  afterEach(async function () {
    if (transport) {
      await transport.disconnect({ closeConnection: true }).catch(() => {});
      transport = null;
    }

    await RabbitMqConnectionRegistry.closeAll().catch(() => {});

    if (names) {
      await cleanupRabbitMq({
        url: RABBITMQ_URL,
        ...names,
      }).catch(() => {});

      names = null;
    }
  });

  function createTransport() {
    const id = createTestId();

    const namespace = id;
    const service = "email-service";

    names = {
      exchange: `conduit.${namespace}.events`,
      deadLetterExchange: `conduit.${namespace}.events.dlx`,
      queue: `${namespace}.${service}`,
      deadLetterQueue: `${namespace}.${service}.dlq`,
    };

    transport = new RabbitMqTransport({
      namespace,
      service,
      connectionName: id,
      url: RABBITMQ_URL,
    });

    return transport;
  }

  it("publishes and consumes an exact event type", async function () {
    const rabbit = createTransport();
    const message = createMessage("user.created");

    const received = waitForMessage("user.created");

    await rabbit.subscribe("user.created", async (event, ctx) => {
      received.resolve({
        event,
        routingKey: ctx.routingKey,
        exchange: ctx.exchange,
      });
    });

    await rabbit.publish(message);

    const result = await received.promise;

    expect(result.event).to.deep.equal(message);
    expect(result.routingKey).to.equal("user.created");
    expect(result.exchange).to.equal(names.exchange);
  });

  it('publishes and consumes all events with "#" subscription', async function () {
    const rabbit = createTransport();
    const message = createMessage("payment.received");

    const received = waitForMessage("payment.received through #");

    await rabbit.subscribe("#", async (event, ctx) => {
      received.resolve({
        event,
        routingKey: ctx.routingKey,
      });
    });

    await rabbit.publish(message);

    const result = await received.promise;

    expect(result.event).to.deep.equal(message);
    expect(result.routingKey).to.equal("payment.received");
  });

  it("dead-letters a message when the handler throws", async function () {
    const rabbit = createTransport();
    const message = createMessage("user.created");

    await rabbit.subscribe("user.created", async () => {
      throw new Error("Handler failed");
    });

    await rabbit.publish(message);

    const adminConnection = await amqp.connect(RABBITMQ_URL);
    const adminChannel = await adminConnection.createChannel();

    try {
      const deadLetteredMessage = await eventually(
        async () => {
          return adminChannel.get(names.deadLetterQueue, {
            noAck: true,
          });
        },
        {
          timeoutMs: 5000,
          intervalMs: 100,
          message: "Message was not dead-lettered in time",
        },
      );

      const body = JSON.parse(deadLetteredMessage.content.toString("utf8"));

      expect(body).to.deep.equal(message);

      expect(deadLetteredMessage.properties.headers).to.have.property(
        "x-death",
      );
    } finally {
      await adminChannel.close().catch(() => {});
      await adminConnection.close().catch(() => {});
    }
  });

  it("does not deliver a non-matching event to an exact subscription", async function () {
    const rabbit = createTransport();

    let handled = false;

    await rabbit.subscribe("user.created", async () => {
      handled = true;
    });

    await rabbit.publish(createMessage("random.gossip"));

    await new Promise((resolve) => setTimeout(resolve, 500));

    expect(handled).to.equal(false);
  });
});
