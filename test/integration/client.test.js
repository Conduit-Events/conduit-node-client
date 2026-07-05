import { expect } from "chai";
import Client from "../../src/client/client.js";
import { RabbitMqConnectionRegistry } from "../../src/transports/rabbitMQ/rabbitmq-connection-registry.js";

const RABBITMQ_URL = process.env.RABBITMQ_URL ?? "amqp://localhost";

function waitFor(assertion, { timeout = 3000, interval = 25 } = {}) {
  return new Promise((resolve, reject) => {
    const startedAt = Date.now();

    const check = () => {
      try {
        assertion();
        resolve();
      } catch (error) {
        if (Date.now() - startedAt >= timeout) {
          reject(error);
          return;
        }

        setTimeout(check, interval);
      }
    };

    check();
  });
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
async function expectRejected(fn) {
  let error;

  try {
    await fn();
  } catch (caughtError) {
    error = caughtError;
  }

  expect(error).to.be.instanceOf(Error);

  return error;
}
function createTestId() {
  return `test_${Date.now()}_${Math.random().toString(16).slice(2)}`;
}
describe("Client", function () {
  this.timeout(10000);
  let publisher;
  let serviceA;
  let serviceB;

  afterEach(async function () {
    await Promise.allSettled([
      serviceA?.stop?.(),
      serviceB?.stop?.(),
      publisher?.stop?.(),
    ]);

    await RabbitMqConnectionRegistry.closeAll();

    publisher = null;
    serviceA = null;
    serviceB = null;
  });

  it("two clients can receive each other's messages", async function () {
    const namespace = createTestId();

    serviceA = Client.create({
      namespace,
      service: "service-a",
      rabbitmq: {
        url: RABBITMQ_URL,
      },
    });

    serviceB = Client.create({
      namespace,
      service: "service-b",
      rabbitmq: {
        url: RABBITMQ_URL,
      },
    });

    const receivedByA = [];
    const receivedByB = [];

    await serviceA.start();
    await serviceB.start();

    await serviceA.on("service-b.sent", async (message) => {
      receivedByA.push(message);
    });

    await serviceB.on("service-a.sent", async (message) => {
      receivedByB.push(message);
    });

    await serviceA.emit("service-a.sent", {
      text: "Hello from A",
    });

    await serviceB.emit("service-b.sent", {
      text: "Hello from B",
    });

    await waitFor(() => {
      expect(receivedByA).to.have.lengthOf(1);
      expect(receivedByB).to.have.lengthOf(1);
    });

    const messageFromB = receivedByA[0];
    const messageFromA = receivedByB[0];

    expect(messageFromB.data).to.deep.equal({
      text: "Hello from B",
    });

    expect(messageFromB.meta.kind).to.equal("event");
    expect(messageFromB.meta.type).to.equal("service-b.sent");
    expect(messageFromB.meta.source).to.equal("service-b");

    expect(messageFromA.data).to.deep.equal({
      text: "Hello from A",
    });

    expect(messageFromA.meta.kind).to.equal("event");
    expect(messageFromA.meta.type).to.equal("service-a.sent");
    expect(messageFromA.meta.source).to.equal("service-a");
  });

  it("validates typed event data before publishing and receiving", async function () {
    const namespace = createTestId();

    const schemas = {
      "service-a.sent": {
        type: "object",
        required: ["text"],
        additionalProperties: false,
        properties: {
          text: {
            type: "string",
            minLength: 1,
          },
        },
      },

      "service-b.sent": {
        type: "object",
        required: ["text", "priority"],
        additionalProperties: false,
        properties: {
          text: {
            type: "string",
            minLength: 1,
          },
          priority: {
            type: "integer",
            minimum: 1,
            maximum: 5,
          },
        },
      },
    };

    serviceA = Client.create({
      namespace,
      service: "service-a",
      schemas,
      rabbitmq: {
        url: RABBITMQ_URL,
        connectionName: `${namespace}-service-a`,
      },
    });

    serviceB = Client.create({
      namespace,
      service: "service-b",
      schemas,
      rabbitmq: {
        url: RABBITMQ_URL,
        connectionName: `${namespace}-service-b`,
      },
    });

    const receivedByA = waitForMessage("service-b.sent received by service-a");
    const receivedByB = waitForMessage("service-a.sent received by service-b");

    await serviceA.start();
    await serviceB.start();

    await serviceA.on("service-b.sent", async (message, ctx) => {
      receivedByA.resolve({
        message,
        routingKey: ctx.routingKey,
      });
    });

    await serviceB.on("service-a.sent", async (message, ctx) => {
      receivedByB.resolve({
        message,
        routingKey: ctx.routingKey,
      });
    });

    await serviceA.emit("service-a.sent", {
      text: "Hello from A",
    });

    await serviceB.emit("service-b.sent", {
      text: "Hello from B",
      priority: 3,
    });

    const resultA = await receivedByA.promise;
    const resultB = await receivedByB.promise;

    expect(resultA.message.meta.type).to.equal("service-b.sent");
    expect(resultA.message.data).to.deep.equal({
      text: "Hello from B",
      priority: 3,
    });

    expect(resultB.message.meta.type).to.equal("service-a.sent");
    expect(resultB.message.data).to.deep.equal({
      text: "Hello from A",
    });

    const invalidServiceAError = await expectRejected(() => {
      return serviceA.emit("service-a.sent", {
        text: "",
      });
    });

    expect(invalidServiceAError.message).to.match(/schema|valid|validation/i);

    const invalidServiceBError = await expectRejected(() => {
      return serviceB.emit("service-b.sent", {
        text: "Hello from B",
      });
    });

    expect(invalidServiceBError.message).to.match(/schema|valid|validation/i);
  });

  it("delivers the same event to multiple handlers on one service and to other subscribed services", async function () {
    const namespace = createTestId();
    const eventType = "shared.message-sent";

    publisher = Client.create({
      namespace,
      service: "publisher-service",
      rabbitmq: {
        url: RABBITMQ_URL,
        connectionName: `${namespace}-publisher-service`,
      },
    });

    serviceA = Client.create({
      namespace,
      service: "service-a",
      rabbitmq: {
        url: RABBITMQ_URL,
        connectionName: `${namespace}-service-a`,
      },
    });

    serviceB = Client.create({
      namespace,
      service: "service-b",
      rabbitmq: {
        url: RABBITMQ_URL,
        connectionName: `${namespace}-service-b`,
      },
    });

    const serviceAFirstHandler = waitForMessage(
      "service-a first handler received shared.message-sent",
    );

    const serviceASecondHandler = waitForMessage(
      "service-a second handler received shared.message-sent",
    );

    const serviceBHandler = waitForMessage(
      "service-b handler received shared.message-sent",
    );

    await publisher.start();
    await serviceA.start();
    await serviceB.start();

    await serviceA.on(eventType, async (message, ctx) => {
      serviceAFirstHandler.resolve({
        message,
        routingKey: ctx.routingKey,
      });
    });

    await serviceA.on(eventType, async (message, ctx) => {
      serviceASecondHandler.resolve({
        message,
        routingKey: ctx.routingKey,
      });
    });

    await serviceB.on(eventType, async (message, ctx) => {
      serviceBHandler.resolve({
        message,
        routingKey: ctx.routingKey,
      });
    });

    await publisher.emit(eventType, {
      text: "Hello to everyone",
    });

    const [serviceAFirstResult, serviceASecondResult, serviceBResult] =
      await Promise.all([
        serviceAFirstHandler.promise,
        serviceASecondHandler.promise,
        serviceBHandler.promise,
      ]);

    expect(serviceAFirstResult.message.data).to.deep.equal({
      text: "Hello to everyone",
    });

    expect(serviceASecondResult.message.data).to.deep.equal({
      text: "Hello to everyone",
    });

    expect(serviceBResult.message.data).to.deep.equal({
      text: "Hello to everyone",
    });

    expect(serviceAFirstResult.message.meta.type).to.equal(eventType);
    expect(serviceASecondResult.message.meta.type).to.equal(eventType);
    expect(serviceBResult.message.meta.type).to.equal(eventType);

    expect(serviceAFirstResult.message.meta.source).to.equal(
      "publisher-service",
    );

    expect(serviceASecondResult.message.meta.source).to.equal(
      "publisher-service",
    );

    expect(serviceBResult.message.meta.source).to.equal("publisher-service");

    expect(serviceAFirstResult.routingKey).to.equal(eventType);
    expect(serviceASecondResult.routingKey).to.equal(eventType);
    expect(serviceBResult.routingKey).to.equal(eventType);

    expect(serviceAFirstResult.message.meta.id).to.equal(
      serviceASecondResult.message.meta.id,
    );

    expect(serviceAFirstResult.message.meta.id).to.equal(
      serviceBResult.message.meta.id,
    );
  });
});
