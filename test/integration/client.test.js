import { expect } from "chai";
import Client from "../../src/client/client.js";

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

describe("Client", function () {
  this.timeout(10000);

  let serviceA;
  let serviceB;

  afterEach(async function () {
    await Promise.allSettled([serviceA?.stop?.(), serviceB?.stop?.()]);
  });

  it("two clients can receive each other's messages", async function () {
    const namespace = `test-${Date.now()}-${Math.random()
      .toString(16)
      .slice(2)}`;

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
});
