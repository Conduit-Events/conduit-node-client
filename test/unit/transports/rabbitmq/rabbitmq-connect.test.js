// test/unit/transports/rabbitmq/rabbitmq-connection.test.js

import { expect } from "chai";
import { RabbitMqConnection } from "../../../../src/transports/rabbitmq/rabbitmq-connection.js";
import {
  createDeferred,
  createFakeAmqpConnection,
} from "../../../helpers/fake-rabbitmq.js";

describe("RabbitMqConnection", function () {
  it("connects to the configured RabbitMQ URL", async function () {
    const fakeBrokerConnection = createFakeAmqpConnection();
    const connectCalls = [];

    const fakeAmqp = {
      async connect(url) {
        connectCalls.push(url);
        return fakeBrokerConnection;
      },
    };

    const connection = new RabbitMqConnection({
      url: "amqp://test-broker",
      amqp: fakeAmqp,
    });

    const result = await connection.connect();

    expect(result).to.equal(fakeBrokerConnection);
    expect(connectCalls).to.deep.equal(["amqp://test-broker"]);
    expect(fakeBrokerConnection.handlers.close).to.be.a("function");
    expect(fakeBrokerConnection.handlers.error).to.be.a("function");
  });

  it("reuses an existing connection", async function () {
    const fakeBrokerConnection = createFakeAmqpConnection();
    const connectCalls = [];

    const fakeAmqp = {
      async connect(url) {
        connectCalls.push(url);
        return fakeBrokerConnection;
      },
    };

    const connection = new RabbitMqConnection({
      url: "amqp://test-broker",
      amqp: fakeAmqp,
    });

    const first = await connection.connect();
    const second = await connection.connect();

    expect(first).to.equal(fakeBrokerConnection);
    expect(second).to.equal(fakeBrokerConnection);
    expect(connectCalls).to.have.length(1);
  });

  it("coalesces concurrent connect calls into one RabbitMQ connection attempt", async function () {
    const fakeBrokerConnection = createFakeAmqpConnection();
    const deferred = createDeferred();
    const connectCalls = [];

    const fakeAmqp = {
      connect(url) {
        connectCalls.push(url);
        return deferred.promise;
      },
    };

    const connection = new RabbitMqConnection({
      url: "amqp://test-broker",
      amqp: fakeAmqp,
    });

    const firstPromise = connection.connect();
    const secondPromise = connection.connect();

    expect(connectCalls).to.deep.equal(["amqp://test-broker"]);

    deferred.resolve(fakeBrokerConnection);

    const [first, second] = await Promise.all([firstPromise, secondPromise]);

    expect(first).to.equal(fakeBrokerConnection);
    expect(second).to.equal(fakeBrokerConnection);
  });

  it("creates a normal channel", async function () {
    const fakeBrokerConnection = createFakeAmqpConnection();

    const fakeAmqp = {
      async connect() {
        return fakeBrokerConnection;
      },
    };

    const connection = new RabbitMqConnection({
      amqp: fakeAmqp,
    });

    const channel = await connection.createChannel();

    expect(channel).to.equal(fakeBrokerConnection.normalChannels[0]);
    expect(fakeBrokerConnection.normalChannels).to.have.length(1);
  });

  it("creates a confirm channel", async function () {
    const fakeBrokerConnection = createFakeAmqpConnection();

    const fakeAmqp = {
      async connect() {
        return fakeBrokerConnection;
      },
    };

    const connection = new RabbitMqConnection({
      amqp: fakeAmqp,
    });

    const channel = await connection.createConfirmChannel();

    expect(channel).to.equal(fakeBrokerConnection.confirmChannels[0]);
    expect(fakeBrokerConnection.confirmChannels).to.have.length(1);
  });

  it("closes the broker connection and clears internal state", async function () {
    const fakeBrokerConnection = createFakeAmqpConnection();

    const fakeAmqp = {
      async connect() {
        return fakeBrokerConnection;
      },
    };

    const connection = new RabbitMqConnection({
      amqp: fakeAmqp,
    });

    await connection.connect();
    await connection.close();

    expect(fakeBrokerConnection.closeCalls).to.equal(1);
  });

  it("does nothing when closing before a connection exists", async function () {
    const fakeAmqp = {
      async connect() {
        throw new Error("Should not connect");
      },
    };

    const connection = new RabbitMqConnection({
      amqp: fakeAmqp,
    });

    await connection.close();
  });
});
