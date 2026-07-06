// test/unit/transports/rabbitmq/rabbitmq-connection-registry.test.js

import { expect } from "chai";
import { RabbitMqConnectionRegistry } from "../../../../src/transports/rabbitmq/rabbitmq-connection-registry.js";
import { createFakeAmqpConnection } from "../../../helpers/fake-rabbitmq.js";

describe("RabbitMqConnectionRegistry", function () {
  beforeEach(function () {
    RabbitMqConnectionRegistry.clear();
  });

  afterEach(async function () {
    await RabbitMqConnectionRegistry.closeAll();
    RabbitMqConnectionRegistry.clear();
  });

  it("returns the same connection for the same name and config", function () {
    const first = RabbitMqConnectionRegistry.get("main", {
      url: "amqp://test-broker",
    });

    const second = RabbitMqConnectionRegistry.get("main", {
      url: "amqp://test-broker",
    });

    expect(first).to.equal(second);
    expect(RabbitMqConnectionRegistry.has("main")).to.equal(true);
  });

  it("returns different connections for different names", function () {
    const main = RabbitMqConnectionRegistry.get("main", {
      url: "amqp://test-broker",
    });

    const analytics = RabbitMqConnectionRegistry.get("analytics", {
      url: "amqp://test-broker",
    });

    expect(main).to.not.equal(analytics);
    expect(RabbitMqConnectionRegistry.has("main")).to.equal(true);
    expect(RabbitMqConnectionRegistry.has("analytics")).to.equal(true);
  });

  it("throws when the same connection name is reused with different config", function () {
    RabbitMqConnectionRegistry.get("main", {
      url: "amqp://first-broker",
    });

    expect(() => {
      RabbitMqConnectionRegistry.get("main", {
        url: "amqp://second-broker",
      });
    }).to.throw(
      'RabbitMQ connection "main" already exists with different config',
    );
  });

  it("closes and removes a named connection", async function () {
    const fakeBrokerConnection = createFakeAmqpConnection();

    const fakeAmqp = {
      async connect() {
        return fakeBrokerConnection;
      },
    };

    const connection = RabbitMqConnectionRegistry.get("main", {
      url: "amqp://test-broker",
      amqp: fakeAmqp,
    });

    await connection.connect();

    expect(RabbitMqConnectionRegistry.has("main")).to.equal(true);

    await RabbitMqConnectionRegistry.close("main");

    expect(fakeBrokerConnection.closeCalls).to.equal(1);
    expect(RabbitMqConnectionRegistry.has("main")).to.equal(false);
  });

  it("close is safe when the named connection does not exist", async function () {
    await RabbitMqConnectionRegistry.close("missing");
  });

  it("closes all registered connections", async function () {
    const firstBrokerConnection = createFakeAmqpConnection();
    const secondBrokerConnection = createFakeAmqpConnection();

    const firstAmqp = {
      async connect() {
        return firstBrokerConnection;
      },
    };

    const secondAmqp = {
      async connect() {
        return secondBrokerConnection;
      },
    };

    const first = RabbitMqConnectionRegistry.get("main", {
      url: "amqp://first-broker",
      amqp: firstAmqp,
    });

    const second = RabbitMqConnectionRegistry.get("analytics", {
      url: "amqp://second-broker",
      amqp: secondAmqp,
    });

    await first.connect();
    await second.connect();

    await RabbitMqConnectionRegistry.closeAll();

    expect(firstBrokerConnection.closeCalls).to.equal(1);
    expect(secondBrokerConnection.closeCalls).to.equal(1);
    expect(RabbitMqConnectionRegistry.has("main")).to.equal(false);
    expect(RabbitMqConnectionRegistry.has("analytics")).to.equal(false);
  });

  it("clear removes registry entries without closing broker connections", async function () {
    const fakeBrokerConnection = createFakeAmqpConnection();

    const fakeAmqp = {
      async connect() {
        return fakeBrokerConnection;
      },
    };

    const connection = RabbitMqConnectionRegistry.get("main", {
      url: "amqp://test-broker",
      amqp: fakeAmqp,
    });

    await connection.connect();

    RabbitMqConnectionRegistry.clear();

    expect(RabbitMqConnectionRegistry.has("main")).to.equal(false);
    expect(fakeBrokerConnection.closeCalls).to.equal(0);
  });

  it("lists multiple connection", async function () {
    const fakeBrokerConnection = createFakeAmqpConnection();

    const fakeAmqp = {
      async connect() {
        return fakeBrokerConnection;
      },
    };
    const _connection = RabbitMqConnectionRegistry.get("main1", {
      url: "amqp://test-broker",
      amqp: fakeAmqp,
    });

    const _connection2 = RabbitMqConnectionRegistry.get("main2", {
      url: "amqp://test-broker2",
      amqp: fakeAmqp,
    });

    expect(RabbitMqConnectionRegistry.list()).to.deep.equal(["main1", "main2"]);
    RabbitMqConnectionRegistry.clear();
  });
});
