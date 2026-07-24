// test/unit/client/client.test.js

import { expect } from "chai";
import Client from "../../../src/client/client.js";
import {
  createDeferred,
  createFakeEnvelopes,
  createFakeTransport,
  createFakeValidator,
  createRuntimeSubscription,
} from "../../helpers/fake-client-dependencies.js";

async function expectAsyncError(promise, expectedMessage) {
  let error;

  try {
    await promise;
  } catch (caught) {
    error = caught;
  }

  expect(error).to.be.instanceOf(Error);

  if (expectedMessage) {
    expect(error.message).to.equal(expectedMessage);
  }

  return error;
}

function createClient({
  config = {},
  events = [],
  transport = createFakeTransport({ events }),
  envelopes = createFakeEnvelopes({ events }),
  validator = createFakeValidator({ events }),
} = {}) {
  const client = Client.create({
    namespace: "studio",
    service: "test-service",
    ...config,
    transport,
    envelopes,
    validator,
  });

  return {
    client,
    transport,
    envelopes,
    validator,
    events,
  };
}

function createParentMessage(overrides = {}) {
  return {
    meta: {
      id: "parent-message-id",
      kind: "event",
      type: "user.created",
      version: "1.0.0",
      source: "user-service",
      streamId: "user_123",
      correlationId: "root-correlation-id",
      timestamp: "2026-07-07T12:00:00.000Z",
      ...overrides.meta,
    },
    data: {
      userId: "user_123",
      ...overrides.data,
    },
  };
}

describe("Client", function () {
  describe("create", function () {
    it("creates a client instance", function () {
      const { transport, envelopes, validator } = createClient();

      const client = Client.create({
        namespace: "studio",
        service: "test-service",
        transport,
        envelopes,
        validator,
      });

      expect(client).to.be.instanceOf(Client);
    });
  });

  describe("start", function () {
    it("connects the transport and returns the client", async function () {
      const { client, transport } = createClient();

      const result = await client.start();

      expect(result).to.equal(client);
      expect(transport.connectCalls).to.equal(1);
    });

    it("does not connect twice when start is called repeatedly", async function () {
      const { client, transport } = createClient();

      await client.start();
      await client.start();

      expect(transport.connectCalls).to.equal(1);
    });

    it("coalesces concurrent start calls", async function () {
      const deferred = createDeferred();
      const transport = createFakeTransport();

      transport.connectImpl = () => deferred.promise;

      const { client } = createClient({ transport });

      const firstStart = client.start();
      const secondStart = client.start();

      expect(transport.connectCalls).to.equal(1);

      deferred.resolve();

      expect(await firstStart).to.equal(client);
      expect(await secondStart).to.equal(client);
      expect(transport.connectCalls).to.equal(1);
    });

    it("activates subscriptions registered before start", async function () {
      const { client, transport } = createClient();
      const handler = async () => {};

      client.on("user.created", handler);

      expect(transport.subscribeCalls).to.have.length(0);

      await client.start();

      expect(transport.subscribeCalls).to.have.length(1);
      expect(transport.subscribeCalls[0].type).to.equal("user.created");
    });

    it("waits for pre-start subscriptions to become active", async function () {
      const deferred = createDeferred();
      const runtimeSubscription = createRuntimeSubscription();
      const transport = createFakeTransport();

      transport.subscribeImpl = () => deferred.promise;

      const { client } = createClient({ transport });

      client.on("user.created", async () => {});

      let startResolved = false;

      const starting = client.start().then((result) => {
        startResolved = true;
        return result;
      });

      await Promise.resolve();

      expect(transport.subscribeCalls).to.have.length(1);
      expect(startResolved).to.equal(false);

      deferred.resolve(runtimeSubscription);

      expect(await starting).to.equal(client);
      expect(startResolved).to.equal(true);
    });

    it("rejects when the transport cannot connect", async function () {
      const transport = createFakeTransport();

      transport.connectError = new Error("Connection failed");

      const { client } = createClient({ transport });

      await expectAsyncError(client.start(), "Connection failed");
    });

    it("rejects when a subscription cannot be activated", async function () {
      const transport = createFakeTransport();

      transport.subscribeError = new Error("Subscription activation failed");

      const { client } = createClient({ transport });

      client.on("user.created", async () => {});

      await expectAsyncError(client.start(), "Subscription activation failed");
    });

    it("cleans up and can retry after subscription activation fails", async function () {
      const transport = createFakeTransport();

      let activationCalls = 0;

      transport.subscribeImpl = async () => {
        activationCalls += 1;

        if (activationCalls === 2) {
          throw new Error("Subscription activation failed");
        }

        return createRuntimeSubscription();
      };

      const { client } = createClient({ transport });

      client.on("user.created", async () => {});
      client.on("user.updated", async () => {});

      await expectAsyncError(client.start(), "Subscription activation failed");

      expect(transport.disconnectCalls).to.deep.equal([{}]);

      transport.subscribeImpl = async () => createRuntimeSubscription();

      await client.start();

      expect(transport.connectCalls).to.equal(2);

      expect(transport.subscribeCalls.map(({ type }) => type)).to.deep.equal([
        "user.created",
        "user.updated",
        "user.created",
        "user.updated",
      ]);
    });
  });

  describe("stop", function () {
    it("does nothing when the client has not started", async function () {
      const { client, transport } = createClient();

      const result = await client.stop();

      expect(result).to.equal(client);
      expect(transport.disconnectCalls).to.have.length(0);
    });

    it("disconnects the transport and returns the client", async function () {
      const { client, transport } = createClient();

      await client.start();

      const result = await client.stop();

      expect(result).to.equal(client);
      expect(transport.disconnectCalls).to.deep.equal([{}]);
    });

    it("passes stop options to the transport", async function () {
      const { client, transport } = createClient();

      await client.start();
      await client.stop({
        reason: "test-shutdown",
      });

      expect(transport.disconnectCalls).to.deep.equal([
        {
          reason: "test-shutdown",
        },
      ]);
    });

    it("does not disconnect twice when stop is called repeatedly", async function () {
      const { client, transport } = createClient();

      await client.start();

      await client.stop();
      await client.stop();

      expect(transport.disconnectCalls).to.have.length(1);
    });

    it("preserves logical subscriptions across a restart", async function () {
      const { client, transport } = createClient();

      client.on("user.created", async () => {});

      await client.start();

      expect(transport.subscribeCalls).to.have.length(1);

      await client.stop();
      await client.start();

      expect(transport.subscribeCalls).to.have.length(2);
      expect(transport.subscribeCalls[1].type).to.equal("user.created");
    });

    it("returns to stopped state when transport disconnect fails", async function () {
      const transport = createFakeTransport();
      const { client } = createClient({ transport });

      client.on("user.created", async () => {});

      await client.start();

      transport.disconnectError = new Error("Transport disconnect failed");

      await expectAsyncError(client.stop(), "Transport disconnect failed");

      await expectAsyncError(
        client.emit("user.created", {}),
        "Client must be started before it can broadcast events.",
      );

      transport.disconnectError = null;

      await client.start();

      expect(transport.connectCalls).to.equal(2);
      expect(transport.subscribeCalls).to.have.length(2);
    });
  });

  describe("publishing", function () {
    it("rejects emit before the client is started", async function () {
      const { client } = createClient();

      await expectAsyncError(
        client.emit("user.created", {}),
        "Client must be started before it can broadcast events.",
      );
    });

    it("rejects command before the client is started", async function () {
      const { client } = createClient();

      await expectAsyncError(
        client.command("user.create", {}),
        "Client must be started before it can broadcast events.",
      );
    });

    it("rejects publishing without a type", async function () {
      const { client } = createClient();

      await client.start();

      await expectAsyncError(
        client.emit("", {}),
        "Client emit requires a type",
      );
    });

    it("creates, validates and publishes an event", async function () {
      const events = [];
      const { client, transport, envelopes, validator } = createClient({
        events,
      });

      await client.start();

      const data = {
        userId: "user_123",
      };

      const result = await client.emit("user.created", data, {
        streamId: "user_123",
      });

      expect(envelopes.createCalls).to.deep.equal([
        {
          meta: {
            kind: "event",
            type: "user.created",
            streamId: "user_123",
          },
          data,
        },
      ]);

      const message = transport.publishCalls[0].message;

      expect(validator.validateCalls).to.deep.equal([
        {
          data: message,
          type: undefined,
        },
      ]);

      expect(transport.publishCalls).to.deep.equal([
        {
          message,
          options: {
            routingKey: "user.created",
          },
        },
      ]);

      expect(result).to.deep.equal({
        accepted: true,
      });

      expect(events).to.deep.equal([
        "transport.connect",
        "envelopes.create",
        "validator.validate",
        "transport.publish",
      ]);
    });

    it("creates and publishes a command", async function () {
      const { client, envelopes, transport } = createClient();

      await client.start();

      await client.command(
        "user.create",
        {
          email: "janx@example.com",
        },
        {
          streamId: "user_123",
        },
      );

      expect(envelopes.createCalls[0]).to.deep.equal({
        meta: {
          kind: "command",
          type: "user.create",
          streamId: "user_123",
        },
        data: {
          email: "janx@example.com",
        },
      });

      expect(transport.publishCalls[0].options.routingKey).to.equal(
        "user.create",
      );
    });

    it("uses the message type as the routing key", async function () {
      const { client, transport } = createClient();

      await client.start();

      await client.emit(
        "user.created",
        {
          userId: "user_123",
        },
        {
          streamId: "user_123",
        },
      );

      expect(transport.publishCalls[0].options).to.deep.equal({
        routingKey: "user.created",
      });
    });

    it("allows overwriting the routing key (not recommended at this stage)", async function () {
      const { client, transport } = createClient();

      await client.start();

      await client.emit(
        "user.created",
        {
          userId: "user_123",
        },
        {
          streamId: "user_123",
          routingKey: "another.event",
        },
      );

      expect(transport.publishCalls[0].options).to.deep.equal({
        routingKey: "another.event",
      });
    });

    it("passes a custom routing key only to the transport", async function () {
      const { client, transport, envelopes } = createClient();

      await client.start();

      await client.emit(
        "user.created",
        {
          userId: "user_123",
        },
        {
          streamId: "user_123",
          routingKey: "another.event",
        },
      );

      expect(envelopes.createCalls[0].meta).to.deep.equal({
        kind: "event",
        type: "user.created",
        streamId: "user_123",
      });

      expect(transport.publishCalls[0].options).to.deep.equal({
        routingKey: "another.event",
      });
    });

    it("separates envelope metadata from transport options", async function () {
      const { client, envelopes, transport } = createClient();

      await client.start();

      await client.emit(
        "user.created",
        { userId: "user_123" },
        {
          streamId: "user_123",
          correlationId: "correlation_123",
          routingKey: "tenant.user.created",
        },
      );

      expect(envelopes.createCalls[0].meta).to.deep.equal({
        kind: "event",
        type: "user.created",
        streamId: "user_123",
        correlationId: "correlation_123",
      });

      expect(transport.publishCalls[0].options).to.deep.equal({
        routingKey: "tenant.user.created",
      });
    });
    it("does not publish when validation fails", async function () {
      const validator = createFakeValidator();

      validator.validateError = new Error("Invalid event");

      const { client, transport } = createClient({
        validator,
      });

      await client.start();

      await expectAsyncError(
        client.emit("user.created", {
          userId: "user_123",
        }),
        "Invalid event",
      );

      expect(transport.publishCalls).to.have.length(0);
    });

    it("propagates transport publishing failures", async function () {
      const transport = createFakeTransport();

      transport.publishError = new Error("Publish failed");

      const { client } = createClient({ transport });

      await client.start();

      await expectAsyncError(
        client.emit("user.created", {
          userId: "user_123",
        }),
        "Publish failed",
      );
    });
  });

  describe("on", function () {
    it("rejects a missing subscription type", function () {
      const { client } = createClient();

      expect(() => client.on("", async () => {})).to.throw(
        "Client subscribe requires a type",
      );
    });

    it("rejects a non-function handler", function () {
      const { client } = createClient();

      expect(() => client.on("user.created", null)).to.throw(
        "Client subscribe requires a handler function",
      );
    });

    it("requires a service or explicit queue name", function () {
      const { client } = createClient({
        config: {
          service: undefined,
        },
      });

      expect(() => client.on("user.created", async () => {})).to.throw(
        "Client subscribe requires config.service",
      );
    });

    it("allows an explicit queue when service is absent", function () {
      const { client } = createClient({
        config: {
          service: undefined,
        },
      });

      expect(() =>
        client.on("user.created", async () => {}, {
          queue: {
            name: "studio.custom-service",
          },
        }),
      ).not.to.throw();
    });

    it("registers before start without touching the transport", function () {
      const { client, transport } = createClient();

      const subscription = client.on("user.created", async () => {});

      expect(subscription.id).to.equal(1);
      expect(subscription.type).to.equal("user.created");
      expect(subscription.ready).to.be.instanceOf(Promise);
      expect(transport.subscribeCalls).to.have.length(0);
    });

    it("activates immediately when registered after start", async function () {
      const { client, transport } = createClient();

      await client.start();

      const subscription = client.on("user.created", async () => {});

      await subscription.ready;

      expect(transport.subscribeCalls).to.have.length(1);
      expect(transport.subscribeCalls[0].type).to.equal("user.created");
    });

    it("passes subscription options to the transport", async function () {
      const { client, transport } = createClient();

      client.on("user.created", async () => {}, {
        queue: {
          name: "studio.user-projection",
        },
        requeueOnError: true,
      });

      await client.start();

      expect(transport.subscribeCalls[0].options).to.deep.equal({
        queue: {
          name: "studio.user-projection",
        },
        requeueOnError: true,
      });
    });

    it("supports multiple handlers for the same event type", async function () {
      const { client, transport } = createClient();

      client.on("user.created", async () => {});
      client.on("user.created", async () => {});

      await client.start();

      expect(transport.subscribeCalls).to.have.length(2);
      expect(transport.subscribeCalls.map(({ type }) => type)).to.deep.equal([
        "user.created",
        "user.created",
      ]);
    });

    it("validates incoming messages before calling the handler", async function () {
      const events = [];
      const { client, transport } = createClient({ events });

      let handledMessage;

      client.on("user.created", async (message) => {
        events.push("handler");
        handledMessage = message;
      });

      await client.start();

      const message = createParentMessage();
      const wrappedHandler = transport.subscribeCalls[0].handler;

      await wrappedHandler(message);

      expect(handledMessage).to.equal(message);
      expect(events).to.deep.equal([
        "transport.connect",
        "transport.subscribe:user.created",
        "validator.validate",
        "handler",
      ]);
    });

    it("does not call the handler when incoming validation fails", async function () {
      const validator = createFakeValidator();

      validator.validateError = new Error("Incoming message invalid");

      const { client, transport } = createClient({
        validator,
      });

      let handlerCalls = 0;

      client.on("user.created", async () => {
        handlerCalls += 1;
      });

      await client.start();

      const wrappedHandler = transport.subscribeCalls[0].handler;

      await expectAsyncError(
        wrappedHandler(createParentMessage()),
        "Incoming message invalid",
      );

      expect(handlerCalls).to.equal(0);
    });

    it("passes transport context and the client to the handler", async function () {
      const { client, transport } = createClient();

      let receivedContext;

      client.on("user.created", async (_message, context) => {
        receivedContext = context;
      });

      await client.start();

      await transport.subscribeCalls[0].handler(createParentMessage(), {
        routingKey: "user.created",
        deliveryTag: 42,
      });

      expect(receivedContext.routingKey).to.equal("user.created");
      expect(receivedContext.deliveryTag).to.equal(42);
      expect(receivedContext.client).to.equal(client);
      expect(receivedContext.emit).to.be.a("function");
      expect(receivedContext.command).to.be.a("function");
    });

    it("returns the handler result to the transport", async function () {
      const { client, transport } = createClient();

      client.on("user.created", async () => {
        return "handler-result";
      });

      await client.start();

      const result = await transport.subscribeCalls[0].handler(
        createParentMessage(),
      );

      expect(result).to.equal("handler-result");
    });
  });

  describe("subscribe", function () {
    it("registers and waits until a post-start subscription is active", async function () {
      const deferred = createDeferred();
      const runtimeSubscription = createRuntimeSubscription();
      const transport = createFakeTransport();

      const { client } = createClient({ transport });

      await client.start();

      transport.subscribeImpl = () => deferred.promise;

      let subscribeResolved = false;

      const subscribing = client
        .subscribe("user.created", async () => {})
        .then((subscription) => {
          subscribeResolved = true;
          return subscription;
        });

      await Promise.resolve();

      expect(subscribeResolved).to.equal(false);
      expect(transport.subscribeCalls).to.have.length(1);

      deferred.resolve(runtimeSubscription);

      const subscription = await subscribing;

      expect(subscribeResolved).to.equal(true);
      expect(subscription.type).to.equal("user.created");
    });

    it("rejects before start without registering a subscription", async function () {
      const { client, transport } = createClient();

      await expectAsyncError(
        client.subscribe("user.created", async () => {}),
        "Client must be started before subscribe() can activate a subscription. Use on() to register handlers before start().",
      );

      await client.start();

      expect(transport.subscribeCalls).to.have.length(0);
    });
  });

  describe("unsubscribe", function () {
    it("removes a pre-start subscription without touching the transport", async function () {
      const { client, transport } = createClient();

      const subscription = client.on("user.created", async () => {});

      await subscription.unsubscribe();
      await client.start();

      expect(transport.subscribeCalls).to.have.length(0);
    });

    it("unsubscribes the active transport subscription", async function () {
      const { client, transport } = createClient();

      await client.start();

      const subscription = client.on("user.created", async () => {});

      await subscription.ready;

      const runtimeSubscription = transport.runtimeSubscriptions[0];

      await subscription.unsubscribe();

      expect(runtimeSubscription.unsubscribeCalls).to.equal(1);
    });

    it("does not unsubscribe the transport twice", async function () {
      const { client, transport } = createClient();

      await client.start();

      const subscription = client.on("user.created", async () => {});

      await subscription.ready;

      const runtimeSubscription = transport.runtimeSubscriptions[0];

      await subscription.unsubscribe();
      await subscription.unsubscribe();

      expect(runtimeSubscription.unsubscribeCalls).to.equal(1);
    });

    it("waits for in-progress activation before unsubscribing", async function () {
      const deferred = createDeferred();
      const runtimeSubscription = createRuntimeSubscription();
      const transport = createFakeTransport();

      const { client } = createClient({ transport });

      await client.start();

      transport.subscribeImpl = () => deferred.promise;

      const subscription = client.on("user.created", async () => {});

      let unsubscribeResolved = false;

      const unsubscribing = subscription.unsubscribe().then(() => {
        unsubscribeResolved = true;
      });

      await Promise.resolve();

      expect(unsubscribeResolved).to.equal(false);

      deferred.resolve(runtimeSubscription);

      await unsubscribing;

      expect(unsubscribeResolved).to.equal(true);
      expect(runtimeSubscription.unsubscribeCalls).to.equal(1);
    });

    it("does not restore an unsubscribed handler after restart", async function () {
      const { client, transport } = createClient();

      const subscription = client.on("user.created", async () => {});

      await client.start();
      await subscription.unsubscribe();

      await client.stop();
      await client.start();

      expect(transport.subscribeCalls).to.have.length(1);
    });
  });

  describe("handler context", function () {
    it("emits a child event with inherited tracing metadata", async function () {
      const { client, transport, envelopes } = createClient();

      client.on("user.created", async (_message, context) => {
        await context.emit("welcome-email.requested", {
          userId: "user_123",
        });
      });

      await client.start();

      await transport.subscribeCalls[0].handler(createParentMessage());

      expect(envelopes.createCalls[0]).to.deep.equal({
        meta: {
          kind: "event",
          type: "welcome-email.requested",
          streamId: "user_123",
          correlationId: "root-correlation-id",
          causationId: "parent-message-id",
        },
        data: {
          userId: "user_123",
        },
      });
    });

    it("uses the parent id as correlationId when the parent has none", async function () {
      const { client, transport, envelopes } = createClient();

      client.on("user.created", async (_message, context) => {
        await context.emit("child.created", {});
      });

      await client.start();

      const parent = createParentMessage();

      delete parent.meta.correlationId;

      await transport.subscribeCalls[0].handler(parent);

      expect(envelopes.createCalls[0].meta.correlationId).to.equal(
        "parent-message-id",
      );
    });

    it("allows child streamId and correlationId to override inherited metadata", async function () {
      const { client, transport, envelopes } = createClient();

      client.on("user.created", async (_message, context) => {
        await context.emit(
          "child.created",
          {},
          {
            streamId: "custom-stream",
            correlationId: "custom-correlation",
          },
        );
      });

      await client.start();

      await transport.subscribeCalls[0].handler(createParentMessage());

      expect(envelopes.createCalls[0].meta).to.deep.equal({
        kind: "event",
        type: "child.created",
        streamId: "custom-stream",
        correlationId: "custom-correlation",
        causationId: "parent-message-id",
      });
    });

    it("does not allow child causationId to override the parent id", async function () {
      const { client, transport, envelopes } = createClient();

      client.on("user.created", async (_message, context) => {
        await context.emit(
          "child.created",
          {},
          {
            causationId: "custom-causation",
          },
        );
      });

      await client.start();

      await transport.subscribeCalls[0].handler(createParentMessage());

      expect(envelopes.createCalls[0].meta.causationId).to.equal(
        "parent-message-id",
      );
    });

    it("creates child commands with inherited tracing metadata", async function () {
      const { client, transport, envelopes } = createClient();

      client.on("user.created", async (_message, context) => {
        await context.command("welcome-email.send", {
          userId: "user_123",
        });
      });

      await client.start();

      await transport.subscribeCalls[0].handler(createParentMessage());

      expect(envelopes.createCalls[0]).to.deep.equal({
        meta: {
          kind: "command",
          type: "welcome-email.send",
          streamId: "user_123",
          correlationId: "root-correlation-id",
          causationId: "parent-message-id",
        },
        data: {
          userId: "user_123",
        },
      });
    });
  });
});
