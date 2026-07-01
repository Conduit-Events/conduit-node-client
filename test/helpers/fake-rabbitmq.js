// test/helpers/fake-rabbitmq.js

export function createDeferred() {
  let resolve;
  let reject;

  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });

  return { promise, resolve, reject };
}
export function createFakeAmqpConnection(options = {}) {
  const normalChannels = [];
  const confirmChannels = [];
  const handlers = {};

  return {
    handlers,
    normalChannels,
    confirmChannels,
    closeCalls: 0,

    on(event, handler) {
      handlers[event] = handler;
      return this;
    },

    async close() {
      this.closeCalls += 1;

      if (handlers.close) {
        handlers.close();
      }
    },

    async createChannel() {
      const channel = createFakeChannel(options);
      normalChannels.push(channel);
      return channel;
    },

    async createConfirmChannel() {
      const channel = createFakeChannel(options);
      confirmChannels.push(channel);
      return channel;
    },
  };
}

export function createFakeChannel(options = {}) {
  const handlers = {};
  const onceHandlers = {};

  return {
    handlers,
    onceHandlers,

    assertExchangeCalls: [],
    assertQueueCalls: [],
    bindQueueCalls: [],
    publishCalls: [],
    consumeCalls: [],
    cancelCalls: [],
    ackCalls: [],
    nackCalls: [],
    prefetchCalls: [],

    closeCalls: 0,
    waitForConfirmsCalls: 0,

    publishAccepted: options.publishAccepted ?? true,

    on(event, handler) {
      handlers[event] = handler;
      return this;
    },

    once(event, handler) {
      onceHandlers[event] = handler;
      return this;
    },

    emit(event, ...args) {
      if (handlers[event]) {
        handlers[event](...args);
      }

      if (onceHandlers[event]) {
        onceHandlers[event](...args);
        delete onceHandlers[event];
      }
    },

    async assertExchange(...args) {
      this.assertExchangeCalls.push(args);
    },

    async assertQueue(...args) {
      this.assertQueueCalls.push(args);
    },

    async bindQueue(...args) {
      this.bindQueueCalls.push(args);
    },

    publish(...args) {
      this.publishCalls.push(args);
      return this.publishAccepted;
    },

    async waitForConfirms() {
      this.waitForConfirmsCalls += 1;
    },

    async prefetch(count) {
      this.prefetchCalls.push(count);
    },

    async consume(queue, handler, options) {
      this.consumeCalls.push({ queue, handler, options });

      return {
        consumerTag:
          options?.consumerTag ?? `consumer-${this.consumeCalls.length}`,
      };
    },

    async cancel(consumerTag) {
      this.cancelCalls.push(consumerTag);
    },

    ack(rawMessage) {
      this.ackCalls.push(rawMessage);
    },

    nack(rawMessage, allUpTo, requeue) {
      this.nackCalls.push({
        rawMessage,
        allUpTo,
        requeue,
      });
    },

    async close() {
      this.closeCalls += 1;
      this.emit("close");
    },
  };
}

export function createRawMessage(content, overrides = {}) {
  return {
    content: Buffer.from(content, "utf8"),

    fields: {
      routingKey: "user.created",
      exchange: "conduit.studio.events",
      redelivered: false,
      ...overrides.fields,
    },

    properties: {
      messageId: "evt_123",
      correlationId: "corr_123",
      ...overrides.properties,
    },
  };
}
