// test/helpers/fake-client-dependencies.js

export function createDeferred() {
  let resolve;
  let reject;

  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });

  return {
    promise,
    resolve,
    reject,
  };
}

export function createRuntimeSubscription() {
  return {
    unsubscribeCalls: 0,

    async unsubscribe() {
      this.unsubscribeCalls += 1;
    },
  };
}

export function createFakeTransport({ events = [] } = {}) {
  return {
    connectCalls: 0,
    disconnectCalls: [],
    publishCalls: [],
    subscribeCalls: [],
    runtimeSubscriptions: [],

    connectError: null,
    disconnectError: null,
    publishError: null,
    subscribeError: null,

    connectImpl: null,
    disconnectImpl: null,
    publishImpl: null,
    subscribeImpl: null,

    publishResult: {
      accepted: true,
    },

    async connect() {
      this.connectCalls += 1;
      events.push("transport.connect");

      if (this.connectError) throw this.connectError;
      if (this.connectImpl) return this.connectImpl();
    },

    async disconnect(options = {}) {
      this.disconnectCalls.push(options);
      events.push("transport.disconnect");

      if (this.disconnectError) throw this.disconnectError;
      if (this.disconnectImpl) return this.disconnectImpl(options);
    },

    async publish(message, options = {}) {
      this.publishCalls.push({
        message,
        options,
      });

      events.push("transport.publish");

      if (this.publishError) throw this.publishError;
      if (this.publishImpl) {
        return this.publishImpl(message, options);
      }

      return this.publishResult;
    },

    async subscribe(type, handler, options = {}) {
      const call = {
        type,
        handler,
        options,
      };

      this.subscribeCalls.push(call);
      events.push(`transport.subscribe:${type}`);

      if (this.subscribeError) throw this.subscribeError;

      const runtimeSubscription = this.subscribeImpl
        ? await this.subscribeImpl(call)
        : createRuntimeSubscription();

      this.runtimeSubscriptions.push(runtimeSubscription);

      return runtimeSubscription;
    },
  };
}

export function createFakeEnvelopes({ events = [] } = {}) {
  let messageId = 0;

  return {
    createCalls: [],

    create(meta, data) {
      this.createCalls.push({
        meta,
        data,
      });

      events.push("envelopes.create");

      messageId += 1;

      return {
        meta: {
          id: `message-${messageId}`,
          version: "1.0.0",
          source: "test-service",
          streamId: meta.streamId ?? `stream-${messageId}`,
          correlationId: meta.correlationId ?? `correlation-${messageId}`,
          timestamp: "2026-07-07T12:00:00.000Z",
          ...meta,
        },
        data,
      };
    },
  };
}

export function createFakeValidator({ events = [] } = {}) {
  return {
    validateCalls: [],
    validateError: null,
    validateImpl: null,

    validate(data, type) {
      this.validateCalls.push({
        data,
        type,
      });

      events.push("validator.validate");

      if (this.validateError) throw this.validateError;
      if (this.validateImpl) return this.validateImpl(data, type);

      return true;
    },
  };
}
