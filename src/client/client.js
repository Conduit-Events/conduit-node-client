import { EnvelopeFactory } from "../envelope/envelope-factory.js";
import { SchemaValidator } from "../schema/schema-validator.js";
import { RabbitMqTransport } from "../transports/rabbitmq/rabbitmq-transport.js";

const DEFAULT_NAMESPACE = "default";

export default class Client {
  _config;

  constructor(config = {}) {
    const namespace = config.namespace ?? DEFAULT_NAMESPACE;
    const service = config.service;
    const source = config.source ?? service;

    this._config = {
      ...config,
      namespace,
      service,
      source,
    };

    this.transport =
      config.transport ??
      new RabbitMqTransport({
        ...(config.rabbitmq ?? {}),
        namespace,
        service,
      });

    this.envelopes = config.envelopes ?? new EnvelopeFactory(this._config);

    this.validator = config.validator ?? new SchemaValidator(config.schemas);

    this._subscriptions = new Map();
    this._subscriptionId = 0;
    this._state = "stopped";
  }

  static create(...args) {
    return new this(...args);
  }
  async start() {
    if (this._state === "started") return this;
    if (this._state === "starting") return this._starting;

    this._state = "starting";

    this._starting = (async () => {
      try {
        await this.transport.connect();

        for (const subscription of this._subscriptions.values()) {
          await this.#activateSubscription(subscription);
        }

        this._state = "started";
        return this;
      } catch (error) {
        await this.transport.disconnect().catch(() => {});

        for (const subscription of this._subscriptions.values()) {
          subscription.active = false;
          subscription.transportSubscription = null;
          subscription.ready = Promise.resolve();
        }

        this._state = "failed";
        throw error;
      }
    })();

    try {
      return await this._starting;
    } finally {
      this._starting = null;
    }
  }
  async stop(options = {}) {
    if (this._state === "stopped") {
      return this;
    }

    this._state = "stopping";

    await this.transport.disconnect(options);

    for (const subscription of this._subscriptions.values()) {
      subscription.active = false;
      subscription.transportSubscription = null;
      subscription.ready = Promise.resolve();
    }

    this._state = "stopped";
    return this;
  }

  async emit(type, data, options = {}) {
    return this.#publish("event", type, data, options);
  }

  async command(type, data, options = {}) {
    return this.#publish("command", type, data, options);
  }

  on(type, handler, options = {}) {
    this.#assertCanSubscribe(type, handler, options);

    const subscription = {
      id: ++this._subscriptionId,
      type,
      handler,
      options,
      active: false,
      cancelled: false,
      transportSubscription: null,
      ready: Promise.resolve(),
    };

    this._subscriptions.set(subscription.id, subscription);

    if (this._state === "started") {
      subscription.ready = this.#activateSubscription(subscription);
    }

    return {
      id: subscription.id,
      type,
      get ready() {
        return subscription.ready;
      },
      unsubscribe: async () => {
        await this.#unsubscribe(subscription.id);
      },
    };
  }

  async subscribe(type, handler, options = {}) {
    // Subscirbe is a post start alias for 'on', which is asynchronous and will only complete once the
    // subscription is ready.
    if (this._state !== "started") {
      throw new Error(
        "Client must be started before subscribe() can activate a subscription. " +
          "Use on() to register handlers before start().",
      );
    }
    const subscription = this.on(type, handler, options);
    await subscription.ready;
    return subscription;
  }

  async #activateSubscription(subscription) {
    if (subscription.cancelled || subscription.active) {
      return;
    }

    const transportSubscription = await this.transport.subscribe(
      subscription.type,
      async (message, transportContext = {}) => {
        this.validator.validate(message);

        const context = this.#createHandlerContext(message, transportContext);

        return subscription.handler(message, context);
      },
      subscription.options,
    );

    subscription.transportSubscription = transportSubscription;
    subscription.active = true;
  }

  #assertStarted() {
    if (this._state !== "started") {
      throw new Error(`Client must be started before it can broadcast events.`);
    }
  }

  #assertCanPublish(type) {
    if (!type) {
      throw new Error("Client emit requires a type");
    }
  }

  async #publish(kind, type, data, options = {}) {
    this.#assertStarted();
    this.#assertCanPublish(type);
    const message = this.envelopes.create(
      {
        kind,
        type,
        ...options,
      },
      data,
    );
    this.validator.validate(message);

    return this.transport.publish(message, {
      routingKey: options.routingKey ?? type,
    });
  }
  #createHandlerContext(message, transportContext = {}) {
    return {
      ...transportContext,

      client: this,

      emit: (type, data, options = {}) => {
        return this.emit(
          type,
          data,
          this.#deriveChildOptions(message, options),
        );
      },

      command: (type, data, options = {}) => {
        return this.command(
          type,
          data,
          this.#deriveChildOptions(message, options),
        );
      },
    };
  }

  #deriveChildOptions(parentMessage, options = {}) {
    const parentMeta = parentMessage.meta ?? {};

    return {
      streamId: parentMeta.streamId,
      correlationId: parentMeta.correlationId ?? parentMeta.id,
      causationId: parentMeta.id,
      ...options,
    };
  }

  #assertCanSubscribe(type, handler, options = {}) {
    if (!type) {
      throw new Error("Client subscribe requires a type");
    }

    if (typeof handler !== "function") {
      throw new Error("Client subscribe requires a handler function");
    }

    if (!this._config.service && !options.queue?.name) {
      throw new Error(
        "Client subscribe requires config.service or options.queue.name. This prevents accidental queue-name collisions.",
      );
    }
  }

  async #unsubscribe(subscriptionId) {
    const subscription = this._subscriptions.get(subscriptionId);

    if (!subscription) {
      return false;
    }

    subscription.cancelled = true;
    this._subscriptions.delete(subscriptionId);

    // If activation is currently in progress, wait for it to settle.
    // This prevents a race where unsubscribe() is called while start()
    // is still activating subscriptions.
    try {
      await subscription.ready;
    } catch {
      // If activation failed, there may be nothing to clean up.
      // The original activation error should surface from start()/ready,
      // not from unsubscribe().
    }

    await this.#deactivateSubscription(subscription);

    return true;
  }

  async #deactivateSubscription(subscription) {
    const transportSubscription = subscription.transportSubscription;

    subscription.active = false;
    subscription.transportSubscription = null;
    subscription.ready = Promise.resolve();

    if (transportSubscription?.unsubscribe) {
      await transportSubscription.unsubscribe();
    }
  }
}
