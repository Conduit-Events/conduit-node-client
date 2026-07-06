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
  }

  static create(...args) {
    return new this(...args);
  }

  async start() {
    await this.transport.connect();
    this._started = true;
    return this;
  }

  async stop(options = {}) {
    await this.transport.disconnect(options);
    this._started = false;
    return this;
  }

  async emit(type, data, options = {}) {
    return this.#publish("event", type, data, options);
  }

  async command(type, data, options = {}) {
    return this.#publish("command", type, data, options);
  }

  async on(type, handler, options = {}) {
    this.#assertCanSubscribe(type, handler, options);

    return this.transport.subscribe(
      type,
      async (message, transportContext = {}) => {
        this.validator.validate(message);
        const context = this.#createHandlerContext(message, transportContext);

        return handler(message, context);
      },
      options,
    );
  }

  #assertStarted() {
    if (!this._started) {
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
      persistent: options.persistent,
      headers: options.headers,
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
}
