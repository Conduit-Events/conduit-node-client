import EnvelopeFactory from "../envelope/envelope-factory.js";
import SchemaValidator from "../schema/schema-validator.js";
import { RabbitMqTransport } from "../transports/rabbitmq/rabbitmq-transport.js";

const DEFAULT_NAMESPACE = "default";

export default class Client {
  _config;

  constructor(config = {}) {
    const namespace = config.namespace ?? DEFAULT_NAMESPACE;
    const serviceName = config.serviceName;
    const source = config.source ?? serviceName;

    this._config = {
      ...config,
      namespace,
      serviceName,
      source,
    };

    this.transport =
      config.transport ??
      new RabbitMqTransport({
        ...(config.rabbitmq ?? {}),
        namespace,
        serviceName,
      });

    this.envelopes = config.envelopes ?? new EnvelopeFactory(this._config);

    this.validator = config.validator ?? new SchemaValidator(config.schemas);
  }

  static create(...args) {
    return new this(...args);
  }

  async connect() {
    await this.transport.connect();
    return this;
  }

  async disconnect(options = {}) {
    await this.transport.disconnect(options);
    return this;
  }

  async emit(type, data, options = {}) {
    return this.#publish("event", type, data, options);
  }

  async command(type, data, options = {}) {
    return this.#publish("command", type, data, options);
  }

  async subscribe(type, handler, options = {}) {
    this.#assertCanSubscribe(type, handler, options);

    return this.transport.subscribe(
      type,
      async (rawMessage, transportContext = {}) => {
        const message = this.envelopes.parse(rawMessage);

        this.validator.validate(message);

        const context = this.#createHandlerContext(message, transportContext);

        return handler(message, context);
      },
      options,
    );
  }

  async #publish(kind, type, data, options = {}) {
    const message = this.envelopes.create({
      kind,
      type,
      data,
      ...options,
    });

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

    if (!this._config.serviceName && !options.queue?.name) {
      throw new Error(
        "Client subscribe requires config.serviceName or options.queue.name. This prevents accidental queue-name collisions.",
      );
    }
  }
}
