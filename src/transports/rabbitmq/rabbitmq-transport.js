import { Transport } from "../transport.js";
import { RabbitMqConnectionRegistry } from "./rabbitmq-connection-registry.js";
import {
  configureRabbitMqTransport,
  configureRabbitMqQueue,
} from "./rabbitmq-transport-config.js";

export class RabbitMqTransport extends Transport {
  constructor(config = {}) {
    super();
    this._config = configureRabbitMqTransport(config);

    this._connection =
      this._config.connection ??
      RabbitMqConnectionRegistry.get(this._config.connectionName, {
        url: this._config.url,
        amqp: this._config.amqp,
      });

    this._usesRegistry = !this._config.connection;

    this._publishChannel = null;
    this._consumeChannel = null;
    this._connecting = null;

    this._declaredQueues = new Map();
    this._subscriptionsByQueue = new Map();
    this._consumersByQueue = new Map();

    this._subscriptionId = 0;
  }

  async connect() {
    if (this._publishChannel && this._consumeChannel) {return;}

    if (this._connecting) {return this._connecting;}

    this._connecting = this._connectChannels();

    try {
      await this._connecting;
    } finally {
      this._connecting = null;
    }
  }

  async _connectChannels() {
    this._publishChannel = await this._connection.createConfirmChannel();
    this._consumeChannel = await this._connection.createChannel();

    this._publishChannel.on("close", () => {
      this._publishChannel = null;
    });

    this._consumeChannel.on("close", () => {
      this._consumeChannel = null;
    });

    this._publishChannel.on("error", (err) => {
      console.error("RabbitMQ publish channel error:", err);
    });

    this._consumeChannel.on("error", (err) => {
      console.error("RabbitMQ consume channel error:", err);
    });

    await this._publishChannel.assertExchange(
      this._config.exchange,
      this._config.exchangeType,
      { durable: true },
    );

    await this._consumeChannel.assertExchange(
      this._config.exchange,
      this._config.exchangeType,
      { durable: true },
    );

    await this._consumeChannel.prefetch(this._config.prefetch);
  }

  async disconnect(options = {}) {
    const closeConnection = options.closeConnection ?? false;

    for (const consumer of this._consumersByQueue.values()) {
      if (this._consumeChannel) {
        await this._consumeChannel.cancel(consumer.consumerTag);
      }
    }

    this._consumersByQueue.clear();
    this._subscriptionsByQueue.clear();

    if (this._publishChannel) {
      await this._publishChannel.close();
      this._publishChannel = null;
    }

    if (this._consumeChannel) {
      await this._consumeChannel.close();
      this._consumeChannel = null;
    }

    if (closeConnection && this._usesRegistry) {
      await RabbitMqConnectionRegistry.close(this._config.connectionName);
    }
  }

  async publish(message, options = {}) {
    await this.connect();

    const routingKey = options.routingKey ?? message?.meta?.type;

    if (!routingKey) {
      throw new Error(
        "RabbitMQ publish requires options.routingKey or message.meta.type",
      );
    }

    const body = Buffer.from(JSON.stringify(message));

    const accepted = this._publishChannel.publish(
      this._config.exchange,
      routingKey,
      body,
      {
        persistent: options.persistent ?? true,
        contentType: "application/json",
        messageId: message?.meta?.id,
        correlationId: message?.meta?.correlationId,
        timestamp: Date.now(),
        headers: {
          kind: message?.meta?.kind,
          type: message?.meta?.type,
          version: message?.meta?.version,
          source: message?.meta?.source,
          namespace: this._config.namespace,
          ...options.headers,
        },
      },
    );

    if (!accepted) {
      await new Promise((resolve) => {
        this._publishChannel.once("drain", resolve);
      });
    }

    await this._publishChannel.waitForConfirms();
  }

  async ensureQueue(queueInput = {}) {
    await this.connect();
    const queueOptions = configureRabbitMqQueue(this._config, queueInput);

    await this._assertQueueInfrastructure(queueOptions);

    return queueOptions.name;
  }

  async subscribe(pattern, handler, options = {}) {
    this._validatePattern(pattern);

    if (typeof handler !== "function") {
      throw new Error("RabbitMQ subscribe requires a handler function");
    }

    const queue = await this.ensureQueue(options.queue);

    await this._consumeChannel.bindQueue(queue, this._config.exchange, pattern);

    const subscription = {
      id: ++this._subscriptionId,
      queue,
      pattern,
      handler,
      onError: options.onError,
      requeueOnError: options.requeueOnError ?? false,
    };

    this._addSubscription(subscription);

    await this._ensureConsumer(queue, {
      consumerTag: options.consumerTag,
    });

    return {
      queue,
      pattern,

      unsubscribe: async () => {
        await this._removeSubscription(subscription);
      },
    };
  }

  _addSubscription(subscription) {
    const subscriptions =
      this._subscriptionsByQueue.get(subscription.queue) ?? [];

    subscriptions.push(subscription);

    this._subscriptionsByQueue.set(subscription.queue, subscriptions);
  }

  async _ensureConsumer(queue, options = {}) {
    if (this._consumersByQueue.has(queue)) {return;}

    const consumer = await this._consumeChannel.consume(
      queue,
      async (rawMessage) => {
        await this._handleRawMessage(queue, rawMessage);
      },
      {
        noAck: false,
        consumerTag: options.consumerTag,
      },
    );

    this._consumersByQueue.set(queue, {
      queue,
      consumerTag: consumer.consumerTag,
    });
  }

  async _handleRawMessage(queue, rawMessage) {
    if (!rawMessage) {return;}

    let message;

    try {
      message = JSON.parse(rawMessage.content.toString("utf8"));
    } catch {
      this._consumeChannel.nack(rawMessage, false, false);
      return;
    }

    const routingKey = rawMessage.fields.routingKey;

    const subscriptions = this._subscriptionsByQueue.get(queue) ?? [];

    const matchingSubscriptions = subscriptions.filter((subscription) =>
      this._matchesPattern(subscription.pattern, routingKey),
    );

    if (matchingSubscriptions.length === 0) {
      this._consumeChannel.ack(rawMessage);
      return;
    }

    const ctx = {
      routingKey,
      exchange: rawMessage.fields.exchange,
      redelivered: rawMessage.fields.redelivered,
      properties: rawMessage.properties,
    };

    for (const subscription of matchingSubscriptions) {
      try {
        await subscription.handler(message, ctx);
      } catch (err) {
        if (subscription.onError) {
          await subscription.onError(err, message, ctx);
        }

        this._consumeChannel.nack(
          rawMessage,
          false,
          subscription.requeueOnError,
        );

        return;
      }
    }

    this._consumeChannel.ack(rawMessage);
  }

  async _removeSubscription(subscription) {
    const subscriptions =
      this._subscriptionsByQueue.get(subscription.queue) ?? [];

    const remainingSubscriptions = subscriptions.filter(
      (item) => item.id !== subscription.id,
    );

    if (remainingSubscriptions.length > 0) {
      this._subscriptionsByQueue.set(
        subscription.queue,
        remainingSubscriptions,
      );

      return;
    }

    this._subscriptionsByQueue.delete(subscription.queue);

    const consumer = this._consumersByQueue.get(subscription.queue);

    if (consumer && this._consumeChannel) {
      await this._consumeChannel.cancel(consumer.consumerTag);
    }

    this._consumersByQueue.delete(subscription.queue);
  }

  async _assertQueueInfrastructure(queueOptions) {
    this._assertQueueDeclarationIsCompatible(queueOptions.name, queueOptions);

    if (queueOptions.deadLetter.enabled) {
      await this._consumeChannel.assertExchange(
        queueOptions.deadLetter.exchange,
        queueOptions.deadLetter.exchangeType,
        { durable: true },
      );

      await this._consumeChannel.assertQueue(queueOptions.deadLetter.queue, {
        durable: true,
      });

      await this._consumeChannel.bindQueue(
        queueOptions.deadLetter.queue,
        queueOptions.deadLetter.exchange,
        queueOptions.deadLetter.routingKey,
      );
    }

    await this._consumeChannel.assertQueue(queueOptions.name, {
      durable: queueOptions.durable,
      exclusive: queueOptions.exclusive,
      autoDelete: queueOptions.autoDelete,
      arguments: queueOptions.arguments,
    });
  }

  _validatePattern(pattern) {
    if (!pattern) {
      throw new Error("RabbitMQ subscribe requires a pattern");
    }

    if (pattern === "#") {return;}

    if (pattern.includes("*") || pattern.includes("#")) {
      throw new Error(
        'RabbitMQ transport currently supports exact event types or "#" only',
      );
    }
  }

  _matchesPattern(pattern, routingKey) {
    return pattern === "#" || pattern === routingKey;
  }

  _assertQueueDeclarationIsCompatible(queue, queueOptions) {
    const signature = this._getQueueDeclarationSignature(queueOptions);
    const existingSignature = this._declaredQueues.get(queue);

    if (existingSignature && existingSignature !== signature) {
      throw new Error(
        `RabbitMQ queue "${queue}" has already been declared with different options`,
      );
    }

    this._declaredQueues.set(queue, signature);
  }

  _getQueueDeclarationSignature(queueOptions) {
    return this._stableStringify({
      durable: queueOptions.durable,
      exclusive: queueOptions.exclusive,
      autoDelete: queueOptions.autoDelete,
      arguments: queueOptions.arguments ?? {},
    });
  }

  _stableStringify(value) {
    if (Array.isArray(value)) {
      return `[${value.map((item) => this._stableStringify(item)).join(",")}]`;
    }

    if (value && typeof value === "object") {
      return `{${Object.keys(value)
        .sort()
        .map(
          (key) =>
            `${JSON.stringify(key)}:${this._stableStringify(value[key])}`,
        )
        .join(",")}}`;
    }

    return JSON.stringify(value);
  }
}
