export function configureRabbitMqTransport(config = {}) {
  const namespace = config.namespace ?? "default";
  if (!config.service) {
    throw new Error("RabbitMQ transport requires config.service");
  }
  const service = config.service;

  const exchange = config.exchange ?? `conduit.${namespace}.events`;

  const queue = normalizeQueueInput(config.queue);

  const queueName =
    queue.name ?? (service ? `${namespace}.${service}` : undefined);

  return {
    namespace,
    service,

    connectionName: config.connectionName ?? "main",
    url: config.url,
    amqp: config.amqp,
    connection: config.connection,

    exchange,
    exchangeType: config.exchangeType ?? "topic",
    prefetch: config.prefetch ?? 10,

    queue: {
      name: queueName,
      durable: queue.durable ?? true,
      exclusive: queue.exclusive ?? false,
      autoDelete: queue.autoDelete ?? false,
      arguments: queue.arguments ?? {},
      deadLetter: queue.deadLetter ?? true,
    },
  };
}

export function configureRabbitMqQueue(baseConfig, queueInput = {}) {
  const queue = normalizeQueueInput(queueInput);

  const name = queue.name ?? baseConfig.queue.name;

  if (!name) {
    throw new Error(
      "RabbitMQ queue name is required. Provide service, queue.name, or subscribe queue.name.",
    );
  }

  const deadLetter = configureDeadLetterQueue({
    queueName: name,
    exchange: baseConfig.exchange,
    deadLetter: queue.deadLetter ?? baseConfig.queue.deadLetter,
  });

  return {
    name,
    durable: queue.durable ?? baseConfig.queue.durable,
    exclusive: queue.exclusive ?? baseConfig.queue.exclusive,
    autoDelete: queue.autoDelete ?? baseConfig.queue.autoDelete,

    arguments: {
      ...(baseConfig.queue.arguments ?? {}),
      ...(queue.arguments ?? {}),
      ...(deadLetter.enabled
        ? {
            "x-dead-letter-exchange": deadLetter.exchange,
            "x-dead-letter-routing-key": deadLetter.routingKey,
          }
        : {}),
    },

    deadLetter,
  };
}

function configureDeadLetterQueue({ queueName, exchange, deadLetter = true }) {
  if (deadLetter === false) {
    return {
      enabled: false,
    };
  }

  if (deadLetter === true || deadLetter == null) {
    return {
      enabled: true,
      exchange: `${exchange}.dlx`,
      exchangeType: "direct",
      queue: `${queueName}.dlq`,
      routingKey: `${queueName}.dead`,
    };
  }

  if (deadLetter.enabled === false) {
    return {
      enabled: false,
    };
  }

  return {
    enabled: true,
    exchange: deadLetter.exchange ?? `${exchange}.dlx`,
    exchangeType: deadLetter.exchangeType ?? "direct",
    queue: deadLetter.queue ?? `${queueName}.dlq`,
    routingKey: deadLetter.routingKey ?? `${queueName}.dead`,
  };
}

function normalizeQueueInput(queue) {
  if (typeof queue === "string") {
    return {
      name: queue,
    };
  }

  return queue ?? {};
}
