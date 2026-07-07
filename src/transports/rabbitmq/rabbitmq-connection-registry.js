import { RabbitMqConnection } from "./rabbitmq-connection.js";

const connections = new Map();

function normalizeConfig(config = {}) {
  const trimmedUrl = config.url?.trim();

  return {
    ...config,
    url: trimmedUrl || "amqp://localhost",
  };
}

function getConnectionSignature(config = {}) {
  return JSON.stringify({
    url: config.url,
  });
}

export class RabbitMqConnectionRegistry {
  static acquire(name = "main", config = {}) {
    const normalizedConfig = normalizeConfig(config);
    const signature = getConnectionSignature(normalizedConfig);

    let record = connections.get(name);

    if (record) {
      if (record.signature !== signature) {
        throw new Error(
          `RabbitMQ connection "${name}" already exists with different config`,
        );
      }

      record.references += 1;
      return record.connection;
    }

    const connection = new RabbitMqConnection(normalizedConfig);

    connections.set(name, {
      connection,
      signature,
      references: 1,
    });

    return connection;
  }

  static async release(name = "main", expectedConnection) {
    const record = connections.get(name);

    if (!record) return false;

    // Prevent an old transport from releasing a newer connection that
    // happens to use the same registry name.
    if (expectedConnection && record.connection !== expectedConnection) {
      return false;
    }

    record.references -= 1;

    if (record.references > 0) {
      return false;
    }

    // Remove it before awaiting close. A new acquisition during closing
    // can then safely create a new registry record.
    connections.delete(name);

    await record.connection.close();

    return true;
  }

  static references(name = "main") {
    return connections.get(name)?.references ?? 0;
  }

  static has(name = "main") {
    return connections.has(name);
  }

  static list() {
    return [...connections.keys()];
  }

  static async closeAll() {
    const records = [...connections.values()];

    connections.clear();

    await Promise.allSettled(
      records.map(({ connection }) => connection.close()),
    );
  }

  static clear() {
    connections.clear();
  }
}
