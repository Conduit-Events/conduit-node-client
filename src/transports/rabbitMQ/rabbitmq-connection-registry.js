import { RabbitMqConnection } from "./rabbitmq-connection.js";

const connections = new Map();

function getConnectionSignature(config = {}) {
  return JSON.stringify({
    url: config.url,
  });
}

function normalizeConfig(config = {}) {
  const trimmedUrl = config.url?.trim();

  return {
    ...config,
    url: trimmedUrl || "amqp://localhost",
  };
}
export class RabbitMqConnectionRegistry {
  static get(name = "main", config = {}) {
    config = normalizeConfig(config);
    const connectionName = name ?? "main";
    const signature = getConnectionSignature(config);

    const existing = connections.get(connectionName);

    if (existing) {
      if (existing.signature !== signature) {
        throw new Error(
          `RabbitMQ connection "${connectionName}" already exists with different config`,
        );
      }

      return existing.connection;
    }

    const connection = new RabbitMqConnection(config);

    connections.set(connectionName, {
      connection,
      signature,
    });

    return connection;
  }

  static async close(name = "main") {
    const record = connections.get(name);

    if (!record) return;

    await record.connection.close();
    connections.delete(name);
  }

  static async closeAll() {
    for (const record of connections.values()) {
      await record.connection.close();
    }

    connections.clear();
  }

  static has(name = "main") {
    return connections.has(name);
  }

  static clear() {
    connections.clear();
  }
}
