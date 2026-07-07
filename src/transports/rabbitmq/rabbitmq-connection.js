// rabbitmq-connection.js
import amqp from "amqplib";

export class RabbitMqConnection {
  constructor(config = {}) {
    this._url = config.url ?? "amqp://localhost";
    this._amqp = config.amqp ?? amqp;

    this._connection = null;
    this._connecting = null;
    this._closing = false;
  }

  async connect() {
    if (this._connection) {
      return this._connection;
    }
    if (this._connecting) {
      return this._connecting;
    }

    this._connecting = this._amqp.connect(this._url);

    try {
      const connection = await this._connecting;
      this._connection = connection;

      connection.on("close", () => {
        this._connection = null;
      });

      connection.on("error", (err) => {
        if (!this._closing) {
          console.error("RabbitMQ connection error:", err);
        }
      });

      return connection;
    } finally {
      this._connecting = null;
    }
  }

  async createChannel() {
    const connection = await this.connect();
    return connection.createChannel();
  }

  async createConfirmChannel() {
    const connection = await this.connect();
    return connection.createConfirmChannel();
  }

  async close() {
    if (!this._connection) {
      return;
    }

    this._closing = true;

    try {
      await this._connection.close();
    } finally {
      this._connection = null;
      this._connecting = null;
      this._closing = false;
    }
  }
}
