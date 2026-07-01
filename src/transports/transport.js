// src/transports/transport.js

export class Transport {
  async connect() {
    throw new Error("Transport.connect() must be implemented");
  }

  async disconnect() {
    throw new Error("Transport.disconnect() must be implemented");
  }

  async publish(_message, _options = {}) {
    throw new Error("Transport.publish() must be implemented");
  }

  async subscribe(_pattern, _handler, _options = {}) {
    throw new Error("Transport.subscribe() must be implemented");
  }
}
