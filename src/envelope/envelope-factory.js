import { randomUUID } from "node:crypto";

export class EnvelopeFactory {
  constructor({
    source,
    defaultVersion = "1.0.0",
    idGenerator = randomUUID,
    clock = () => new Date().toISOString(),
  } = {}) {
    this._source = source;
    this._defaultVersion = defaultVersion;
    this._idGenerator = idGenerator;
    this._clock = clock;
  }

  createEvent(meta, data) {
    return this.create(
      {
        ...meta,
        kind: "event",
      },
      data,
    );
  }

  createCommand(meta, data) {
    return this.create(
      {
        ...meta,
        kind: "command",
      },
      data,
    );
  }

  create(meta, data = {}) {
    if (!meta) throw new Error("Envelope meta is required");

    const source = meta.source ?? this._source;
    const version = meta.version ?? this._defaultVersion;

    if (!meta.kind) throw new Error("Envelope kind is required");
    if (!meta.type) throw new Error("Envelope type is required");
    // if (!meta.streamId) throw new Error("Envelope streamId is required");
    if (!source) throw new Error("Envelope source is required");

    if (!data || typeof data !== "object" || Array.isArray(data)) {
      throw new Error("Envelope data must be an object");
    }

    return {
      meta: {
        ...meta,
        id: this._idGenerator(),
        version,
        source,
        streamId: meta.streamId ?? this._idGenerator(),
        correlationId: meta.correlationId ?? this._idGenerator(),
        timestamp: this._clock(),
      },
      data,
    };
  }
}
