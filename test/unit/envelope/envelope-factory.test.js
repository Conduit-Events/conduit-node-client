import { expect } from "chai";
import { EnvelopeFactory } from "../../../src/envelope/envelope-factory.js";

describe("EnvelopeFactory", function () {
  const fixedTimestamp = "2026-06-29T12:00:00.000Z";

  const coreEventMeta = {
    kind: "event",
    type: "user.created",
    streamId: "user_123",
  };

  const coreCommandMeta = {
    kind: "command",
    type: "user.create",
    streamId: "user_123",
  };

  const coreEventData = {
    userId: "user_123",
    email: "janx@example.com",
  };

  function createFactory(overrides = {}) {
    const ids = [
      "message-id-1",
      "correlation-id-1",
      "message-id-2",
      "correlation-id-2",
    ];

    return new EnvelopeFactory({
      source: "test-service",
      defaultVersion: "1.0.0",
      idGenerator: () => ids.shift(),
      clock: () => fixedTimestamp,
      ...overrides,
    });
  }

  describe("createEvent", function () {
    it("creates an event envelope", function () {
      const factory = createFactory();

      const envelope = factory.createEvent(
        {
          type: coreEventMeta.type,
          streamId: coreEventMeta.streamId,
        },
        coreEventData,
      );

      expect(envelope).to.deep.equal({
        meta: {
          id: "message-id-1",
          kind: "event",
          type: "user.created",
          version: "1.0.0",
          streamId: "user_123",
          source: "test-service",
          correlationId: "correlation-id-1",
          timestamp: fixedTimestamp,
        },
        data: coreEventData,
      });
    });

    it("forces kind to event", function () {
      const factory = createFactory();

      const envelope = factory.createEvent(
        {
          ...coreEventMeta,
          kind: "command",
        },
        {},
      );

      expect(envelope.meta.kind).to.equal("event");
    });
  });

  describe("createCommand", function () {
    it("creates a command envelope", function () {
      const factory = createFactory();

      const envelope = factory.createCommand(
        {
          type: coreCommandMeta.type,
          streamId: coreCommandMeta.streamId,
        },
        {
          email: "janx@example.com",
        },
      );

      expect(envelope.meta.kind).to.equal("command");
      expect(envelope.meta.type).to.equal("user.create");
      expect(envelope.meta.streamId).to.equal("user_123");
      expect(envelope.meta.source).to.equal("test-service");
      expect(envelope.meta.version).to.equal("1.0.0");
      expect(envelope.data).to.deep.equal({
        email: "janx@example.com",
      });
    });

    it("forces kind to command", function () {
      const factory = createFactory();

      const envelope = factory.createCommand(
        {
          ...coreCommandMeta,
          kind: "event",
        },
        {},
      );

      expect(envelope.meta.kind).to.equal("command");
    });
  });

  describe("create", function () {
    it("creates a generic event envelope", function () {
      const factory = createFactory();

      const envelope = factory.create(coreEventMeta, coreEventData);

      expect(envelope.meta.kind).to.equal("event");
      expect(envelope.meta.type).to.equal("user.created");
      expect(envelope.meta.streamId).to.equal("user_123");
      expect(envelope.meta.id).to.equal("message-id-1");
      expect(envelope.meta.correlationId).to.equal("correlation-id-1");
      expect(envelope.meta.timestamp).to.equal(fixedTimestamp);
      expect(envelope.data).to.deep.equal(coreEventData);
    });

    it("creates a generic command envelope", function () {
      const factory = createFactory();

      const envelope = factory.create(coreCommandMeta, {
        email: "janx@example.com",
      });

      expect(envelope.meta.kind).to.equal("command");
      expect(envelope.meta.type).to.equal("user.create");
      expect(envelope.meta.streamId).to.equal("user_123");
      expect(envelope.data).to.deep.equal({
        email: "janx@example.com",
      });
    });

    it("uses an empty object as the default data", function () {
      const factory = createFactory();

      const envelope = factory.create(coreEventMeta);

      expect(envelope.data).to.deep.equal({});
    });

    it("allows message source to override factory source", function () {
      const factory = createFactory();

      const envelope = factory.create(
        {
          ...coreEventMeta,
          source: "override-service",
        },
        {},
      );

      expect(envelope.meta.source).to.equal("override-service");
    });

    it("allows message version to override default version", function () {
      const factory = createFactory();

      const envelope = factory.create(
        {
          ...coreEventMeta,
          version: "2.0.0",
        },
        {},
      );

      expect(envelope.meta.version).to.equal("2.0.0");
    });

    it("uses provided correlationId instead of generating one", function () {
      const factory = createFactory();

      const envelope = factory.create(
        {
          ...coreEventMeta,
          correlationId: "existing-correlation-id",
        },
        {},
      );

      expect(envelope.meta.id).to.equal("message-id-1");
      expect(envelope.meta.correlationId).to.equal("existing-correlation-id");
    });

    it("preserves causationId when provided", function () {
      const factory = createFactory();

      const envelope = factory.create(
        {
          ...coreEventMeta,
          type: "user.emailChanged",
          causationId: "previous-message-id",
        },
        {},
      );

      expect(envelope.meta.causationId).to.equal("previous-message-id");
    });

    it("factory-owned fields override caller-provided id and timestamp", function () {
      const factory = createFactory();

      const envelope = factory.create(
        {
          ...coreEventMeta,
          id: "caller-id",
          timestamp: "1999-01-01T00:00:00.000Z",
        },
        {},
      );

      expect(envelope.meta.id).to.equal("message-id-1");
      expect(envelope.meta.timestamp).to.equal(fixedTimestamp);
    });
  });

  describe("validation errors", function () {
    it("throws when meta is missing", function () {
      const factory = createFactory();

      expect(() => factory.create()).to.throw("Envelope meta is required");
    });

    it("throws when kind is missing", function () {
      const factory = createFactory();

      expect(() =>
        factory.create({
          type: "user.created",
          streamId: "user_123",
        }),
      ).to.throw("Envelope kind is required");
    });

    it("throws when type is missing", function () {
      const factory = createFactory();

      expect(() =>
        factory.create({
          kind: "event",
          streamId: "user_123",
        }),
      ).to.throw("Envelope type is required");
    });

    it("throws when streamId is missing", function () {
      const factory = createFactory();

      expect(() =>
        factory.create({
          kind: "event",
          type: "user.created",
        }),
      ).to.throw("Envelope streamId is required");
    });

    it("throws when source is missing from both meta and factory config", function () {
      const factory = createFactory({
        source: undefined,
      });

      expect(() => factory.create(coreEventMeta)).to.throw(
        "Envelope source is required",
      );
    });

    it("throws when data is null", function () {
      const factory = createFactory();

      expect(() => factory.create(coreEventMeta, null)).to.throw(
        "Envelope data must be an object",
      );
    });

    it("throws when data is an array", function () {
      const factory = createFactory();

      expect(() => factory.create(coreEventMeta, [])).to.throw(
        "Envelope data must be an object",
      );
    });

    it("throws when data is a primitive", function () {
      const factory = createFactory();

      expect(() => factory.create(coreEventMeta, "invalid-data")).to.throw(
        "Envelope data must be an object",
      );
    });
  });

  describe("default dependencies", function () {
    it("uses default id and clock dependencies when dependencies are not injected", function () {
      const factory = new EnvelopeFactory({
        source: "test-service",
      });

      const envelope = factory.createEvent(
        {
          type: coreEventMeta.type,
          streamId: coreEventMeta.streamId,
        },
        {},
      );

      expect(envelope.meta.id).to.be.a("string");
      expect(envelope.meta.correlationId).to.be.a("string");
      expect(envelope.meta.timestamp).to.be.a("string");
      expect(envelope.meta.source).to.equal("test-service");
      expect(envelope.meta.version).to.equal("1.0.0");
      expect(envelope.meta.kind).to.equal("event");
    });
  });
});
