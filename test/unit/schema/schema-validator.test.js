import { should } from "chai";
import { SchemaValidator } from "../../../src/schema/schema-validator.js";
should();

const goodEventMessage = {
  meta: {
    id: "evt_01JYH7Z7K8N9Q2M3R4S5T6V7W8",
    kind: "event",
    type: "user.created",
    version: "1.0.0",
    streamId: "user_123",
    correlationId: "corr_01JYH7Z7K8N9Q2M3R4S5T6V7W8",
    timestamp: "2026-06-24T19:30:00.000Z",
    source: "test-suite",
  },
  data: {
    userId: "user_123",
    email: "janx@example.com",
    name: "Janx",
  },
};
const badEventMessage = { meta: { ...goodEventMessage.meta } };
const goodCommandMessage = {
  meta: {
    id: "cmd_01JYH80A1B2C3D4E5F6G7H8J9K",
    kind: "command",
    type: "user.create",
    version: "1.0.0",
    streamId: "user_123",
    correlationId: "corr_01JYH80A1B2C3D4E5F6G7H8J9K",
    timestamp: "2026-06-24T19:31:00.000Z",
    source: "test-suite",
  },
  data: {
    email: "janx@example.com",
    name: "Janx",
  },
};

const goodMessage = goodEventMessage;
describe("SchemaValidator", function () {
  let validator;

  beforeEach(function () {
    validator = new SchemaValidator();
  });

  it("validates a valid message", function () {
    validator.validate(goodMessage).should.be.true;
  });

  it("validates an event using the default event alias", function () {
    validator.validate(goodEventMessage, "event").should.be.true;
  });

  it("validates a command using the default command alias", function () {
    validator.validate(goodCommandMessage, "command").should.be.true;
  });

  it("throws for an invalid message", function () {
    (() => validator.validate({})).should.throw("Invalid message");
  });

  it("throws when required data is missing", function () {
    (() => validator.validate(badEventMessage)).should.throw("Invalid event");
  });

  it("allows custom schemas", function () {
    const customValidator = new SchemaValidator({
      test: {
        type: "object",
        required: ["flag"],
        additionalProperties: true,
        properties: {
          flag: { type: "boolean" },
          str: { type: "string" },
        },
      },
    });

    customValidator.validate(
      { flag: false, str: "string", additional: "string" },
      "test",
    ).should.be.true;
  });

  it("throws when custom schema validation fails", function () {
    const customValidator = new SchemaValidator({
      test: {
        type: "object",
        required: ["flag"],
        properties: {
          flag: { type: "boolean" },
          str: { type: "string" },
        },
      },
    });

    (() =>
      customValidator.validate({ flag: false, str: 23 }, "test")).should.throw(
      "Invalid test",
    );
  });
});
