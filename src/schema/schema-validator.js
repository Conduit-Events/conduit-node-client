import Ajv from "ajv";
import addFormats from "ajv-formats";
import protocolSchema from "./protocol/conduit-message.schema.json" assert { type: "json" };

export class SchemaValidator {
  #ajv;
  #validators;

  constructor(schemas = {}) {
    console.log("SCHEMAS", schemas);
    this.#validators = {};
    this.#ajv = new Ajv();
    addFormats(this.#ajv);
    this.addSchema("message", protocolSchema);
    const aliases = [];

    for (const [type, schema] of Object.entries(schemas)) {
      if (typeof schema === "string") {
        aliases.push([type, schema]);
      } else {
        this.addSchema(type, schema);
      }
    }

    for (const [aliasType, targetType] of aliases) {
      this.alias(aliasType, targetType);
    }
    if (!this.#validators.event) this.alias("event", "message");
    if (!this.#validators.command) this.alias("command", "message");
  }

  addSchema(type, schema) {
    this.#validators[type] = this.#ajv.compile(schema);
  }

  alias(aliasType, targetType) {
    const target = this.#validators[targetType];
    if (!target) {
      throw new Error(`Unknown schema alias target: ${targetType}`);
    }
    this.#validators[aliasType] = target;
  }

  validateEnvelope(data) {
    return this.#validateAs(data, data?.meta?.kind ?? "message");
  }

  validatePayload(payload) {
    const type = payload?.meta?.type;

    if (!type || !this.#validators[type]) {
      return true; // no payload schema registered yet
    }

    return this.#validateAs(payload.data, type);
  }

  validateMessage(message) {
    this.validateEnvelope(message);
    this.validatePayload(message);
    return true;
  }

  isValid(data, type = "message") {
    const validator = this.#validators[type];
    if (!validator) return false;
    return validator(data);
  }

  validate(data, type = "message") {
    console.log("VALIDATING");
    if (type === "message") return this.validateMessage(data);
    return this.#validateAs(data, type);
  }

  #validateAs(data, type) {
    const validator = this.#validators[type];

    if (!validator) {
      throw new Error(`Unknown schema type: ${type}`);
    }

    const valid = validator(data);

    if (!valid) {
      throw new Error(`Invalid ${type}`, validator.errors);
    }

    return true;
  }
}
