import { should } from "chai";
import { Client } from "../../../index.js";
describe("Client", function () {
  should();
  it("Creates a client", function () {
    let client1 = new Client();
    client1.should.be.ok;
    let client2 = Client.create();
    client2.should.be.ok;
    client1.should.not.equal(client2);
    let config3 = { config: 3 };
    let client3 = new Client(config3);
    client3._config.should.equal(config3);
    let config4 = { config: 4 };
    let client4 = Client.create(config4);
    client4._config.should.equal(config4);
  });
});
