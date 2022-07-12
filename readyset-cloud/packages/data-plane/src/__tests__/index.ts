import * as path from "path";
import { Repository } from "..";

describe("Data Plane", () => {
  it("can create a repository from fixtures", () => {
    const repository = new Repository(
      path.resolve(__dirname, "fixtures/customers"),
      path.resolve(__dirname, "fixtures/sops/age-test-key.txt")
    );
    const customers = repository.getCustomers();
    expect(customers.map((c) => c.id)).toStrictEqual(
      expect.arrayContaining(["test-0"])
    );
  });
});
