const { Client } = require("../lib/index");
const process = require("process");

const CONFIG = {
  deployment: process.env.DEPLOYMENT,
  zookeeperAddress: process.env.ZOOKEEPER_HOST + ":2181",
};

const EMPLOYEES = [
  {
    emp_no: 10001,
    first_name: "first1",
    last_name: "last1",
  },
  {
    emp_no: 10002,
    first_name: "first2",
    last_name: "last2",
  },
  {
    emp_no: 10003,
    first_name: "first3",
    last_name: "last3",
  },
  {
    emp_no: 10004,
    first_name: "first4",
    last_name: "last4",
  },
  {
    emp_no: 10005,
    first_name: "first5",
    last_name: "last5",
  },
];

function createEmployeesTable() {
  const client = new Client(CONFIG);
  client.query(
    "CREATE TABLE employees (emp_no INT PRIMARY KEY, first_name TEXT, last_name TEXT)"
  );
}

async function fillEmployees() {
  const client = new Client(CONFIG);
  return EMPLOYEES.forEach(
    async (e) =>
      await client.query(
        `INSERT INTO employees (emp_no, first_name, last_name) VALUES (${e.emp_no}, "${e.first_name}", "${e.last_name}")`
      )
  );
}

async function clearEmployees() {
  const client = new Client(CONFIG);
  return EMPLOYEES.forEach(
    async (e) =>
      await client.query(`DELETE FROM employees WHERE emp_no = ${e.emp_no}`) // note: noria only supports DELETE with WHERE clause at the moment
  );
}

beforeAll(() => {
  return createEmployeesTable();
});

beforeEach(() => {
  return fillEmployees();
});

afterEach(() => {
  return clearEmployees();
});

test("Create client", () => {
  const client = new Client(CONFIG);
});

test("Ad-hoc SELECT", async () => {
  const client = new Client(CONFIG);
  const res = await client.query(
    "SELECT emp_no, first_name FROM employees WHERE emp_no = 10001"
  );
  expect(res).toEqual({
    data: [
      {
        emp_no: 10001,
        first_name: "first1",
      },
    ],
  });
});

test("Ad-hoc UPDATE", async () => {
  const client = new Client(CONFIG);
  const res = await client.query(
    'UPDATE employees SET first_name = "TESTING AD-HOC" WHERE emp_no = 10001'
  );
  expect(res).toEqual({
    numRowsUpdated: 1,
    lastInsertedId: 0,
  });
});

test("Prepared SELECT no params", async () => {
  const client = new Client(CONFIG);
  const res = await client.execute(
    "SELECT emp_no, first_name FROM employees WHERE emp_no = 10001",
    []
  );
  expect(res).toEqual({
    data: [
      {
        emp_no: 10001,
        first_name: "first1",
      },
    ],
  });
});

test("Prepared SELECT 1 param", async () => {
  const client = new Client(CONFIG);
  const res = await client.execute(
    "SELECT emp_no, first_name FROM employees WHERE emp_no = ?",
    [10001]
  );
  expect(res).toEqual({
    data: [
      {
        emp_no: 10001,
        first_name: "first1",
      },
    ],
  });
});

test("Prepared UPDATE 2 params", async () => {
  const client = new Client(CONFIG);
  const res = await client.execute(
    "UPDATE employees SET first_name = ? WHERE emp_no = ?",
    ["TESTING EXEC", 10001]
  );
  expect(res).toEqual({
    numRowsUpdated: 1,
    lastInsertedId: 0,
  });
});

test("Parse error", async () => {
  const client = new Client(CONFIG);
  await expect(client.query("bLaHblAhBlaH")).rejects.toThrow(
    "Query failed to parse: bLaHblAhBlaH"
  );
});

test("Prepared query using the cache", async () => {
  const client = new Client(CONFIG);

  const res1 = await client.execute(
    "SELECT emp_no FROM employees WHERE emp_no = ?",
    [10001]
  );
  expect(res1).toEqual({
    data: [
      {
        emp_no: 10001,
      },
    ],
  });
  var expected_prep_cache = new Map();
  expected_prep_cache.set("SELECT emp_no FROM employees WHERE emp_no = ?", 1);
  expect(client.prep_cache).toEqual(expected_prep_cache);

  const res2 = await client.execute(
    "SELECT emp_no FROM employees WHERE emp_no = ?",
    [10002]
  );
  expect(res2).toEqual({
    data: [
      {
        emp_no: 10002,
      },
    ],
  });
  expect(client.prep_cache).toEqual(expected_prep_cache);
});
