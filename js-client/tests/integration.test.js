const { Client } = require("../lib/index");
const process = require("process");

const CONFIG = {
  deployment: process.env.NORIA_DEPLOYMENT,
  zookeeperAddress: process.env.ZOOKEEPER_ADDRESS,
};

const EMPLOYEES = [
  {
    emp_no: 10001,
    first_name: "first1",
    last_name: "last1",
    birthday: "2015-09-05 23:56:04",
  },
  {
    emp_no: 10002,
    first_name: "first2",
    last_name: "last2",
    birthday: "2015-09-05 23:56:04",
  },
  {
    emp_no: 10003,
    first_name: "first3",
    last_name: "last3",
    birthday: "2015-09-05 23:56:04",
  },
  {
    emp_no: 10004,
    first_name: "first4",
    last_name: "last4",
    birthday: "2015-09-05 23:56:04",
  },
  {
    emp_no: 10005,
    first_name: "first5",
    last_name: "last5",
    birthday: "2015-09-05 23:56:04",
  },
];

function createEmployeesTable() {
  const client = new Client(CONFIG);
  client.query(
    "CREATE TABLE employees (emp_no INT PRIMARY KEY, first_name TEXT, last_name TEXT, birthday TIMESTAMP)"
  );
}

async function fillEmployees() {
  const client = new Client(CONFIG);
  return EMPLOYEES.forEach(
    async (e) =>
      await client.query(
        `INSERT INTO employees (emp_no, first_name, last_name, birthday) VALUES (${e.emp_no}, "${e.first_name}", "${e.last_name}", "${e.birthday}")`
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
  const d = new Date("2015-09-05 23:56:04");
  const client = new Client(CONFIG);
  const res = await client.query(
    "SELECT emp_no, first_name, birthday FROM employees WHERE emp_no = 10001"
  );
  expect(res).toEqual({
    data: [
      {
        emp_no: 10001,
        first_name: "first1",
        birthday: d,
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

  const checkSelectRes = await client.query(
    "SELECT emp_no, first_name FROM employees WHERE emp_no = 10001"
  );
  expect(checkSelectRes).toEqual({
    data: [
      {
        emp_no: 10001,
        first_name: "TESTING AD-HOC",
      },
    ],
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
  const updateRes = await client.execute(
    "UPDATE employees SET first_name = ? WHERE emp_no = ?",
    ["TESTING EXEC", 10001]
  );
  expect(updateRes).toEqual({
    numRowsUpdated: 1,
    lastInsertedId: 0,
  });

  const checkSelectRes = await client.query(
    "SELECT emp_no, first_name FROM employees WHERE emp_no = 10001"
  );
  expect(checkSelectRes).toEqual({
    data: [
      {
        emp_no: 10001,
        first_name: "TESTING EXEC",
      },
    ],
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

test("Prepared UPDATE with Date", async () => {
  const d = new Date("December 16, 1998 03:24:00");
  const client = new Client(CONFIG);
  const updateRes = await client.execute(
    "UPDATE employees SET birthday = ? WHERE emp_no = ?",
    [d, 10001]
  );
  expect(updateRes).toEqual({
    numRowsUpdated: 1,
    lastInsertedId: 0,
  });

  const checkSelectRes = await client.query(
    "SELECT emp_no, birthday FROM employees WHERE emp_no = 10001"
  );
  expect(checkSelectRes).toEqual({
    data: [
      {
        emp_no: 10001,
        birthday: d,
      },
    ],
  });
});
