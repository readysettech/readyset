const rs = require("../lib/index.js");

config = {
  deployment: "myapp",
  zookeeperAddress: "127.0.0.1:2181",
  sanitize: true,
};
const client = new rs.Client(config);

async function test_select() {
  const r = await client.query("select * from employees;");
  console.log(JSON.stringify(r, undefined, 2));
}

async function test_update() {
  const r = await client.query(
    'update employees set first_name = "TESTING AD-HOC" where emp_no = 10001;'
  );
  console.log(JSON.stringify(r, undefined, 2));
}

async function test_exec_select() {
  const r = await client.execute("select * from employees where emp_no = ?;", [
    10001,
  ]);
  console.log(JSON.stringify(r, undefined, 2));
}

async function test_exec_update() {
  const r = await client.execute(
    "update employees set first_name = ? where emp_no = ?;",
    ["TESTING EXEC", 10001]
  );
  console.log(JSON.stringify(r, undefined, 2));
}

async function test_select_err() {
  try {
    await client.query("bLaHblAhBlaH");
  } catch (e) {
    console.log(e);
  }
}

async function test_prep_cache() {
  var r = await client.execute("select * from employees where emp_no = ?;", [
    10001,
  ]);
  console.log(JSON.stringify(r, undefined, 2));
  r = await client.execute("select * from employees where emp_no = ?;", [
    10001,
  ]);
  console.log(JSON.stringify(r, undefined, 2));
}

test_prep_cache();
