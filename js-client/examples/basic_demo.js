var rs = require('../lib/index.js');

config = {
    deployment: "myapp",
    zookeeperAddress: "127.0.0.1:2181",
    sanitize: true
}
var client = new rs.Client(config); // use environment variables instead?

async function test_select() {
    var r = await client.query("select * from employees;");
    console.log(JSON.stringify(r, undefined, 2));
}

async function test_prep_select() {
    var r = await client.prepare("select * from employees where first_name = ?;");
    console.log(JSON.stringify(r, undefined, 2));
}

async function test_prep_update() {
    var r = await client.prepare("update employees set first_name = ? where last_name = ?;");
    console.log(JSON.stringify(r, undefined, 2));
}

async function test_prep_insert() {
    var r = await client.prepare("insert into employees (emp_no, first_name, last_name, gender) values (?, ?, ?, ?);");
    console.log(JSON.stringify(r, undefined, 2));
}

test_select()