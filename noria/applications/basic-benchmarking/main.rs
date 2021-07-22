#![warn(clippy::dbg_macro)]
use clap::{App, Arg};
use mysql::prelude::*;
use mysql::*;
use noria::consensus::LocalAuthority;
use noria::{Builder, DataType, Handle};
use rand::Rng;
use std::convert::TryFrom;
use std::result;
use std::time;
use vec1::vec1;

#[tokio::main]
async fn main() {
    let args = App::new("noria-basic-benchmarking")
        .version("0.1")
        .about("Benchmarks noria reads")
        .arg(
            Arg::with_name("ntests")
                .short("n")
                .default_value("1000")
                .help("Number of employees to query for benchmarking."),
        )
        .arg(
            Arg::with_name("nemployees")
                .short("e")
                .default_value("50000")
                .help("Total number of employees to populate tables with"),
        )
        .get_matches();

    println!("Starting benchmark...");

    let ntests = args.value_of("ntests").unwrap().parse::<i32>().unwrap();
    let nemployees = args.value_of("nemployees").unwrap().parse::<i32>().unwrap();

    let mut benchmark_application = BenchmarkApplication::new().await;
    benchmark_application
        .install_schemas_and_queries()
        .await
        .unwrap();
    benchmark_application.load_data(nemployees).await.unwrap();
    benchmark_application
        .run_benchmarks(ntests, nemployees)
        .await
        .unwrap();
}

pub struct BenchmarkApplication {
    g: Handle<LocalAuthority>,
    mysql: PooledConn,
}

impl BenchmarkApplication {
    pub async fn new() -> BenchmarkApplication {
        let mut builder = Builder::default();
        builder.disable_partial();
        let g = builder.start_local().await.unwrap();

        let url = "mysql://mysqluser:mysqlpw@localhost:3306/inventory";
        let pool = Pool::new(url).unwrap();
        let conn = pool.get_conn().unwrap();

        BenchmarkApplication { g, mysql: conn }
    }

    pub async fn run_benchmarks(
        &mut self,
        ntests: i32,
        nemployees: i32,
    ) -> result::Result<(), String> {
        println!("Starting mysql queries");
        let mut employee_ids_to_query = Vec::new();
        let mut rng = rand::thread_rng();
        for _i in 0..ntests {
            employee_ids_to_query.push(rng.gen_range(0, nemployees));
        }

        let mut noria_employee_count = 0;
        let mut noria_manager_count = 0;
        let mut noria_dept_emp_count = 0;
        let mut noria_salary_count = 0;

        let mut mysql_employee_count = 0;
        let mut mysql_manager_count = 0;
        let mut mysql_dept_emp_count = 0;
        let mut mysql_salary_count = 0;

        let mysql_start = time::Instant::now();
        for i in &employee_ids_to_query {
            let employee_string = format!("SELECT * FROM employees WHERE emp_no={}", i.to_string());
            let dept_manager_string =
                format!("SELECT * FROM dept_manager WHERE emp_no={}", i.to_string());
            let dept_emp_string = format!("SELECT * FROM dept_emp WHERE emp_no={}", i.to_string());
            let salary_string = format!("SELECT * FROM salaries WHERE emp_no={}", i.to_string());

            mysql_employee_count += self
                .mysql
                .query_map(employee_string, |(emp_no, age, gender)| Employee {
                    emp_no,
                    age,
                    gender,
                })
                .unwrap()
                .len();
            mysql_manager_count += self
                .mysql
                .query_map(dept_manager_string, |(emp_no, dept_no, id)| DeptManager {
                    emp_no,
                    dept_no,
                    id,
                })
                .unwrap()
                .len();
            mysql_dept_emp_count += self
                .mysql
                .query_map(dept_emp_string, |(emp_no, dept_no, id)| DeptEmp {
                    emp_no,
                    dept_no,
                    id,
                })
                .unwrap()
                .len();
            mysql_salary_count += self
                .mysql
                .query_map(salary_string, |(emp_no, salary, id)| Salary {
                    emp_no,
                    salary,
                    id,
                })
                .unwrap()
                .len();
        }
        let mysql_dur = mysql_start.elapsed().as_secs_f64();

        println!("\nMysql found {:?} employees", mysql_employee_count);
        println!("Mysql found {:?} dept_manager", mysql_manager_count);
        println!("Mysql found {:?} dept_emp", mysql_dept_emp_count);
        println!("Mysql found {:?} salaries", mysql_salary_count);

        let mut emp_view = self.g.view("select_employee").await.unwrap();
        let mut manager_view = self.g.view("employee_dept_manager").await.unwrap();
        let mut dept_emp_view = self.g.view("employee_dept").await.unwrap();
        let mut salary_view = self.g.view("employee_salary_history").await.unwrap();

        let noria_warmup_start = time::Instant::now();
        for i in employee_ids_to_query.clone() {
            noria_employee_count += emp_view
                .lookup(&[i.into()], true)
                .await
                .unwrap()
                .iter()
                .len();
            noria_dept_emp_count += dept_emp_view
                .lookup(&[i.into()], true)
                .await
                .unwrap()
                .iter()
                .len();
            noria_manager_count += manager_view
                .lookup(&[i.into()], true)
                .await
                .unwrap()
                .iter()
                .len();
            noria_salary_count += salary_view
                .lookup(&[i.into()], true)
                .await
                .unwrap()
                .iter()
                .len();
        }
        let noria_warmup_dur = noria_warmup_start.elapsed().as_secs_f64();

        let noria_start = time::Instant::now();
        for i in employee_ids_to_query.clone() {
            emp_view.lookup(&[i.into()], false).await.unwrap();
            dept_emp_view.lookup(&[i.into()], false).await.unwrap();
            manager_view.lookup(&[i.into()], false).await.unwrap();
            salary_view.lookup(&[i.into()], false).await.unwrap();
        }
        let noria_dur = noria_start.elapsed().as_secs_f64();

        let noria_smart_start = time::Instant::now();
        let noria_lookup: Vec<_> = employee_ids_to_query
            .iter()
            .map(|e_id| vec1![(*e_id).into()].into())
            .collect();
        emp_view
            .multi_lookup(noria_lookup.clone(), true)
            .await
            .unwrap();
        dept_emp_view
            .multi_lookup(noria_lookup.clone(), true)
            .await
            .unwrap();
        manager_view
            .multi_lookup(noria_lookup.clone(), true)
            .await
            .unwrap();
        salary_view
            .multi_lookup(noria_lookup.clone(), true)
            .await
            .unwrap();
        let noria_smart_dur = noria_smart_start.elapsed().as_secs_f64();

        println!("Noria found {:?} employees", noria_employee_count);
        println!("Noria found {:?} dept_manager", noria_manager_count);
        println!("Noria found {:?} dept_emp", noria_dept_emp_count);
        println!("Noria found {:?} salaries\n", noria_salary_count);

        assert!(
            mysql_employee_count == noria_employee_count,
            "Found incorrect number of employees"
        );
        assert!(
            mysql_dept_emp_count == noria_dept_emp_count,
            "Found incorrect number of dept_emp"
        );
        assert!(
            mysql_manager_count == noria_manager_count,
            "Found incorrect number of managers"
        );
        assert!(
            mysql_salary_count == noria_salary_count,
            "Found incorrect number of salaries\n"
        );
        println!("All finished. Results match up.\n");

        println!("MySQL time: {:?}", mysql_dur);
        println!("Noria warmup time : {:?}", noria_warmup_dur);
        println!("Noria time : {:?}", noria_dur);
        println!("Noria smart time : {:?}", noria_smart_dur);

        Ok(())
    }

    pub async fn install_schemas_and_queries(&mut self) -> result::Result<(), String> {
        let noria_sql_string = "
            CREATE TABLE employees (
                `emp_no`      INT             NOT NULL,
                `age`         INT             NOT NULL,
                `gender`      VARCHAR(1)  NOT NULL,
                PRIMARY KEY (emp_no)
            );

            CREATE TABLE dept_manager (
            `emp_no`       INT             NOT NULL,
            `dept_no`      INT             NOT NULL,
            `id`           INT             NOT NULL,
            PRIMARY KEY (id)
            );

            CREATE TABLE dept_emp (
                `emp_no`      INT             NOT NULL,
                `dept_no`     INT             NOT NULL,
                `id`           INT             NOT NULL,
                PRIMARY KEY (id)
            );

            CREATE TABLE salaries (
                `emp_no`      INT             NOT NULL,
                `salary`      INT             NOT NULL,
                `id`           INT            NOT NULL,
                PRIMARY KEY (id)
            );

            QUERY select_employee: SELECT * FROM employees WHERE emp_no=?;
            QUERY employee_dept: SELECT * FROM dept_emp WHERE emp_no=?;
            QUERY employee_dept_manager: SELECT * FROM dept_manager WHERE emp_no=?;
            QUERY employee_salary_history: SELECT * FROM salaries WHERE emp_no=?;
        ";

        println!("Initializing noria tables and queries.");
        self.g.install_recipe(noria_sql_string).await.unwrap();

        println!("Attempting to drop all mysql tables in case they already exist.");
        self.mysql
            .query_drop(
                r"DROP TABLE employees;
        DROP TABLE dept_manager;
        DROP TABLE dept_emp;
        DROP TABLE salaries;",
            )
            .unwrap();

        println!("Initializing all mysql tables.");
        self.mysql
            .query_drop(
                r"
            CREATE TABLE employees (
                `emp_no`      INT             NOT NULL,
                `age`         INT             NOT NULL,
                `gender`      VARCHAR(1)  NOT NULL,
                PRIMARY KEY (emp_no)
            );

            CREATE TABLE dept_manager (
            `emp_no`       INT             NOT NULL,
            `dept_no`      INT             NOT NULL,
            `id`           INT             NOT NULL,
            PRIMARY KEY (id)
            );

            CREATE TABLE dept_emp (
                `emp_no`      INT             NOT NULL,
                `dept_no`     INT             NOT NULL,
                `id`           INT             NOT NULL,
                PRIMARY KEY (id)
            );

            CREATE TABLE salaries (
                `emp_no`      INT             NOT NULL,
                `salary`      INT             NOT NULL,
                `id`           INT            NOT NULL,
                PRIMARY KEY (id)
            );
        ",
            )
            .unwrap();
        Ok(())
    }

    pub async fn load_data(&mut self, n_employees: i32) -> result::Result<(), String> {
        println!("Populating employees table");
        let mut employees: Vec<Vec<DataType>> = Vec::new();
        let mut employees_mysql = Vec::new();
        let mut rng = rand::thread_rng();
        for i in 1..n_employees {
            let age = rng.gen_range(20, 80);
            let gender = match rng.gen_range(0, 2) {
                0 => "M",
                1 => "F",
                _ => "G",
            };
            employees.push(vec![
                i.into(),
                age.into(),
                DataType::try_from(gender).unwrap(),
            ]);
            employees_mysql.push(Employee {
                emp_no: i,
                age,
                gender: gender.to_string(),
            });
        }

        let mut employees_mutator = self.g.table("employees").await.unwrap();
        employees_mutator.i_promise_dst_is_same_process();
        employees_mutator
            .perform_all(employees.clone())
            .await
            .unwrap();
        self.mysql
            .exec_batch(
                r"INSERT INTO employees (emp_no, age, gender)
              VALUES (:emp_no, :age, :gender)",
                employees_mysql.iter().map(|d| {
                    params! {
                        "emp_no" => d.emp_no,
                        "age" => d.age,
                        "gender" => d.gender.clone()
                    }
                }),
            )
            .unwrap();

        //decide how many departments there are
        let num_departments = n_employees / 50;

        let num_managers = num_departments * 5;

        println!("Populating dept_managers table");
        let mut dep_managers: Vec<Vec<DataType>> = Vec::new();
        let mut dep_managers_mysql = Vec::new();
        for i in 1..num_managers {
            let manager = rng.gen_range(0, n_employees);
            let department = rng.gen_range(0, num_departments);
            dep_managers.push(vec![manager.into(), department.into(), i.into()]);
            dep_managers_mysql.push(DeptManager {
                emp_no: manager,
                dept_no: department,
                id: i,
            });
        }
        let mut manager_mutator = self.g.table("dept_manager").await.unwrap();
        manager_mutator.i_promise_dst_is_same_process();
        manager_mutator
            .perform_all(dep_managers.clone())
            .await
            .unwrap();
        self.mysql
            .exec_batch(
                r"INSERT INTO dept_manager (emp_no, dept_no, id)
              VALUES (:emp_no, :dept_no, :id)",
                dep_managers_mysql.iter().map(|d| {
                    params! {
                        "emp_no" => d.emp_no,
                        "dept_no" => d.dept_no,
                        "id" => d.id
                    }
                }),
            )
            .unwrap();

        println!("Populating dept_emp table");
        let mut dep_employees: Vec<Vec<DataType>> = Vec::new();
        let mut dep_employees_mysql = Vec::new();
        for i in 1..n_employees {
            let department = rng.gen_range(0, num_departments);
            dep_employees.push(vec![i.into(), department.into(), i.into()]);
            dep_employees_mysql.push(DeptEmp {
                emp_no: i,
                dept_no: department,
                id: i,
            });
        }
        let mut dep_emp_mutator = self.g.table("dept_emp").await.unwrap();
        dep_emp_mutator.i_promise_dst_is_same_process();
        dep_emp_mutator
            .perform_all(dep_employees.clone())
            .await
            .unwrap();
        self.mysql
            .exec_batch(
                r"INSERT INTO dept_emp (emp_no, dept_no, id)
              VALUES (:emp_no, :dept_no, :id)",
                dep_employees_mysql.iter().map(|d| {
                    params! {
                        "emp_no" => d.emp_no,
                        "dept_no" => d.dept_no,
                        "id" => d.id
                    }
                }),
            )
            .unwrap();

        println!("Populating salaries table");
        let mut salaries: Vec<Vec<DataType>> = Vec::new();
        let mut salaries_mysql = Vec::new();
        for i in 1..n_employees * 3 {
            let emp = rng.gen_range(0, n_employees);
            let salary = rng.gen_range(0, 400000);
            salaries.push(vec![emp.into(), salary.into(), i.into()]);
            salaries_mysql.push(Salary {
                emp_no: emp,
                salary,
                id: i,
            });
        }

        let mut salary_mutator = self.g.table("salaries").await.unwrap();
        salary_mutator.i_promise_dst_is_same_process();
        salary_mutator.perform_all(salaries.clone()).await.unwrap();
        self.mysql
            .exec_batch(
                r"INSERT INTO salaries (emp_no, salary, id)
              VALUES (:emp_no, :salary, :id)",
                salaries_mysql.iter().map(|d| {
                    params! {
                        "emp_no" => d.emp_no,
                        "salary" => d.salary,
                        "id" => d.id
                    }
                }),
            )
            .unwrap();
        Ok(())
    }
}

struct Employee {
    emp_no: i32,
    age: i32,
    gender: String,
}

struct DeptManager {
    emp_no: i32,
    dept_no: i32,
    id: i32,
}

struct DeptEmp {
    emp_no: i32,
    dept_no: i32,
    id: i32,
}

struct Salary {
    emp_no: i32,
    salary: i32,
    id: i32,
}
