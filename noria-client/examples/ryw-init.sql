USE inventory;
DROP TABLE IF EXISTS employees;
CREATE TABLE employees (
emp_no      INT             NOT NULL,
first_name  VARCHAR(14)     NOT NULL,
last_name   VARCHAR(16)     NOT NULL,
gender      VARCHAR(1)  NOT NULL,
PRIMARY KEY (emp_no)
);