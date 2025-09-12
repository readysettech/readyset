use criterion::{Criterion, black_box, criterion_group, criterion_main};
use readyset_sql::Dialect;
use readyset_sql_parsing::{ParsingPreset, parse_query_with_config};
use sqlparser::{dialect::MySqlDialect, parser::Parser};

fn parsing_comparison_mysql(c: &mut Criterion) {
    let selects = vec![
        ("select_basic", "SELECT id, name FROM users"),
        (
            "select_with_where",
            "SELECT * FROM products WHERE price > 100 AND category = 'electronics'",
        ),
        (
            "select_with_joins",
            "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
        ),
        (
            "select_with_grouping",
            "SELECT category, COUNT(*), AVG(price) FROM products GROUP BY category HAVING COUNT(*) > 10",
        ),
        // (
        //     "select_with_window",
        //     "SELECT name, salary, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) FROM employees",
        // ),
        ("expr_simple", "SELECT 1 + 2"),
        (
            "expr_function_call",
            "SELECT UPPER(CONCAT(first_name, ' ', last_name))",
        ),
        (
            "expr_case_when",
            "SELECT CASE WHEN age >= 18 THEN 'adult' ELSE 'minor' END",
        ),
        (
            "expr_complex_arithmetic",
            "SELECT (price * quantity * (1 + tax_rate)) - discount",
        ),
        (
            "expr_subquery",
            "SELECT EXISTS (SELECT 1 FROM orders WHERE user_id = u.id)",
        ),
        (
            "create_simple",
            "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))",
        ),
        (
            "create_with_constraints",
            "CREATE TABLE orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                total DECIMAL(10,2) DEFAULT 0.00,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id),
                INDEX idx_created_at (created_at)
            )",
        ),
        (
            "create_with_complex_types",
            "CREATE TABLE products (
                id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                description TEXT,
                metadata JSON,
                price DECIMAL(12,4) NOT NULL,
                stock INT UNSIGNED DEFAULT 0,
                -- not supported by nom-sql: tags SET('new', 'sale', 'featured', 'discontinued'),
                category ENUM('electronics', 'clothing', 'books', 'home'),
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uk_name (name),
                KEY idx_category_price (category, price),
                FULLTEXT KEY ft_description (description)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
        ),
    ];

    for (name, sql) in selects {
        c.benchmark_group(format!("parsing_{}", name))
            .bench_function("nom", |b| {
                b.iter(|| {
                    black_box(
                        parse_query_with_config(ParsingPreset::OnlyNom, Dialect::MySQL, sql)
                            .unwrap(),
                    )
                })
            })
            .bench_function("sqlparser", |b| {
                b.iter(|| {
                    black_box(
                        parse_query_with_config(ParsingPreset::OnlySqlparser, Dialect::MySQL, sql)
                            .unwrap(),
                    )
                })
            })
            .bench_function("sqlparser_without_ast_conversion", |b| {
                b.iter(|| {
                    black_box(
                        Parser::new(&MySqlDialect {})
                            .try_with_sql(sql)
                            .unwrap()
                            .parse_statement()
                            .unwrap(),
                    )
                })
            });
    }
}

fn parsing_overhead_both_parsers_mysql(c: &mut Criterion) {
    let sql = "SELECT customer_id, amount FROM payment WHERE customer_id = 1";

    c.benchmark_group("parsing_overhead")
        .bench_function("nom_only", |b| {
            b.iter(|| {
                black_box(
                    parse_query_with_config(ParsingPreset::OnlyNom, Dialect::MySQL, sql).unwrap(),
                )
            })
        })
        .bench_function("sqlparser_only", |b| {
            b.iter(|| {
                black_box(
                    parse_query_with_config(ParsingPreset::OnlySqlparser, Dialect::MySQL, sql)
                        .unwrap(),
                )
            })
        })
        .bench_function("both_prefer_nom", |b| {
            b.iter(|| {
                black_box(
                    parse_query_with_config(ParsingPreset::BothPreferNom, Dialect::MySQL, sql)
                        .unwrap(),
                )
            })
        })
        .bench_function("both_prefer_sqlparser", |b| {
            b.iter(|| {
                black_box(
                    parse_query_with_config(
                        ParsingPreset::BothPreferSqlparser,
                        Dialect::MySQL,
                        sql,
                    )
                    .unwrap(),
                )
            })
        })
        .bench_function("both_panic_on_mismatch", |b| {
            b.iter(|| {
                black_box(
                    parse_query_with_config(
                        ParsingPreset::BothPanicOnMismatch,
                        Dialect::MySQL,
                        sql,
                    )
                    .unwrap(),
                )
            })
        })
        .bench_function("both_error_on_mismatch", |b| {
            b.iter(|| {
                black_box(
                    parse_query_with_config(
                        ParsingPreset::BothErrorOnMismatch,
                        Dialect::MySQL,
                        sql,
                    )
                    .unwrap(),
                )
            })
        });
}

fn large_query_parsing_mysql(c: &mut Criterion) {
    let large_query = format!(
        "SELECT {} FROM large_table t1 {} WHERE {} ORDER BY {} LIMIT 1000",
        (1..=50)
            .map(|i| format!("t1.col{}", i))
            .collect::<Vec<_>>()
            .join(", "),
        (2..=10)
            .map(|i| format!("LEFT JOIN table{} t{} ON t1.id = t{}.foreign_id", i, i, i))
            .collect::<Vec<_>>()
            .join(" "),
        (1..=20)
            .map(|i| format!("t1.col{} = ?", i))
            .collect::<Vec<_>>()
            .join(" AND "),
        (1..=5)
            .map(|i| format!("t1.col{} DESC", i))
            .collect::<Vec<_>>()
            .join(", ")
    );

    c.benchmark_group("large_query_parsing")
        .bench_function("nom", |b| {
            b.iter(|| {
                black_box(
                    parse_query_with_config(ParsingPreset::OnlyNom, Dialect::MySQL, &large_query)
                        .unwrap(),
                )
            })
        })
        .bench_function("sqlparser", |b| {
            b.iter(|| {
                black_box(
                    parse_query_with_config(
                        ParsingPreset::OnlySqlparser,
                        Dialect::MySQL,
                        &large_query,
                    )
                    .unwrap(),
                )
            })
        })
        .bench_function("sqlparser_without_ast_conversion", |b| {
            b.iter(|| {
                black_box(
                    Parser::new(&MySqlDialect {})
                        .try_with_sql(&large_query)
                        .unwrap()
                        .parse_statement()
                        .unwrap(),
                )
            })
        });
}

criterion_group!(
    benches,
    parsing_comparison_mysql,
    parsing_overhead_both_parsers_mysql,
    large_query_parsing_mysql,
);
criterion_main!(benches);
