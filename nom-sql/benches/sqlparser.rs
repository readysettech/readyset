// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use criterion::{criterion_group, criterion_main, Criterion};
use nom_sql::{Dialect, parse_select_statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

const SIMPLE_QUERY: &str = "SELECT * FROM table WHERE 1 = 1";
const WITH_QUERY: &str = "
    WITH derived AS (
        SELECT MAX(a) AS max_a,
               COUNT(b) AS b_num,
               user_id
        FROM TABLE
        GROUP BY user_id
    )
    SELECT * FROM table
    LEFT JOIN derived USING (user_id)
";
const COMPLEX_QUERY: &str = r#"
WITH EmployeeTotals AS (
  SELECT 
    department_id, 
    SUM(salary) AS total_salary 
  FROM employees 
  GROUP BY department_id
)
, RankByAge AS (
  SELECT 
    id,
    RANK() OVER (PARTITION BY department_id ORDER BY age DESC) AS age_rank
  FROM employees
)
SELECT 
  A.id,
  CONCAT(A.name, '-', B.name) AS merged_name,
  CASE 
    WHEN A.salary > 50000 THEN 'High'
    ELSE 'Low'
  END AS salary_range,
  (SELECT AVG(salary) FROM employees WHERE department_id = A.department_id) AS avg_department_salary,
  COUNT(DISTINCT C.project_id),
  E.total_salary,
  F.age_rank,
  COALESCE(G.max_spent, 0) AS max_spent,
  STRING_AGG(D.project_name, ', ') AS project_names
FROM 
  employees A
LEFT JOIN 
  departments B ON A.department_id = B.id
INNER JOIN 
  employee_projects C ON A.id = C.employee_id
LEFT JOIN 
  projects D ON C.project_id = D.id
INNER JOIN 
  EmployeeTotals E ON A.department_id = E.department_id
INNER JOIN 
  RankByAge F ON A.id = F.id
LEFT JOIN (
  SELECT employee_id, MAX(spent) AS max_spent
  FROM expenses
  GROUP BY employee_id
) G ON A.id = G.employee_id
WHERE 
  A.age BETWEEN 30 AND 50
  AND B.location IN ('New York', 'San Francisco')
  AND A.id NOT IN (SELECT id FROM terminated_employees)
GROUP BY 
  A.id, B.name, E.total_salary, F.age_rank
HAVING 
  COUNT(DISTINCT C.project_id) > 2
  AND avg_department_salary > 60000
ORDER BY 
  F.age_rank, E.total_salary DESC
LIMIT 10 OFFSET 5
UNION ALL
SELECT 
  id, 
  'Unknown', 
  'Unknown', 
  0, 
  0, 
  0, 
  0, 
  0, 
  'Unknown'
FROM 
  terminated_employees;
"#;
const LARGE_QUERY: &str = r#"SELECT                                                
    "posts"."id",                                       
        "posts"."user_id",                                  
        "posts"."topic_id",                                 
        "posts"."post_number",                              
        "posts"."raw",                                      
        "posts"."cooked",                                   
        "posts"."created_at",                               
        "posts"."updated_at",                               
        "posts"."reply_to_post_number",                     
        "posts"."reply_count",                              
        "posts"."quote_count",                              
        "posts"."deleted_at",                               
        "posts"."off_topic_count",                          
        "posts"."like_count",                               
        "posts"."incoming_link_count",                      
        "posts"."bookmark_count",                           
        "posts"."score",                                    
        "posts"."reads",                                    
        "posts"."post_type",                                
        "posts"."sort_order",                               
        "posts"."last_editor_id",                           
        "posts"."hidden",                                   
        "posts"."hidden_reason_id",                         
        "posts"."notify_moderators_count",                  
        "posts"."spam_count",                               
        "posts"."illegal_count",                            
        "posts"."inappropriate_count",                      
        "posts"."last_version_at",                          
        "posts"."user_deleted",                             
        "posts"."reply_to_user_id",                         
        "posts"."percent_rank",                             
        "posts"."notify_user_count",                        
        "posts"."like_score",                               
        "posts"."deleted_by_id",                            
        "posts"."edit_reason",                              
        "posts"."word_count",                               
        "posts"."version",                                  
        "posts"."cook_method",                              
        "posts"."wiki",                                     
        "posts"."baked_at",                                 
        "posts"."baked_version",                            
        "posts"."hidden_at",                                
        "posts"."self_edits",                               
        "posts"."reply_quoted",                             
        "posts"."via_email",                                
        "posts"."raw_email",                                
        "posts"."public_version",                           
        "posts"."action_code",                              
        "posts"."locked_by_id",                             
        "posts"."image_upload_id",                          
        "posts"."outbound_message_id"                       
        FROM                                                  
        "posts"                                             
        INNER JOIN "topics" ON (                            
            ("topics"."deleted_at" IS NULL)                   
                AND ("topics"."id" = "posts"."topic_id")          
        )                                                   
            WHERE                                                 
            (                                                   
                ("posts"."deleted_at" IS NULL)                    
                    AND (                                             
                        ("topics"."archetype" != 'private_message')     
                            AND (                                           
                                ("topics"."visible" = $1)                     
                                    AND (                                         
                                        ("posts"."hidden" = $2)                     
                                            AND (                                       
                                                ("posts"."post_type" = $3)                
                                                    AND (                                     
                                                        ("posts"."id" <= 8804731)               
                                                            AND ("posts"."id" > 8804681)            
                                                    )                                         
                                            )                                           
                                    )                                             
                            )                                               
                    )                                                 
            )                                                   
        ORDER BY                                              
        "posts"."created_at" DESC                           
    "#;

fn basic_queries_sqlparser(c: &mut Criterion) {
    let mut group = c.benchmark_group("sqlparser-rs parsing benchmark");
    let dialect = GenericDialect {};

    group.bench_function("sqlparser::select", |b| {
        b.iter(|| Parser::parse_sql(&dialect, SIMPLE_QUERY));
    });

    group.bench_function("sqlparser::with_select", |b| {
        b.iter(|| Parser::parse_sql(&dialect, WITH_QUERY));
    });

    group.bench_function("sqlparser::large_query", |b| {
        b.iter(|| Parser::parse_sql(&dialect, LARGE_QUERY));
    });

    group.bench_function("sqlparser::complex_query", |b| {
        b.iter(|| Parser::parse_sql(&dialect, COMPLEX_QUERY));
    });
}

fn basic_queries_nomsql(c: &mut Criterion) {
    let mut group = c.benchmark_group("nomsql parsing benchmark");

    group.bench_function("nomsql::select", |b| {
        b.iter(|| parse_select_statement(Dialect::PostgreSQL, SIMPLE_QUERY));
    });

    group.bench_function("nomsql::with_select", |b| {
        b.iter(|| parse_select_statement(Dialect::PostgreSQL, WITH_QUERY));
    });

    group.bench_function("nomsql::large_query", |b| {
        b.iter(|| parse_select_statement(Dialect::PostgreSQL, LARGE_QUERY));
    });

    group.bench_function("nomsql::complex_query", |b| {
        b.iter(|| parse_select_statement(Dialect::PostgreSQL, COMPLEX_QUERY));
    });
}


criterion_group!(benches, basic_queries_sqlparser, basic_queries_nomsql);
criterion_main!(benches);
