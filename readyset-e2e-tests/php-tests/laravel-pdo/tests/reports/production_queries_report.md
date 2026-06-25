# CartPanda Production Query Test Report

**Generated:** 2026-01-29 13:40:23
**Backend:** MySQL
**Total Queries:** 22
**Passed:** 22
**Unsupported (cannot cache):** 0
**Failed:** 0
**Parse Errors:** 0
**Total Time:** 36.22ms

## Results Summary

| Query | Name | Status | Time (ms) | Complexity |
|-------|------|--------|-----------|------------|
| 1 | Count Orders with Complex Filtering | PASS | 14.32 | FORCE INDEX with comment, GROUP_CONCAT |
| 2 | Select Orders with Full Details | PASS | 0.67 | FORCE INDEX, Multiple JOINs |
| 3 | Orders with Affiliate Info | PASS | 0.97 | Correlated subqueries with JOINs, FORCE INDEX |
| 4 | Count Distinct Orders | PASS | 0.21 | FORCE INDEX with comment, DISTINCT in subquery |
| 5 | Orders with Customer - UNION | PASS | 2.04 | UNION, Multiple JOINs |
| 6 | Complex Seller Split Calculation | PASS | 1.56 | Deeply nested IF/IFNULL, CAST JSON_EXTRACT |
| 7 | Revenue with Exchange Rates | PASS | 0.75 | UNION ALL, Complex IF expressions |
| 8 | Sum Seller Split Amounts | PASS | 0.52 | SUM with complex CASE, Derived tables |
| 9 | Orders Customer Shipping - UNION LIMIT | PASS | 0.99 | UNION, ORDER BY on UNION result |
| 10 | Abandoned Carts Triple UNION ALL | PASS | 3.51 | Triple UNION ALL, Multiple JOINs |
| 11 | Orders with Subscriptions JSON | PASS | 1.25 | JSON_ARRAYAGG, JSON_OBJECT |
| 12 | Order Counts by Status - Multiple UNION ALL | PASS | 0.73 | Six-way UNION ALL, EXISTS subqueries |
| 13 | Sales Analysis by Date | PASS | 0.51 | date() function, EXISTS with multiple conditions |
| 14 | Abandoned Carts Revenue Analysis | PASS | 0.59 | Triple UNION ALL, Aggregation on UNION |
| 15 | Global Shipping Order Details | PASS | 3.44 | GROUP_CONCAT with CONCAT, Multiple correlated subqueries |
| 16 | GMV Take Rate Analysis | PASS | 0.32 | FORCE INDEX on JOIN, DATE_FORMAT |
| 17 | Rankings with User Avatars | PASS | 1.04 | USE INDEX, FORCE INDEX |
| 18 | Order Counts with Optimizer Hints | PASS | 0.33 | Optimizer hints /*+ */, EXISTS subqueries |
| 19 | Users for Timeline | PASS | 0.5 | CONCAT for username, Multiple INNER JOINs |
| 20 | Balance Calculation with Variables | PASS | 0.31 | User variable @sum, Derived table |
| 21 | Count Orders with Affiliate UNION | PASS | 0.55 | UNION, MAX aggregation in SELECT |
| 22 | Select Orders with Affiliates LIMIT | PASS | 0.58 | UNION, ORDER BY on UNION |
