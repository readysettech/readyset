# CartPanda Phase 1 Query Test Report

Generated: 2026-01-26 18:25:32
Backend: ReadySet
Total Time: 160.56ms

## Summary

| Metric | Count |
|--------|-------|
| Passed | 2/5 |
| Parse Errors | 2/5 |
| Unsupported | 1/5 |
| Failed | 0/5 |

## Parse Errors

These queries cannot be parsed by ReadySet:

### Query 1: GMV and Take Rate Analysis

**Blocking Features:** DATE_FORMAT, FORCE INDEX on JOIN, Subqueries with ORDER BY/LIMIT, SUM aggregations, CASE statement

**Error:** SQLSTATE[HY000]: General error: 1105 Query failed to parse: SELECT DATE_FORMAT(o.created_at, '%Y-%m') AS 'Created at', o.shop_id AS 'ID Shop', CASE WHEN op.gateway = 'cartpanda_pay' THEN 'Cartpanda Pay' ELSE 'Other' END AS 'Gateway', SUM(op.amount) AS 'GMV', SUM(op.cartpanda_pay_split_amount) AS 'Cartpanda pay split amount', SUM(op.seller_split_amount) AS 'Seller split amount', SUM(op.cartpanda_pay_split_amount)/SUM(op.amount)*100 AS 'Take rate %' FROM orders o JOIN orders_payments op force inde

### Query 5: Orders with Affiliates UNION

**Blocking Features:** UNION, TRIM/CONCAT/COALESCE, Multiple JOINs, MAX aggregation, ORDER BY on UNION

**Error:** SQLSTATE[HY000]: General error: 1105 Query failed to parse: ( SELECT `orders`.`note`, `orders`.`customer_id`, `orders`.`id`, `orders`.`created_at`, `orders`.`total_price`, `orders`.`fulfillment_status`, `orders`.`name`, `orders`.`order_number`, `orders`.`status_id`, `orders`.`shop_id`, `orders`.`is_archived`, `orders`.`is_cp_pay`, `orders`.`cancelled_at`, `orders`.`updated_at`, `orders`.`payment_details`, `orders`.`total_price_in_decimal`, `orders`.`test`, customers.email AS customers_email, TRI

## Passed Queries

- Query 3: Count Orders with Complex Filtering (1 rows)
- Query 4: Order Details with Affiliates (0 rows)

