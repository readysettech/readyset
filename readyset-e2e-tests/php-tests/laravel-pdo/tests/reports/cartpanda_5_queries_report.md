# CartPanda 5 Core Queries Test Report

**Generated:** 2026-01-26 18:25:32
**Backend:** ReadySet
**Total Queries:** 5
**Passed:** 0
**Unsupported:** 0
**Failed:** 0
**Parse Errors:** 5
**Total Time:** 13.8ms

## Query Results

| Query | Name | Status | Features |
|-------|------|--------|----------|
| 1 | Complex Order Count (No FORCE INDEX) | PARSE ERROR | GROUP_CONCAT, JSON_EXTRACT, JSON_UNQUOTE |
| 2 | Full Order Details (No FORCE INDEX) | PARSE ERROR | Multiple JOINs, Correlated subqueries, CASE statements |
| 3 | Orders with Affiliate Info (No FORCE INDEX) | PARSE ERROR | Correlated subqueries with JOINs, Multiple subqueries in SELECT |
| 4 | Count Distinct Orders (No FORCE INDEX) | PARSE ERROR | DISTINCT in subquery, Multiple LEFT JOINs, Aggregate on subquery |
| 5 | Orders with Customer Info - UNION (Expected to Fail) | PARSE ERROR | UNION (unsupported), Multiple JOINs, COALESCE |

## Parse Errors Detail

### Query 1: Complex Order Count (No FORCE INDEX)

**Features:** GROUP_CONCAT, JSON_EXTRACT, JSON_UNQUOTE, Correlated subqueries, Multiple CASE statements

**Error:**
```
SQLSTATE[HY000]: General error: 1105 Query failed to parse: 'select count(*) as aggregate from (select `orders`.`id`, `orders`.`browser_ip`, `orders`.`email`, `orders`.`created_at`, `orders`.`currency`, `orders`.`total_price`, `orders`.`shop_id`, `orders`.`customer_id`,
    ((SELECT GROUP_CONCAT(`order_tags`.`tag`) FROM `order_tags` WHERE `order_tags`.`id` IN
        ((SELECT `order_order_tag`.`order_tag_id` FROM `order_order_tag` WHERE `order_order_tag`.`order_id` = `orders`.`id`))
        AND 
```

### Query 2: Full Order Details (No FORCE INDEX)

**Features:** Multiple JOINs, Correlated subqueries, CASE statements, GROUP_CONCAT

**Error:**
```
SQLSTATE[HY000]: General error: 1105 Query failed to parse: 'select `orders`.`id`, `orders`.`email`, `orders`.`created_at`, `orders`.`currency`, `orders`.`total_price`, `orders`.`shop_id`, `orders`.`customer_id`,
    ((SELECT GROUP_CONCAT(`order_tags`.`tag`) FROM `order_tags` WHERE `order_tags`.`id` IN
        ((SELECT `order_order_tag`.`order_tag_id` FROM `order_order_tag` WHERE `order_order_tag`.`order_id` = `orders`.`id`))
        AND `order_tags`.`shop_id` = `orders`.`shop_id`
    )) AS `tag
```

### Query 3: Orders with Affiliate Info (No FORCE INDEX)

**Features:** Correlated subqueries with JOINs, Multiple subqueries in SELECT

**Error:**
```
SQLSTATE[HY000]: General error: 1105 Query failed to parse: 'select `orders`.`id`, `orders`.`email`, `orders`.`total_price`, `orders`.`shop_id`,
    (SELECT S.name FROM afillio_orders AO JOIN shops S ON S.id = AO.shop_id WHERE AO.order_id = orders.id AND AO.affiliate_commission > 0 LIMIT 1) AS affiliate_name,
    (SELECT U.email FROM afillio_orders AO JOIN shops S ON S.id = AO.shop_id JOIN users U ON S.owner_id = U.id WHERE AO.order_id = orders.id AND AO.affiliate_commission > 0 LIMIT 1) AS affi
```

### Query 4: Count Distinct Orders (No FORCE INDEX)

**Features:** DISTINCT in subquery, Multiple LEFT JOINs, Aggregate on subquery

**Error:**
```
SQLSTATE[HY000]: General error: 1105 Query failed to parse: 'select count(*) as aggregate from (
    select distinct `orders`.`id`
    from `orders`
    inner join `orders_payments` on `orders`.`id` = `orders_payments`.`order_id`
    left join `orders_discounts` on `orders_discounts`.`order_id` = `orders`.`id`
    left join `s2s_orders_postback_queue` on `orders`.`id` = `s2s_orders_postback_queue`.`order_id`
    where (`orders`.`shop_id` = 1) and `orders`.`id` > 0
    group by `orders`.`id`
) as
```

### Query 5: Orders with Customer Info - UNION (Expected to Fail)

**Features:** UNION (unsupported), Multiple JOINs, COALESCE, TRIM, CONCAT, MAX aggregation

**Error:**
```
SQLSTATE[HY000]: General error: 1105 Query failed to parse: 'select count(*) as aggregate from (
    (select `orders`.`note`, `orders`.`customer_id`, `orders`.`id`, `orders`.`created_at`, `orders`.`total_price`,
        customers.email as customers_email,
        TRIM(CONCAT(COALESCE(customers.first_name,''''), '' '',COALESCE(customers.last_name,''''))) as customers_full_name,
        orders_payments.status_id as payments_status_id,
        orders_payments.gateway as payments_gateway,
        or
```

