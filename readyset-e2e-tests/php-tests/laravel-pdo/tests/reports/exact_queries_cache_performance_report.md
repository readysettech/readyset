# Exact Queries - Shallow Cache Performance Report

Generated: 2026-01-28 15:48:02

## Configuration

| Setting | Value |
|---------|-------|
| TTL | 300 seconds |
| Refresh | Every 60 seconds |
| Coalesce | 5 seconds |

## Summary

| Metric | Value |
|--------|-------|
| Total Queries | 22 |
| Executed Successfully | 22 |
| Queries Cached | 22 |
| Queries Not Cached | 0 |
| Avg Cache Miss | 3.21 ms |
| Avg Cache Hit | 2.09 ms |
| Avg Improvement | 35% |

## Query Performance

| Q# | Query | Cache Miss | Cache Hit | Improvement | Cached |
|----|-------|------------|-----------|-------------|--------|
| 1 | Count orders with complex filtering | 22.68 ms | 6.96 ms | +69% | Yes |
| 2 | Select orders with all details (without count) | 2.99 ms | 3.41 ms | -14% | Yes |
| 3 | Orders with affiliate information including names and emails | 3.49 ms | 4.17 ms | -20% | Yes |
| 4 | Count distinct orders | 0.79 ms | 0.74 ms | +7% | Yes |
| 5 | Orders with customer info and shipping - using UNION | 2.14 ms | 2.59 ms | -21% | Yes |
| 6 | Complex seller split calculation | 3.58 ms | 3.70 ms | -3% | Yes |
| 7 | Revenue calculation with exchange rates | 1.92 ms | 1.95 ms | -2% | Yes |
| 8 | Sum seller split amounts | 2.87 ms | 3.03 ms | -6% | Yes |
| 9 | Orders with customer and shipping - UNION with limit | 1.40 ms | 1.06 ms | +24% | Yes |
| 10 | Abandoned carts with products and customers - UNION ALL | 3.26 ms | 1.29 ms | +60% | Yes |
| 11 | Complex order details with subscriptions and multiple joins | 4.80 ms | 3.04 ms | +37% | Yes |
| 12 | Multiple UNION for order counts by status | 3.33 ms | 0.90 ms | +73% | Yes |
| 13 | Sales analysis by date | 0.89 ms | 0.71 ms | +21% | Yes |
| 14 | Abandoned carts revenue analysis | 1.41 ms | 1.48 ms | -5% | Yes |
| 15 | Order details for global shipping | 5.62 ms | 1.73 ms | +69% | Yes |
| 16 | GMV and take rate analysis | 1.64 ms | 0.87 ms | +47% | Yes |
| 17 | Rankings with user avatars | 1.45 ms | 0.85 ms | +42% | Yes |
| 18 | Order counts by status with index hints | 1.06 ms | 0.95 ms | +10% | Yes |
| 19 | Users for timeline | 0.62 ms | 0.54 ms | +13% | Yes |
| 20 | Balance calculation | 0.67 ms | 2.46 ms | -269% | Yes |
| 21 | Count orders with affiliate unions | 2.34 ms | 2.13 ms | +9% | Yes |
| 22 | Select orders with affiliates and limit | 1.75 ms | 1.53 ms | +13% | Yes |
