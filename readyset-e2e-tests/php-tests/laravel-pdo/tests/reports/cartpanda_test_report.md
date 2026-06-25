# CartPanda Query Test Report

**Generated**: 2026-01-23 17:45:23
**Target**: ReadySet
**Mode**: ReadySet (Shallow Cache)
**Test Plan**: cartpanda-shallow-cache-test-plan.md
**Protocol**: MySQL Binary Protocol (PDO prepared statements)

---

## Summary

| Metric | Value |
|--------|-------|
| **Total Tests** | 11 |
| **Passed** | 11 |
| **Failed** | 0 |
| **Skipped** | 0 |
| **Pass Rate** | 100% |
| **Total Time** | 1081.22ms |

> **Status**: :white_check_mark: ALL TESTS PASSED

---

## Detailed Results

### P0 - Blockers

| Test ID | Test Name | Status | Time (ms) | Details |
|---------|-----------|--------|-----------|--------|
| P0-05 | Prepared Statement Support (Binary Protocol) | :white_check_mark: PASS | 3.80 | Binary protocol working. Times: 1.63ms -> 0.67ms -> 0.69ms -... |
| P0-04 | Large Result Set Handling | :white_check_mark: PASS | 1.29 | Successfully retrieved 51 rows in 1.27ms |
| P0-02 | Serving Stale Data During Refresh | :white_check_mark: PASS | 1034.57 | All 10 cache hits returned consistent data. Avg latency: 2.6... |

### P1 - Critical

| Test ID | Test Name | Status | Time (ms) | Details |
|---------|-----------|--------|-----------|--------|
| P1-05 | Manual Cache Invalidation (DROP CACHE) | :white_check_mark: PASS | 15.87 | ReadySet commands available. Show: none, Drop: yes |
| P1-06 | Metrics Accuracy (hits/misses/refreshes) | :white_check_mark: PASS | 4.51 | Metrics available. 2 commands working: SHOW STATUS, SHOW TAB... |

### P2 - Important

| Test ID | Test Name | Status | Time (ms) | Details |
|---------|-----------|--------|-----------|--------|
| P2-03-Q1 | Complex Order Retrieval with Affiliate Splits | :white_check_mark: PASS | 6.53 | Complex query executed successfully. 51 rows. Times: 4.39 ->... |
| P2-03-Q2 | Abandoned Cart Reporting with Product Details | :white_check_mark: PASS | 3.10 | Abandoned cart query executed successfully. 10 rows. Times: ... |
| P2-02 | Multi-Tenant Isolation (shop_id filtering) | :white_check_mark: PASS | 1.47 | Multi-tenant isolation verified. Shop 1: 50 rows, Shop 2: 49... |
| P2-01 | LIMIT/OFFSET Pagination Efficiency | :white_check_mark: PASS | 3.05 | Pagination working. Page sizes: 10/10/10. Page 1 cached: 0.8... |
| P2-05 | UNION Query Caching | :white_check_mark: PASS | 2.17 | UNION query cached successfully. 21 rows. Times: 1.59 -> 0.5... |
| BONUS-01 | Order Line Items with Product Details | :white_check_mark: PASS | 2.05 | Order line items query successful. 7 rows. Times: 1.36 -> 0.... |

---

## Test Metrics

### P0-05: Prepared Statement Support (Binary Protocol)

```json
{
    "exec1_time_ms": 1.63,
    "exec2_time_ms": 0.67,
    "exec3_time_ms": 0.69,
    "exec4_time_ms": 0.69,
    "row_count": 10,
    "binary_protocol": true
}
```

### P0-04: Large Result Set Handling

```json
{
    "row_count": 51,
    "execution_time_ms": 1.27
}
```

### P0-02: Serving Stale Data During Refresh

```json
{
    "baseline_rows": 50,
    "total_hits": 10,
    "avg_hit_time_ms": 2.67,
    "min_hit_time_ms": 0.66,
    "max_hit_time_ms": 4.33,
    "data_consistent": true
}
```

### P1-05: Manual Cache Invalidation (DROP CACHE)

```json
{
    "show_command_worked": null,
    "proxied_count": 0,
    "drop_cache_supported": true,
    "post_drop_time_ms": 1.29
}
```

### P1-06: Metrics Accuracy (hits/misses/refreshes)

```json
{
    "status_entries": 499,
    "proxied_queries": 0,
    "tables_tracked": 39,
    "commands_available": [
        "SHOW STATUS",
        "SHOW TABLES"
    ],
    "status_sample": {
        "Aborted_clients": "2",
        "Aborted_connects": "0",
        "Acl_cache_items_count": "0",
        "Binlog_cache_disk_use": "0",
        "Binlog_cache_use": "0"
    }
}
```

### P2-03-Q1: Complex Order Retrieval with Affiliate Splits

```json
{
    "exec1_time_ms": 4.39,
    "exec2_time_ms": 1.18,
    "exec3_time_ms": 0.89,
    "row_count": 51,
    "tables_joined": 4,
    "has_subqueries": true,
    "has_case_statement": true
}
```

### P2-03-Q2: Abandoned Cart Reporting with Product Details

```json
{
    "exec1_time_ms": 2.45,
    "exec2_time_ms": 0.62,
    "row_count": 10,
    "tables_joined": 6
}
```

### P2-02: Multi-Tenant Isolation (shop_id filtering)

```json
{
    "shop1_rows": 50,
    "shop2_rows": 49,
    "shop1_isolated": true,
    "shop2_isolated": true
}
```

### P2-01: LIMIT/OFFSET Pagination Efficiency

```json
{
    "page1_time_ms": 0.81,
    "page2_time_ms": 1.01,
    "page1_cached_time_ms": 0.54,
    "page3_time_ms": 0.54,
    "page1_rows": 10,
    "page2_rows": 10,
    "page3_rows": 10,
    "no_data_overlap": true
}
```

### P2-05: UNION Query Caching

```json
{
    "exec1_time_ms": 1.59,
    "exec2_time_ms": 0.54,
    "row_count": 21
}
```

### BONUS-01: Order Line Items with Product Details

```json
{
    "exec1_time_ms": 1.36,
    "exec2_time_ms": 0.64,
    "row_count": 7,
    "tables_joined": 4
}
```

---

## Test Environment

| Component | Value |
|-----------|-------|
| Test Mode | ReadySet (Full Test) |
| Target Host | readyset:3306 |
| MySQL Host | mysql:3306 |
| Database | prod_ecomm_cartx |
| Test Shop ID | 1 |
| Secondary Shop ID | 2 |
| PHP Version | 8.2.30 |
| PDO Emulate Prepares | false (binary protocol) |

---

*Report generated by CartPanda Test Suite*
