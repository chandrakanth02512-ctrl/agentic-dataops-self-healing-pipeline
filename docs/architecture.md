# Phase 1: order data pipeline

The first part of Agentic DataOps is a regular data pipeline. The AI investigation
and approval components will use its validation errors and run metrics later.

## Data flow

1. **Source:** Three fictional CSV files: customers, products, and order events.
2. **Bronze:** Preserve raw source records, plus the source file and load timestamp.
3. **Silver events:** Validate columns, types, customer/product references, and
   duplicate events. Give each rejected row a reason.
4. **Silver current orders:** Select the latest valid event for each order.
   Deletes remain as markers to prevent an older event reviving a deleted order.
5. **Gold:** Calculate daily order counts and revenue by product category.
   Deleted and rejected orders are excluded.

## Data contracts

| Source | Required columns |
| --- | --- |
| Customers | `customer_id`, `customer_name`, `region` |
| Products | `product_id`, `product_name`, `category` |
| Orders | `order_id`, `customer_id`, `product_id`, `quantity`, `unit_price`, `order_status`, `order_date`, `operation`, `event_time` |

Order operations are `I` (insert), `U` (update), and `D` (delete). The newest
`event_time` wins. With tied timestamps, deletes take priority.

## Local and cloud execution

- This implementation uses PySpark throughout.
- Development runs write Parquet so tests can execute without extra Delta jars.
- Fabric Spark can write Delta tables in OneLake using `--format delta`.
- This version replays all supplied events. Incremental checkpoints and Delta
  MERGE are still **planned within Phase 1**, not completed yet.

Sample data is fictional. We will measure actual run statistics before adding
performance claims to the README or resume.
