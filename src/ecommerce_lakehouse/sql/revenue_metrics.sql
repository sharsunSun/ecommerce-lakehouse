{view_operation} {catalog}.{semantic_schema}.mv_ecommerce_revenue
{view_options}
AS $$
version: 1.1
comment: "Ecommerce revenue metrics"
source: SELECT * FROM {catalog}.{gold_schema}.fact_order_wide
filter: status <> 'refunded'
dimensions:
  - name: sale_date
    expr: sale_date
  - name: city
    expr: city
  - name: category
    expr: category
measures:
  - name: order_cnt
    expr: COUNT(DISTINCT order_id)
  - name: buyer_cnt
    expr: COUNT(DISTINCT user_id)
  - name: gmv
    expr: SUM(total_amount)
  - name: avg_order_value
    expr: SUM(total_amount) / NULLIF(COUNT(DISTINCT order_id), 0)
$$;