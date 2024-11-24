{{ config(materialized='table') }}

with wh_orders as (
  select * 
  from "s3://cdkstack-csvbucketadda1e74-xwwsrvxkbhl4/lake/2024-11-24/orders.csv"
  -- from staging.orders
) select * from wh_orders
