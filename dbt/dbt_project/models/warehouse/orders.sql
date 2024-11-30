{{ config(materialized='table') }}

with raw_data as (
  select * 
  from read_csv_auto(
    "s3://cdkstack-csvbucketadda1e74-xwwsrvxkbhl4/lake/2024-11-30/orders.csv"
  )
),
filtered_data AS (
  select * 
  from raw_data
  where TRY_CAST(birth_date AS DATE) IS NOT NULL
)

select * 
from filtered_data
