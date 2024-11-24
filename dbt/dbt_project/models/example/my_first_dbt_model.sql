{{ config(materialized='table') }}

with order_selected as (

    select
        order_id as id,
        order_date

    from staging.orders
)

--select * from order_selected
select * from read_csv_auto('./data/seed/orders.csv')
