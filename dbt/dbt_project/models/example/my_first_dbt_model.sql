{{ config(materialized='table') }}

with order_selected as (

    select
        order_id as id,
        user_id as uid,
        order_date

    from orders2
)

select * from order_selected
