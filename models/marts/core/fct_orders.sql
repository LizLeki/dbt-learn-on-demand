{{ config(
    materialized='view'
)}}
select
    order_id,
    customer_id,
    order_date,
    coalesce(amount, 0) as amount
from (select * from {{ ref('stg_orders' )}})
left join (
    select
        order_id,
        sum(case when status = 'success' then amount end) as amount
    from {{ ref('stg_payments') }}
    group by order_id
    ) o
using (order_id)
