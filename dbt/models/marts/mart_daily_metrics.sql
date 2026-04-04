with orders as (
    select * from {{ ref('fct_orders') }}
)

select
    order_date,
    count(*)          as daily_orders,
    sum(order_amount) as daily_revenue,
    avg(order_amount) as avg_order_value
from orders
where order_status = 'completed'
group by order_date
order by order_date
