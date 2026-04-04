with orders as (
    select * from {{ ref('fct_orders') }}
)

select
    product_id,
    product_name,
    product_category,
    count(*)                       as total_orders,
    sum(order_amount)              as total_revenue,
    avg(order_amount)              as avg_order_value,
    count(distinct shipping_state) as states_sold_to
from orders
where order_status = 'completed'
group by product_id, product_name, product_category
