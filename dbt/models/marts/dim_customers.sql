with orders as (
    select * from {{ ref('fct_orders') }}
),

base as (
    select
        customer_id,
        customer_email,
        min(order_date)   as first_order_date,
        max(order_date)   as last_order_date,
        count(*)          as total_orders,
        sum(order_amount) as total_revenue,
        avg(order_amount) as avg_order_value
    from orders
    where order_status = 'completed'
    group by customer_id, customer_email
),

classified as (
    select
        *,
        case
            when total_orders = 1  then 'One-Time'
            when total_orders <= 3 then 'Occasional'
            else 'Repeat'
        end as customer_type,
        case
            when total_revenue >= 1000 then 'VIP'
            when total_revenue >= 500  then 'Premium'
            when total_revenue >= 100  then 'Standard'
            else 'Basic'
        end as customer_tier
    from base
)

select * from classified
