with orders as (
    select * from {{ ref('fct_orders') }}
),

base as (
    select
        marketing_source,
        order_year,
        order_quarter,
        count(*)                    as total_orders,
        count(distinct customer_id) as unique_customers,
        sum(order_amount)           as total_revenue,
        avg(order_amount)           as avg_order_value
    from orders
    where order_status = 'completed'
    group by marketing_source, order_year, order_quarter
),

ranked as (
    select
        *,
        rank() over (
            partition by order_year, order_quarter
            order by total_revenue desc
        ) as revenue_rank
    from base
)

select * from ranked
