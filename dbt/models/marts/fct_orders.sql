with orders as (
    select * from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_product_catalog') }}
),

joined as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.product_id,
        o.order_amount,
        o.customer_email,
        o.shipping_state,
        o.marketing_source,
        o.order_status,
        p.product_name,
        p.product_category,
        p.product_price
    from orders o
    left join products p on o.product_id = p.product_id
),

enriched as (
    select
        *,
        extract(year from order_date)    as order_year,
        extract(month from order_date)   as order_month,
        extract(quarter from order_date) as order_quarter,
        case
            when order_amount >= 500 then 'High Value'
            when order_amount >= 100 then 'Medium Value'
            else 'Low Value'
        end as customer_segment
    from joined
)

select * from enriched
