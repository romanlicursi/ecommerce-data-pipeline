with source as (
    select * from {{ source('ecommerce_cleaned', 'orders') }}
),

renamed as (
    select
        order_id,
        customer_id,
        order_date::date       as order_date,
        product_id,
        order_amount::float    as order_amount,
        customer_email,
        shipping_state,
        marketing_source,
        order_status
    from source
)

select * from renamed
