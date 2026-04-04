with source as (
    select * from {{ source('ecommerce_raw', 'product_catalog') }}
),

renamed as (
    select
        product_id,
        name     as product_name,
        category as product_category,
        price::float as product_price
    from source
)

select * from renamed
