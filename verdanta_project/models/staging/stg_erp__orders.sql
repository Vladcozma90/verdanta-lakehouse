{{ config(materialized='view') }}

with source as (select * from {{ source('bronze', 'erp_orders') }}),

deduplicated as (
    select *,
        row_number() over (
            partition by order_id
            order by updated_at desc, _ingest_ts desc
        ) as _rn
    from source
),

renamed as (
    select
        order_id,
        order_number,
        customer_id,
        store_id,
        employee_id,
        sales_channel,
        order_status,
        upper(trim(country_code))                       as country_code,
        upper(trim(shipping_country_code))               as shipping_country_code,
        currency_code,
        payment_method,
        promotion_code,
        cast(order_ts as timestamp)                      as order_ts,
        cast(order_gross_amount as decimal(18,2))        as order_gross_amount,
        cast(order_discount_amount as decimal(18,2))     as order_discount_amount,
        cast(order_net_amount as decimal(18,2))          as order_net_amount,
        cast(order_vat_amount as decimal(18,2))          as order_vat_amount,
        cast(created_at as timestamp)                    as created_at,
        cast(updated_at as timestamp)                    as updated_at
    from deduplicated
    where _rn = 1
)

select * from renamed