{{ config(materialized='view') }}

with source as (select * from {{ source('bronze', 'erp_order_items') }}),

deduplicated as (
    select *,
        row_number() over (
            partition by order_item_id
            order by updated_at desc, _ingest_ts desc
        ) as _rn
    from source
),

renamed as (
    select
        order_item_id,
        order_id,
        cast(line_number as int)                      as line_number,
        product_id,
        cast(quantity as int)                          as quantity,
        cast(unit_price_local as decimal(18,2))        as unit_price_local,
        cast(discount_amount_local as decimal(18,2))   as discount_amount_local,
        cast(line_gross_amount_local as decimal(18,2)) as line_gross_amount_local,
        cast(line_net_amount_local as decimal(18,2))   as line_net_amount_local,
        cast(vat_rate as decimal(5,4))                 as vat_rate,
        cast(vat_amount_local as decimal(18,2))        as vat_amount_local,
        cast(unit_cost_eur as decimal(18,2))           as unit_cost_eur,
        currency_code,
        cast(created_at as timestamp)                  as created_at,
        cast(updated_at as timestamp)                  as updated_at
    from deduplicated
    where _rn = 1
)

select * from renamed