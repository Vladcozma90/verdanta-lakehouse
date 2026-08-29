{{ config(materialized='view') }}

with source as (select * from {{ source('bronze', 'erp_returns') }}),

deduplicated as (
    select *,
        row_number() over (
            partition by return_id
            order by updated_at desc, _ingest_ts desc
        ) as _rn
    from source
),

renamed as (
    select
        return_id,
        order_id,
        order_item_id,
        product_id,
        return_store_id,
        to_timestamp(return_ts, 'dd/MM/yyyy HH:mm')    as return_ts,
        return_reason_code,
        cast(quantity_returned as int)                 as quantity_returned,
        cast(refund_amount_local as decimal(18,2))     as refund_amount_local,
        currency_code,
        cast(restock_flag as boolean)                  as restock_flag,
        cast(created_at as timestamp)                  as created_at,
        cast(updated_at as timestamp)                  as updated_at
    from deduplicated
    where _rn = 1
)

select * from renamed