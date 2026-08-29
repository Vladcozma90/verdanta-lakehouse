{{ config(materialized='view') }}

with returns as (select * from {{ ref('stg_erp__returns') }}),
orders as (select * from {{ ref('stg_erp__orders') }}),

joined as (
    select
        r.return_id,
        r.order_id,
        r.order_item_id,
        r.product_id,
        r.return_store_id,
        r.return_ts,
        r.return_reason_code,
        r.quantity_returned,
        r.refund_amount_local,
        r.currency_code,
        r.restock_flag,
        r.created_at,
        r.updated_at,

        o.sales_channel,
        o.country_code,
        o.customer_id
    from returns r
    left join orders o
        on r.order_id = o.order_id
)

select * from joined