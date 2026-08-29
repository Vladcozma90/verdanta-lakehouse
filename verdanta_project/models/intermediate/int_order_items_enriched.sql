{{ config(materialized='view') }}

with items as (select * from {{ ref('stg_erp__order_items') }}),
orders as (select * from {{ ref('stg_erp__orders') }}),

joined as (
    select
        i.order_item_id,
        i.order_id,
        i.line_number,
        i.product_id,
        i.quantity,
        i.unit_price_local,
        i.discount_amount_local,
        i.line_gross_amount_local,
        i.line_net_amount_local,
        i.vat_rate,
        i.vat_amount_local,
        i.unit_cost_eur,
        i.currency_code,
        i.created_at,
        i.updated_at,

        o.order_number,
        o.customer_id,
        o.store_id,
        o.employee_id,
        o.sales_channel,
        o.order_status,
        o.country_code,
        o.shipping_country_code,
        o.payment_method,
        o.promotion_code
    from items i
    inner join orders o
        on i.order_id = o.order_id
)

select * from joined