{{ config(
    materialized='incremental',
    unique_key='order_item_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

with items as (select * from {{ ref('int_order_items_fx') }}),

customer_dim as (select * from {{ ref('dim_customer') }}),
product_dim  as (select * from {{ ref('dim_product') }}),
store_dim    as (select * from {{ ref('dim_store') }}),
employee_dim as (select * from {{ ref('dim_employee') }}),
country_dim  as (select * from {{ ref('dim_country') }}),
date_dim     as (select * from {{ ref('dim_date') }}),

joined as (
    select
        items.order_item_id,

        items.order_id,
        items.order_number,
        items.line_number,

        coalesce(d.date_sk, {{ dbt_utils.generate_surrogate_key(["'1900-01-01'"]) }})  as date_sk,
        
        coalesce(c.customer_sk, {{ dbt_utils.generate_surrogate_key(["'-1'"]) }}) as customer_sk,
        coalesce(p.product_sk, {{ dbt_utils.generate_surrogate_key(["'-1'"]) }})  as product_sk,
        coalesce(s.store_sk, {{ dbt_utils.generate_surrogate_key(["'-1'"]) }})    as store_sk,
        coalesce(e.employee_sk, {{ dbt_utils.generate_surrogate_key(["'-1'"]) }}) as employee_sk,
        coalesce(co.country_sk, {{ dbt_utils.generate_surrogate_key(["'XX'"]) }}) as country_sk,

        items.sales_channel,
        items.order_status,
        items.payment_method,
        items.promotion_code,
        items.shipping_country_code,
        items.currency_code,

        items.quantity,
        items.unit_price_local,
        items.discount_amount_local,
        items.line_gross_amount_local,
        items.line_gross_amount_eur,
        items.line_net_amount_local,
        items.line_net_amount_eur,
        items.vat_rate,
        items.vat_amount_local,
        items.vat_amount_eur,
        items.unit_cost_eur,
        items.cost_amount_eur,
        items.fx_rate_to_eur,

        items.created_at   as order_item_ts,
        items.updated_at   as source_updated_at

    from items
    left join date_dim d
        on cast(items.created_at as date) = d.date_day
    left join customer_dim c
        on items.customer_id = c.customer_id
       and items.created_at >= c.valid_from
       and items.created_at <  c.valid_to
    left join product_dim p
        on items.product_id = p.product_id
       and items.created_at >= p.valid_from
       and items.created_at <  p.valid_to
    left join store_dim s
        on items.store_id = s.store_id
    left join employee_dim e
        on items.employee_id = e.employee_id
    left join country_dim co
        on items.country_code = co.country_code
)

select * from joined

{% if is_incremental()%}
where source_updated_at > (select max(source_updated_at) from {{ this }})

{% endif %}