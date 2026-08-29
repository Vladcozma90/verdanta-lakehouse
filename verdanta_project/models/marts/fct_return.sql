{{ config(
    materialized='incremental',
    unique_key='return_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

with returns as (select * from {{ ref('int_returns_fx') }}),

customer_dim as (select * from {{ ref('dim_customer') }}),
product_dim  as (select * from {{ ref('dim_product') }}),
store_dim    as (select * from {{ ref('dim_store') }}),
country_dim  as (select * from {{ ref('dim_country') }}),
date_dim     as (select * from {{ ref('dim_date') }}),

joined as (
    select
        returns.return_id,

        returns.order_id,
        returns.order_item_id,

        coalesce(d.date_sk, {{ dbt_utils.generate_surrogate_key(["'1900-01-01'"]) }})  as date_sk,

        -- effective-dated joins, AS OF the return date
        coalesce(c.customer_sk, {{ dbt_utils.generate_surrogate_key(["'-1'"]) }}) as customer_sk,
        coalesce(p.product_sk, {{ dbt_utils.generate_surrogate_key(["'-1'"]) }})  as product_sk,

        coalesce(s.store_sk, {{ dbt_utils.generate_surrogate_key(["'-1'"]) }})    as store_sk,
        coalesce(co.country_sk, {{ dbt_utils.generate_surrogate_key(["'XX'"]) }}) as country_sk,

        returns.sales_channel,
        returns.return_reason_code,
        returns.restock_flag,
        returns.currency_code,

        returns.quantity_returned,
        returns.refund_amount_local,
        returns.refund_amount_eur,
        returns.fx_rate_to_eur,

        returns.return_ts,
        returns.updated_at   as source_updated_at

    from returns
    left join date_dim d
        on cast(returns.return_ts as date) = d.date_day
    left join customer_dim c
        on returns.customer_id = c.customer_id
       and returns.return_ts >= c.valid_from
       and returns.return_ts <  c.valid_to
    left join product_dim p
        on returns.product_id = p.product_id
       and returns.return_ts >= p.valid_from
       and returns.return_ts <  p.valid_to
    left join store_dim s
        on returns.return_store_id = s.store_id
    left join country_dim co
        on returns.country_code = co.country_code
)

select * from joined

{% if is_incremental() %}
where source_updated_at > (select max(source_updated_at) from {{ this }})
{% endif %}