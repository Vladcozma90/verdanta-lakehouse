{{ config(materialized='table') }}

with products as (select * from {{ ref('snap_pim_products') }}),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['product_id', 'dbt_valid_from']) }} as product_sk,
        product_id,
        product_name,
        brand,
        is_own_brand,
        category_l1,
        category_l2,
        category_l3,
        unit_cost_eur,
        list_price_eur,
        vat_class,
        colour,
        material,
        weight_kg,
        is_seasonal,
        peak_season,
        launch_date,
        discontinued_date,
        product_status,
        dbt_valid_from                                as valid_from,
        dbt_valid_to                                   as valid_to,
        (dbt_valid_to = timestamp('9999-12-31'))       as is_current
    from products
),

unknown_member as (
    select
        {{ dbt_utils.generate_surrogate_key(["'-1'"]) }} as product_sk,
        '-1'      as product_id,
        'Unknown' as product_name,
        'Unknown' as brand,
        false     as is_own_brand,
        'Unknown' as category_l1,
        'Unknown' as category_l2,
        'Unknown' as category_l3,
        null      as unit_cost_eur,
        null      as list_price_eur,
        'STANDARD' as vat_class,
        null      as colour,
        null      as material,
        null      as weight_kg,
        false     as is_seasonal,
        null      as peak_season,
        null      as launch_date,
        null      as discontinued_date,
        'UNKNOWN' as product_status,
        timestamp('1900-01-01')                       as valid_from,
        timestamp('9999-12-31')                         as valid_to,
        true                                             as is_current
)

select * from final
union all
select * from unknown_member