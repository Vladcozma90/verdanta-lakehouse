{{ config(materialized='view') }}

with source as (select * from {{ source('bronze', 'pim_products') }}),

deduplicated as (
    select *,
        row_number() over (
            partition by product_id
            order by updated_at desc, _ingest_ts desc
        ) as _rn
    from source
),

renamed as (
    select
        product_id,
        product_name,
        brand,
        cast(is_own_brand as boolean)              as is_own_brand,

        -- flattened from the nested `category` struct
        category.category_l1                       as category_l1,
        category.category_l2                       as category_l2,
        category.category_l3                       as category_l3,

        supplier_ids,                               -- array<string>, exploded downstream

        cast(unit_cost_eur as decimal(18,2))        as unit_cost_eur,
        cast(list_price_eur as decimal(18,2))       as list_price_eur,
        vat_class,

        -- flattened from the nested `attributes` struct
        attributes.colour                           as colour,
        attributes.material                          as material,
        cast(attributes.weight_kg as decimal(10,2))  as weight_kg,
        cast(attributes.is_seasonal as boolean)      as is_seasonal,
        attributes.peak_season                       as peak_season,

        cast(launch_date as date)                   as launch_date,
        cast(discontinued_date as date)              as discontinued_date,
        product_status,
        cast(created_at as timestamp)                as created_at,
        cast(updated_at as timestamp)                as updated_at
    from deduplicated
    where _rn = 1
)

select * from renamed