{{ config(materialized='table') }}

with stores as (select * from {{ ref('stg_reference__stores') }}),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['store_id']) }} as store_sk,
        store_id,
        store_name,
        store_format,
        address_line_1,
        city,
        postal_code,
        country_code,
        latitude,
        longitude,
        selling_area_sqm,
        store_manager_employee_id,
        opening_date,
        closing_date,
        store_status,
        (store_status = 'OPEN')                     as is_open
    from stores
),

unknown_member as (
    select
        {{ dbt_utils.generate_surrogate_key(["'-1'"]) }} as store_sk,
        '-1'      as store_id,
        'Unknown' as store_name,
        'Unknown' as store_format,
        null      as address_line_1,
        'Unknown' as city,
        null      as postal_code,
        'XX'      as country_code,
        null      as latitude,
        null      as longitude,
        null      as selling_area_sqm,
        null      as store_manager_employee_id,
        null      as opening_date,
        null      as closing_date,
        'UNKNOWN' as store_status,
        false     as is_open
)

select * from final
union all
select * from unknown_member