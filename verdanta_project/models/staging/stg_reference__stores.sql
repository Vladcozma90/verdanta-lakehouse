{{ config(materialized='view') }}


with source as (select * from {{ source('bronze', 'reference_stores') }}),


deduplicated as (
    select *,
        row_number() over (
            partition by store_id
            order by updated_at desc, _ingest_ts desc
        ) as _rn
    from source
),

renamed as (
    select
        store_id,
        store_name,
        store_format,
        address_line_1,
        city,
        postal_code,
        upper(trim(country_code))                  as country_code,
        cast(latitude as decimal(10,6))             as latitude,
        cast(longitude as decimal(10,6))            as longitude,
        cast(selling_area_sqm as int)                as selling_area_sqm,
        store_manager_employee_id,
        cast(opening_date as date)                  as opening_date,
        cast(closing_date as date)                  as closing_date,
        store_status,
        cast(created_at as timestamp)                as created_at,
        cast(updated_at as timestamp)                as updated_at
    from deduplicated
    where _rn = 1
)

select * from renamed