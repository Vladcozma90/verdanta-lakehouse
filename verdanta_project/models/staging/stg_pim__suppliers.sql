{{ config(materialized='view') }}

with source as (select * from {{ source('bronze', 'pim_suppliers') }}),

deduplicated as (
    select *,
        row_number() over (
            partition by supplier_id
            order by updated_at desc, _ingest_ts desc
        ) as _rn
    from source
),

renamed as (
    select
        supplier_id,
        supplier_name,
        upper(trim(supplier_country_code))          as supplier_country_code,
        cast(lead_time_days as int)                 as lead_time_days,
        cast(is_preferred_supplier as boolean)       as is_preferred_supplier,
        cast(payment_terms_days as int)              as payment_terms_days,
        cast(onboarded_date as date)                 as onboarded_date,
        supplier_status,
        cast(created_at as timestamp)                as created_at,
        cast(updated_at as timestamp)                as updated_at
    from deduplicated
    where _rn = 1
)

select * from renamed