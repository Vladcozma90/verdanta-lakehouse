{{ config(materialized='view') }}

with source as (select * from {{ source('bronze', 'crm_customers') }}),

deduplicated as (
    select *,
        row_number() over (
            partition by customer_id
            order by updated_at desc, _ingest_ts desc
        ) as _rn
    from source
),

renamed as (
    select
        customer_id,
        trim(first_name)                          as first_name,
        trim(last_name)                            as last_name,
        email,
        phone_number,
        cast(birth_date as date)                   as birth_date,
        preferred_language,
        loyalty_tier,
        cast(loyalty_points_balance as int)        as loyalty_points_balance,

        case lower(trim(marketing_opt_in))
            when 'true' then true
            when 'y'    then true
            when '1'    then true
            when 'false' then false
            when 'n'     then false
            when '0'     then false
            else null
        end                                         as is_marketing_opt_in,

        address_line_1,
        city,
        postal_code,
        upper(trim(country_code))                  as country_code,
        customer_status,
        cast(signup_date as date)                  as signup_date,
        cast(created_at as timestamp)              as created_at,
        cast(updated_at as timestamp)               as updated_at
    from deduplicated
    where _rn = 1
)

select * from renamed