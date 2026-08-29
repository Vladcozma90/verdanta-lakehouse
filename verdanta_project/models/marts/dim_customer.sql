{{ config(materialized='table') }}

with customers as (select * from {{ ref('snap_crm_customers') }}),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'dbt_valid_from']) }} as customer_sk,
        customer_id,
        first_name,
        last_name,
        coalesce(email, 'Unknown')                    as email,
        phone_number,
        birth_date,
        preferred_language,
        loyalty_tier,
        loyalty_points_balance,
        is_marketing_opt_in,
        address_line_1,
        city,
        postal_code,
        country_code,
        customer_status,
        signup_date,
        dbt_valid_from                                as valid_from,
        dbt_valid_to                                  as valid_to,
        (dbt_valid_to = timestamp('9999-12-31'))      as is_current
    from customers
),

unknown_member as (
    select
        {{ dbt_utils.generate_surrogate_key(["'-1'"]) }} as customer_sk,
        '-1'      as customer_id,
        'Unknown' as first_name,
        'Unknown' as last_name,
        'Unknown' as email,
        null      as phone_number,
        null      as birth_date,
        'Unknown' as preferred_language,
        'UNKNOWN' as loyalty_tier,
        null      as loyalty_points_balance,
        false     as is_marketing_opt_in,
        null      as address_line_1,
        'Unknown' as city,
        null      as postal_code,
        'XX'      as country_code,
        'UNKNOWN' as customer_status,
        null      as signup_date,
        timestamp('1900-01-01')                       as valid_from,
        timestamp('9999-12-31')                       as valid_to,
        true                                          as is_current
)

select * from final
union all
select * from unknown_member