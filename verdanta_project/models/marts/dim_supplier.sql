{{ config(materialized='table') }}

with suppliers as (select * from {{ ref('stg_pim__suppliers') }}),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['supplier_id']) }} as supplier_sk,
        supplier_id,
        supplier_name,
        supplier_country_code,
        lead_time_days,
        is_preferred_supplier,
        payment_terms_days,
        onboarded_date,
        supplier_status,
        (supplier_status = 'ACTIVE')                   as is_active
    from suppliers
),

unknown_member as (
    select
        {{ dbt_utils.generate_surrogate_key(["'-1'"]) }} as supplier_sk,
        '-1'      as supplier_id,
        'Unknown' as supplier_name,
        'XX'      as supplier_country_code,
        null      as lead_time_days,
        false     as is_preferred_supplier,
        null      as payment_terms_days,
        null      as onboarded_date,
        'UNKNOWN' as supplier_status,
        false     as is_active
)

select * from final
union all
select * from unknown_member