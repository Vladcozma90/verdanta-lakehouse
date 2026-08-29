{{ config(materialized='table') }}

with countries as (select * from {{ ref('seed_countries') }}),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['country_code']) }} as country_sk,
        country_code,
        country_name,
        region,
        currency_code
    from countries
),

unknown_member as (
    select
        {{ dbt_utils.generate_surrogate_key(["'XX'"]) }} as country_sk,
        'XX'      as country_code,
        'Unknown' as country_name,
        'Unknown' as region,
        'EUR'     as currency_code
)

select * from final
union all
select * from unknown_member