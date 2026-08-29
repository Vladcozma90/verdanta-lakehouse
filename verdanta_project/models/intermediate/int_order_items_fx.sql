{{ config(materialized='view') }}

with items as (select * from {{ ref('int_order_items_enriched') }}),
fx as (select * from {{ ref('seed_fx_rates_eur') }}),

joined as (
    select
        i.*,
        coalesce(f.exchange_rate, 1.0)                                     as fx_rate_to_eur,
        round(i.line_gross_amount_local / coalesce(f.exchange_rate,1.0),2) as line_gross_amount_eur,
        round(i.line_net_amount_local   / coalesce(f.exchange_rate,1.0),2) as line_net_amount_eur,
        round(i.vat_amount_local        / coalesce(f.exchange_rate,1.0),2) as vat_amount_eur,
        round(i.unit_cost_eur * i.quantity, 2)                             as cost_amount_eur
    from items i
    left join fx f
        on f.to_currency_code = i.currency_code
       and f.rate_date = cast(i.created_at as date)
)

select * from joined