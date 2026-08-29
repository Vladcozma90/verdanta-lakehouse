{{ config(materialized='view') }}

with returns as (select * from {{ ref('int_returns_enriched') }}),
fx as (select * from {{ ref('seed_fx_rates_eur') }}),

joined as (
    select
        r.*,
        coalesce(f.exchange_rate, 1.0)                                       as fx_rate_to_eur,
        round(r.refund_amount_local / coalesce(f.exchange_rate,1.0), 2)      as refund_amount_eur
    from returns r
    left join fx f
        on f.to_currency_code = r.currency_code
       and f.rate_date = cast(r.return_ts as date)
)

select * from joined