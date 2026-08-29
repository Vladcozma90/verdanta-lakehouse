{{ config(materialized='table') }}

with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
),

final as (
    select
        cast(date_day as date)                                    as date_day,
        {{ dbt_utils.generate_surrogate_key(['date_day']) }}      as date_sk,
        year(date_day)                                            as year,
        quarter(date_day)                                         as quarter,
        month(date_day)                                           as month,
        date_format(date_day, 'MMMM')                             as month_name,
        day(date_day)                                             as day_of_month,
        dayofweek(date_day)                                       as day_of_week,
        date_format(date_day, 'EEEE')                             as day_name,
        (dayofweek(date_day) in (1, 7))                           as is_weekend,
        weekofyear(date_day)                                      as iso_week,
        (month(date_day) in (4, 5, 6, 7))                         as is_peak_garden_season
    from spine
)

select * from final