-- Fails if an order's header net amount doesn't reconcile to the sum of its lines.
{{ config(severity = 'warn') }}

with header as (
    select
        order_id,
        order_net_amount
    from {{ ref('stg_erp__orders') }}
),

lines as (
    select
        order_id,
        round(sum(line_net_amount_local), 2) as line_net_total
    from {{ ref('stg_erp__order_items') }}
    group by order_id
),

compared as (
    select
        h.order_id,
        h.order_net_amount,
        l.line_net_total,
        abs(h.order_net_amount - l.line_net_total) as diff
    from header h
    inner join lines l
        on h.order_id = l.order_id
)

select *
from compared
where diff > 0.02