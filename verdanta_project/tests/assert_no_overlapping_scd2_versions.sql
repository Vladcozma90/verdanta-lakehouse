-- Fails if any customer or product has two SCD2 versions with overlapping valid ranges.
-- A correct SCD2 dimension never has two "current at the same instant" rows for one entity.

with customer_overlaps as (
    select
        a.customer_id,
        a.valid_from as a_valid_from,
        a.valid_to   as a_valid_to,
        b.valid_from as b_valid_from,
        b.valid_to   as b_valid_to
    from {{ ref('dim_customer') }} a
    inner join {{ ref('dim_customer') }} b
        on a.customer_id = b.customer_id
       and a.customer_sk != b.customer_sk
       and a.customer_id != '-1'
       and a.valid_from < b.valid_to
       and b.valid_from < a.valid_to
),

product_overlaps as (
    select
        a.product_id,
        a.valid_from as a_valid_from,
        a.valid_to   as a_valid_to,
        b.valid_from as b_valid_from,
        b.valid_to   as b_valid_to
    from {{ ref('dim_product') }} a
    inner join {{ ref('dim_product') }} b
        on a.product_id = b.product_id
       and a.product_sk != b.product_sk
       and a.product_id != '-1'
       and a.valid_from < b.valid_to
       and b.valid_from < a.valid_to
)

select 'customer' as entity, customer_id as natural_key, a_valid_from, a_valid_to, b_valid_from, b_valid_to
from customer_overlaps

union all

select 'product' as entity, product_id as natural_key, a_valid_from, a_valid_to, b_valid_from, b_valid_to
from product_overlaps