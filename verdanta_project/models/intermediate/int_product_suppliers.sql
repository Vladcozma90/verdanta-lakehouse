{{ config(materialized='view') }}

with products as (select * from {{ ref('stg_pim__products') }}),

exploded as (
    select
        product_id,
        explode(supplier_ids) as supplier_id
    from products
)

select * from exploded