{{ config(materialized='view') }}

with source as (select * from {{ source('bronze', 'hr_employees') }}),

deduplicated as (
    select 
        *,
        row_number() over(
            partition by employee_id
            order by updated_at desc, _ingest_ts desc
        ) as _rn
    from source

),

renamed as (
    select
        employee_id,
        trim(first_name)                       as first_name,
        trim(last_name)                        as last_name,
        work_email,
        job_title,
        department,
        store_id,
        manager_employee_id,
        upper(trim(country_code))              as country_code,
        cast(hire_date as date)                as hire_date,
        cast(termination_date as date)         as termination_date,
        employment_status,
        cast(created_at as timestamp)          as created_at,
        cast(updated_at as timestamp)          as updated_at
    from deduplicated
    where _rn = 1
)

select * from renamed