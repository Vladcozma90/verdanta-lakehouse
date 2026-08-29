{{ config(materialized='table') }}

with employees as ( select * from {{ ref('stg_hr__employees') }} ),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['employee_id']) }} as employee_sk,
        employee_id,
        first_name,
        last_name,
        work_email,
        job_title,
        department,
        store_id,
        manager_employee_id,
        country_code,
        hire_date,
        termination_date,
        employment_status,
        (employment_status = 'ACTIVE')          as is_active
    from employees
),

unknown_member as (
    select
        {{ dbt_utils.generate_surrogate_key(["'-1'"]) }} as employee_sk,
        '-1'      as employee_id,
        'Unknown' as first_name,
        'Unknown' as last_name,
        null      as work_email,
        'Unknown' as job_title,
        'Unknown' as department,
        null      as store_id,
        null      as manager_employee_id,
        'XX'      as country_code,
        null      as hire_date,
        null      as termination_date,
        'UNKNOWN' as employment_status,
        false     as is_active
)

select * from final
union all
select * from unknown_member