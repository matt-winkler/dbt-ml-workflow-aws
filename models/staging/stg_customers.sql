{{
    config(
        materialized='view'
    )
}}

with customers as (
    
    select {{ dbt_utils.star(from=source('raw', 'external_customers'), except=["policy_state", "policy_deductable", "policy_liability", "customer_education"]) }},
           
            case
              when policy_state = 'N/A' then 'other'
            else policy_state end as policy_state,

            policy_deductable as policy_deductible,

            case
              when policy_liability = '15/30' then 0
              when policy_liability = '25/50' then 1
              when policy_liability = '30/60' then 2
              when policy_liability = '100/200' then 3
            else 0 end as policy_liability
            ,

            case 
              when customer_education = 'High School' then 1
              when customer_education = 'Associate' then 2
              when customer_education = 'Bachelor' then 3
              when customer_education = 'Advanced Degree' then 4
            else 0 end as customer_education

    from   {{ source('raw', 'external_customers') }}

)

select * from customers
