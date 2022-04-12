{{
    config(
        materialized='view'
    )
}}

with claims_customer as (
    select 
           policy_id,
           
    from {{ source('raw', 'external_claims_customer')}}
)

select * from claims_customer