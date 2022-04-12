{{
    config(
        materialized='view'
    )
}}

with claims as (
    select * from {{ source('raw', 'external_customers') }}
)

select * from claims