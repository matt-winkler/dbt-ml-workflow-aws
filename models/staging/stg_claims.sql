{{
    config(
        materialized='view'
    )
}}

with claims as (
    
    select {{ dbt_utils.star(from=source('raw', 'external_claims'), except=["driver_relationship", "collision_type", "authorities_contacted", "incident_type"]) }},
            
           -- get rid of invalid N/A values for next step
           case when driver_relationship = 'N/A' then 'na' else driver_relationship end as driver_relationship,
           case when collision_type = 'N/A' then 'na' else collision_type end as collision_type,
           case when authorities_contacted = 'N/A' then 'na' else authorities_contacted end as authorities_contacted,
           case 
             when incident_type = 'N/A' then 'na' 
             when incident_type = 'Break-in' then 'break_in'
           else incident_type end as incident_type
    
    from   {{ source('raw', 'external_claims') }}

)


select * from claims
