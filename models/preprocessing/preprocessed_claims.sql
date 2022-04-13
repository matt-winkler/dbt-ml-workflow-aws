{{
    config(
        materialized='view'
    )
}}

with claims as (
    
    select {{
        dbt_utils.star(
            from=ref('stg_claims'), 
            except=[
                "driver_relationship", 
                "collision_type", 
                "authorities_contacted", 
                "incident_type", 
                "incident_severity", 
                "injury_claim",
                "police_report_available"]
        )
    }},
    
    case 
      when incident_severity = 'Minor' then 0.0
      when incident_severity = 'Major' then 1.0
      when incident_severity = 'Totaled' then 2.0
    else  0.0 end as incident_severity
    ,

    case
      when num_injuries = 0 then 0 
    else injury_claim end as injury_claim
    ,

    case
      when police_report_available = 'No' then 0
    else 1 end as police_report_available
    
    from {{ ref('stg_claims') }}
),

driver_relationship as (

    {{
        dbt_ml_preprocessing.one_hot_encoder(
            source_table=ref('stg_claims'), 
            source_column='driver_relationship',
            handle_unknown='ignore'
        )
    }}
),

collision_type as (

    {{
        dbt_ml_preprocessing.one_hot_encoder(
            source_table=ref('stg_claims'), 
            source_column='collision_type',
            handle_unknown='ignore'
        )
    }}
),

authorities_contacted as (

    {{
        dbt_ml_preprocessing.one_hot_encoder(
            source_table=ref('stg_claims'), 
            source_column='authorities_contacted',
            handle_unknown='ignore'
        )
    }}
),

incident_type as (

    {{
        dbt_ml_preprocessing.one_hot_encoder(
            source_table=ref('stg_claims'), 
            source_column='incident_type',
            handle_unknown='ignore'
        )
    }}
),

final as (
    select c.*,
           dr.is_driver_relationship_self,
           dr.is_driver_relationship_na,
           dr.is_driver_relationship_spouse,
           dr.is_driver_relationship_child,
           dr.is_driver_relationship_other,
           
           ct.is_collision_type_front,
           ct.is_collision_type_rear,
           ct.is_collision_type_side,
           ct.is_collision_type_na,

           ac.is_authorities_contacted_police,
           ac.is_authorities_contacted_none,
           ac.is_authorities_contacted_fire,
           ac.is_authorities_contacted_ambulance,

           it.is_incident_type_collision,
           it.is_incident_type_break_in,
           it.is_incident_type_theft
    from claims c
    left join driver_relationship dr 
      on c.policy_id = dr.policy_id
    left join collision_type ct
      on c.policy_id = ct.policy_id
    left join authorities_contacted ac 
      on c.policy_id = ac.policy_id
    left join incident_type it
      on c.policy_id = it.policy_id
)

select * from final
