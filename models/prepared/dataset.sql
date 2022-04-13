{{
    config(
        materialized='table'
    )
}}

with claims as (
    select * from {{ ref('preprocessed_claims') }}
),

customers as (
    select * from {{ ref('preprocessed_customers') }}
),

joined as (
    select cl.policy_id,
           cl.is_incident_type_theft,
           cl.is_incident_type_break_in,
           cl.is_incident_type_collision,
            
           cst.is_policy_state_ca,
           cst.is_policy_state_wa,
           cst.is_policy_state_az,
           cst.is_policy_state_id,
           cst.is_policy_state_or,
           cst.is_policy_state_nv,

           cst.policy_deductible,
           cst.policy_annual_premium,
           cl.num_witnesses,
           cl.incident_month,
           cl.incident_day,
           cl.incident_dow,
           cl.incident_hour,
           cst.is_customer_gender_female,
           cst.is_customer_gender_male,
           cst.num_insurers_past_5_years,
           cl.total_claim_amount,

           cl.is_authorities_contacted_police,
           cl.is_authorities_contacted_none,
           cl.is_authorities_contacted_fire,
           cl.is_authorities_contacted_ambulance,

           cl.is_collision_type_front,
           cl.is_collision_type_rear,
           cl.is_collision_type_side,
           cl.is_collision_type_na,

           cst.customer_age,
           cst.customer_education,

           cl.is_driver_relationship_self,
           cl.is_driver_relationship_na,
           cl.is_driver_relationship_spouse,
           cl.is_driver_relationship_child,
           cl.is_driver_relationship_other,

           cl.injury_claim,
           cl.vehicle_claim,
           cl.incident_severity,

           cst.num_claims_past_year,
           cst.months_as_customer,
           
           cst.auto_year,
           cl.num_vehicles_involved,
           cl.num_injuries,
           cst.policy_liability,
           cl.police_report_available,

           cl.fraud

    from  claims cl
    join customers cst
      on cl.policy_id = cst.policy_id
)

select * from joined