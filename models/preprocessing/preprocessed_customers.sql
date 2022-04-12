{{
    config(
        materialized='view'
    )
}}

with customers as (
    
    select {{
        dbt_utils.star(
            from=ref('stg_customers'), 
            except=[
                "customer_gender", 
                "policy_state"]
        )
    }}
    
    from {{ ref('stg_customers') }}
),

customer_gender as (

    {{
        dbt_ml_preprocessing.one_hot_encoder(
            source_table=ref('stg_customers'), 
            source_column='customer_gender',
            handle_unknown='ignore'
        )
    }}
),

policy_state as (

    {{
        dbt_ml_preprocessing.one_hot_encoder(
            source_table=ref('stg_customers'), 
            source_column='policy_state',
            handle_unknown='ignore'
        )
    }}
),

final as (
    select c.*,
           cg.is_customer_gender_male,
           cg.is_customer_gender_female,

           ps.is_policy_state_ca,
           ps.is_policy_state_wa,
           ps.is_policy_state_az,
           ps.is_policy_state_id,
           ps.is_policy_state_or,
           ps.is_policy_state_nv

    from customers c
    left join customer_gender cg 
      on c.policy_id = cg.policy_id
    left join policy_state ps
      on c.policy_id = ps.policy_id

)

select * from final
