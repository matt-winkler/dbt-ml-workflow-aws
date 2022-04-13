{{
    config(
        materialized='view'
    )
}}

-- {{ ref('fraud_detection_model') }}

select 
  fraud_detection_model(
      {{dbt_utils.star(from=ref('dataset'), except=["policy_id", "fraud"] ) }}
  ) as result

from {{ ref('dataset')}}