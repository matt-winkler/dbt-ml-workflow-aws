{{
    config(
        materialized='table'
    )
}}

select 
  policy_id,
  fraud,
  {{ ref('fraud_detection_model') }}(
      {{dbt_utils.star(from=ref('dataset'), except=["policy_id", "fraud"] ) }}
  ) as pred_result

from {{ ref('dataset')}}
where policy_id > 4000