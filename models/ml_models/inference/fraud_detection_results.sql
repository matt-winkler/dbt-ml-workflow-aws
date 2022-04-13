{{
    config(
        materialized='view'
    )
}}

select 
  policy_id,
  fraud,
  {{ ref('fraud_detection_model').identifier }}(
      {{dbt_utils.star(from=ref('dataset'), except=["policy_id", "fraud"] ) }}
  ) as pred_result

from {{ ref('dataset')}}