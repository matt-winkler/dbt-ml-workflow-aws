{% set cut_point = -2 %}

with result_data as (
    select * from {{ ref('fraud_detection_results')}}
),

refined as (
   
   select policy_id,
          fraud,
          pred_result as raw_pred_result,
          CASE WHEN pred_result >= {{ cut_point }} then 1 else 0 end as pred_result
   from  result_data
),

stats as (
   select fraud, pred_result, count(*) as records
   from refined
   group by 1,2
), 

stats_grouped_by_pred as (
  select  
          raw_pred_result,
          fraud,
          pred_result,
          count(*) as records
   from refined
   group by 1,2,3

)

select * from stats