{% set cut_point = -1 %}

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
   select 
          sum(fraud)::numeric(10,2) / count(*)::numeric(10,2) as actual_fraud_rate, 
          sum(pred_result)::numeric(10,2) / count(*)::numeric(10,2) as predicted_fraud_rate,
   
          sum(case when fraud = pred_result then 1.00 else 0.00 end) / count(*)::numeric(10,2) as accuracy,

          sum(case when fraud = 1 and pred_result = 1 then 1.00 else 0.00 end) /
          sum(pred_result)::numeric(10,2) as precision,

          sum(case when fraud = 1 and pred_result = 1 then 1.00 else 0.00 end) /
          sum(fraud)::numeric(10,2) as recall   

   from refined
), 

stats_grouped_by_pred as (
  select  
          raw_pred_result,
          count(*) as num_records,
          sum(fraud)::numeric(10,2) / count(*)::numeric(10,2) as actual_fraud_rate, 
          sum(pred_result)::numeric(10,2) / count(*)::numeric(10,2) as predicted_fraud_rate,
   
          sum(case when fraud = pred_result then 1.00 else 0.00 end)::numeric(10,2) / count(*)::numeric(10,2) as accuracy,

          
          sum(case when fraud = 1 and pred_result = 1 then 1.00 else 0.00 end)::numeric(10,2) /
          sum(case when pred_result >= {{ cut_point }} then 1 else 0 end)::numeric(10,2) as precision,

         case 
            when sum(fraud) > 0 then 
              sum(case when fraud = 1 and pred_result = 1 then 1.00 else 0.00 end)::numeric(10,2) /
              sum(fraud)::numeric(10,2)
            else -1 
         end as recall

   from refined
   group by 1

)

select * from stats