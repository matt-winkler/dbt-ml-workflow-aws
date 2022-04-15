{{
    config(
        materialized='machine_learning_model',
        ml_config={
            'drop_existing_model': 'True',
            'target_variable': 'fraud',
            'prediction_function_name': 'fraud_detection_model',
            'iam_role': 'arn:aws:iam::486758181003:role/dbt-ml-workflow--redshift-model-manager-role',
            's3_bucket': 'dbt-ml-workflow--data-bucket',
            'max_cells': 1000000,
            'auto_ml': 'OFF',
            'model_type': 'XGBOOST',
            'problem_type': 'binary_classification',
            'objective': 'binary:hinge',
            'preprocessors': 'none',
            'hyperparameters': "DEFAULT EXCEPT (
                    eval_metric 'auc',
                    max_depth '5',
                    num_round '250',
                    scale_pos_weight '75'
                )"
        }
    )
}}

select {{ 
    dbt_utils.star(
        from=ref('dataset'), 
        except=["policy_id"] 
        ) 
    }} 
from {{ ref('dataset') }} 
where policy_id <= 4000