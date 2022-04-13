{{
    config(
        materialized='model',
        ml_config={
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
            'hyperparameters': 'default'
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