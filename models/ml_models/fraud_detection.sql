{{
    config(
        materialized='ml_model',
        target_variable='fraud',
        prediction_function_name='dbt_ml_workflow_fraud_model',
        iam_role='arn:aws:iam::486758181003:role/dbt-ml-workflow--redshift-model-manager-role',
        s3_bucket='dbt-ml-workflow--data-bucket',
        auto_ml='ON',
        model_type='XGBOOST',
        problem_type='binary_classification',
        objective='binary:logistic',
        hyperparameters='default'
    )
}}

select {{ 
    dbt_utils.star(
        from=ref('dataset'), 
        except=["policy_id"] 
        ) 
    }} 
from {{ ref('dataset') }} 
-- use a where filter + macro for training mode to hide some data