{% macro default__create_model_as(relation, sql) %}
    {{ exceptions.raise_compiler_error("ML model creation is not implemented for the default adapter") }}
{% endmacro %}

{% macro redshift__create_model_as(relation, sql) %}
    {%- set ml_config = config.get('ml_config', {}) -%}
    {%- set sql_header = config.get('sql_header', none) -%}
    
    {%- set target_variable = ml_config.get('target_variable', none) -%}
    {%- set prediction_function_name = ml_config.get('prediction_function_name', none) -%}
    {%- set iam_role = ml_config.get('iam_role', none) -%}
    {%- set auto_ml = ml_config.get('auto_ml', none) -%}
    {%- set model_type = ml_config.get('model_type', none) -%}
    {%- set problem_type = ml_config.get('problem_type', none) -%}
    {%- set objective = ml_config.get('objective', none) -%}
    {%- set preprocessors = ml_config.get('preprocessors', none) -%}
    {%- set hyperparameters = ml_config.get('hyperparameters', none) -%}
    {%- set s3_bucket = ml_config.get('s3_bucket', none) -%}

    {{ sql_header if sql_header is not none }}

    commit; 

    drop model if exists {{ prediction_function_name }};

    create model {{ prediction_function_name }}
    from ( {{ sql }} )
    target {{ target_variable }}
    function {{ prediction_function_name }}
    iam_role '{{ iam_role }}'
    auto {{ auto_ml }}
    model_type {{ model_type }}
    -- problem_type {{ problem_type }}
    objective '{{ objective }}'
    preprocessors '{{ preprocessors }}'
    hyperparameters {{ hyperparameters }}
    settings (
        s3_bucket '{{ s3_bucket }}'
    );

{% endmacro %}

{% materialization model, adapter='redshift' -%}
    {%- set identifier = model['alias'] -%}   
    {%- set target_relation = api.Relation.create(database=database, schema=schema, identifier=identifier) -%}

    {{ run_hooks(pre_hooks) }}

    {% call statement('main') -%}
        {{ redshift__create_model_as(target_relation, sql) }}
    {% endcall -%}

    {{ run_hooks(post_hooks) }}

    {{ return( {'relations': [target_relation.identifier]} ) }}
{% endmaterialization %}