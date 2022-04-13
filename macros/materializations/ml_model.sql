{% macro default__create_model_as(relation, sql) %}
    {{ exceptions.raise_compiler_error("ML model creation is not implemented for the default adapter") }}
{% endmacro %}

{% macro redshift__create_model_as(relation, sql) %}
    {%- set config = config.get('config', {}) -%}
    {%- set raw_labels = config.get('labels', {}) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {{ sql_header if sql_header is not none }}

    commit; 

    drop model if exists ml_testing;

    create model ml_testing
    from ( {{ sql }} )
    target config['target_variable']
    function config['prediction_function_name']
    iam_role 'arn:aws:iam::486758181003:role/dbt-ml-workflow--redshift-model-manager-role'
    auto config['auto_ml']
    model_type config['model_type']
    problem_type config['problem_type']
    objective config['objective']
    hyperparameters config['hyperparameters']
    settings (
        s3_bucket 'dbt-ml-workflow--data-bucket'
    );

{% endmacro %}

{% materialization ml_model, adapter='redshift' -%}
    {%- set identifier = ml_model['alias'] -%}
    {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}    
    {%- set target_relation = api.Relation.create(database=database, schema=schema, identifier=identifier) -%}

    {{ run_hooks(pre_hooks) }}

    {% call statement('main') -%}
        {{ create_model_as(target_relation, sql) }}
    {% endcall -%}

    {{ run_hooks(post_hooks) }}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}