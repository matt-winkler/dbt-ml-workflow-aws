{% macro check_is_model_ready(model_name) %}

  {% set check_sql %}
     select * 
     from STV_ML_MODEL_INFO 
     where model_name = {{ model_name }}
     and schema_name = {{target.schema}}
     and model_state = 'Model is Ready'
  {% endset %}

  {% set results = run_query(check_sql) %}

  {{ return(results) }}

{% endmacro %}