-- macros/generate_schema_name.sql
-- Overrides dbt's default schema naming so custom schema values in
-- dbt_project.yml are used as-is (not prefixed with your target schema).
-- Without this, "staging" becomes "your_target_schema_staging" in Snowflake.

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema | upper }}
    {%- else -%}
        {{ custom_schema_name | upper }}
    {%- endif -%}
{%- endmacro %}


-- macros/cents_to_dollars.sql
-- Reusable conversion macro if you ever store amounts in cents
{% macro cents_to_dollars(column_name, scale=2) %}
    round({{ column_name }} / 100, {{ scale }})
{% endmacro %}


-- macros/safe_divide.sql
-- Avoids division-by-zero errors in all models
{% macro safe_divide(numerator, denominator) %}
    case
        when {{ denominator }} = 0 or {{ denominator }} is null then null
        else {{ numerator }} / {{ denominator }}
    end
{% endmacro %}


-- macros/log_model_run.sql
-- Post-hook macro to log model completion to audit table
{% macro log_model_run(model_name) %}
    insert into NYC_TAXI.AUDIT.PIPELINE_RUNS (dag_id, run_id, target_month, rows_ingested, status, completed_at)
    select
        'dbt' as dag_id,
        '{{ invocation_id }}' as run_id,
        null as target_month,
        count(*) as rows_ingested,
        'dbt_model_complete' as status,
        current_timestamp() as completed_at
    from {{ this }}
{% endmacro %}
