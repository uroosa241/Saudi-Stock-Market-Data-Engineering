-- Schema naming:
--   no custom schema   -> target.schema
--   target 'ci'        -> <target.schema>_<custom schema>  (e.g. ci_pr_12_silver, ci_pr_12_gold), isolated per PR
--   any other target   -> custom schema as-is (silver, gold)
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.name == 'ci' -%}
        {{ target.schema ~ '_' ~ (custom_schema_name | trim) }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
