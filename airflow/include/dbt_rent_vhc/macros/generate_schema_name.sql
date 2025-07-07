{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name }}
    {%- else -%}
        {{ node.config.schema }}
    {%- endif -%}
{%- endmacro %}