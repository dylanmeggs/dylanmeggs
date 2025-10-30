{% macro string_clean(column) %}
    nullif(nullif(upper(trim({{ column }})),''),'null')
{% endmacro %}