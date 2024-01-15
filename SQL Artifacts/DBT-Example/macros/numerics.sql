-- Standard DW logic for turning int (cent values) into a numeric

{% macro int_cents_to_num_dollar(field, rounding=2) -%}

ROUND(CAST(COALESCE( {{ field }} , 0) AS NUMERIC) / 100, {{ rounding }})

{%- endmacro %}

{% macro int_cents_to_num_dollar_with_null(field, rounding=2) -%}

ROUND(CAST( {{ field }} AS NUMERIC) / 100, {{ rounding }})

{%- endmacro %}

{% macro int_calc_period(field_1, field_2, period_days=30.4) -%}

FLOOR((( {{ field_1 }} :: DATE) - ( {{ field_2 }} :: DATE)) :: NUMERIC / {{ period_days }} ) :: INTEGER + 1

{%- endmacro %}