-- Standard DW logic for handling strings.

{% macro str_std(field) -%}

UPPER(NULLIF(LOWER(NULLIF(TRIM(REPLACE( {{ field }} , '_', ' ')), '')), 'null'))

{%- endmacro %}

-- Standard DW logic for handling strings with additional coalesce

{% macro str_std_c(field, no_data_string) -%}

UPPER(COALESCE(NULLIF(LOWER(NULLIF(TRIM(REPLACE( {{ field }} , '_', ' ')), '')), 'null'), '{{ no_data_string }}'))

{%- endmacro %}

{% macro str_email(field) -%}

UPPER(NULLIF(TRIM( {{ field }} ), ''))

{%- endmacro %}

{% macro str_no_replace(field) -%}

UPPER(NULLIF(TRIM( {{ field }} ), ''))

{%- endmacro %}

{% macro str_no_replace_lower(field) -%}

LOWER(NULLIF(TRIM( {{ field }} ), ''))

{%- endmacro %}

{% macro str_nrnu(field) -%}

NULLIF(NULLIF(TRIM( {{ field }} ), ''), 'null')

{%- endmacro %}

{% macro str_parse_url(field, url, search_str, split_str = '&', null_if_1 = '', null_if_2 = '') -%}

UPPER(COALESCE(NULLIF(TRIM( {{ field }} ), '{{ null_if_1 }}'),
	                     NULLIF(TRIM(SPLIT_PART(SPLIT_PART(LOWER(NULLIF(TRIM( {{ url }} ), '')),
	                                                       '{{ search_str }}', 2),
	                                            '{{ split_str }}', 1)),
	                            '{{ null_if_2 }}')))

{%- endmacro %}

{% macro str_replace_non_ascii(field) -%}

regexp_replace( {{ field }} , '[\u0080-\u00ff]', '', 'g')

{%- endmacro %}

{% macro str_to_bool(field) -%}

CASE
	WHEN UPPER( {{ field }} ) SIMILAR TO '%(TRUE|YES|1)%' THEN TRUE
	WHEN UPPER( {{ field }} ) SIMILAR TO '%(FALSE|NO|0)%' THEN FALSE
	END

{%- endmacro %}