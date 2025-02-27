{% macro build_unique_key(columns, delimiter='|', null_value='null_value') %}
    md5(
        '{{ this.identifier }}' ||
        {% for column in columns %}
            {{ 'coalesce(' ~ column ~ '::text, \'' ~ null_value ~ '\')' }}
            {%- if not loop.last -%} || {% endif %}
        {% endfor %}
    )
{% endmacro %}