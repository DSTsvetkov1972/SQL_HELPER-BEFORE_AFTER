SELECT * FROM {{ svod_table_name }}
{# #}


{# #}
{% include 'svod.sql' %}
{# #}


{# #}
{% include 'rks.sql' %}
{# #}


{# #}
{% if order_id_field %}
{% include 'rks_answer.sql' %}
{% endif %}
{# #}


{# #}
{% if date_field %}
{% include 'rks_before_after.sql' %}
{# #}

{# #}
{% include 'rks_before_after_answer.sql' %}
{% endif %}
{# #}


{# #}
{% include 'download.sql' %}
{# #}


{# #}
/* сформировано на основе conditions.json
{{ json }}
*/