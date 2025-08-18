SELECT * FROM {{ svod_table_name }}
{# #}

{% include 'rks.sql' %}
{# #}

{% if order_id_field %}
{% include 'rks_answer.sql' %}
{# #}
{% endif %}

{% include 'rks_before_after.sql' %}
{# #}

{% include 'rks_before_after_answer.sql' %}
{# #}

{% include 'download.sql' %}
