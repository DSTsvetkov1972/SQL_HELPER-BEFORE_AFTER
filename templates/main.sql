SELECT * FROM {{ svod_table_name }}

{% include 'rks.sql' %}
{# #}

{% include 'rks_answer.sql' %}
{# #}

{% include 'rks_before_after.sql' %}
{# #}

{% include 'rks_before_after_answer.sql' %}
{# #}

{% include 'download.sql' %}
