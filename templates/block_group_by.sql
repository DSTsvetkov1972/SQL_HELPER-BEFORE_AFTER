    {% for rks_field in rks_fields if  "--" not in rks_field %}
    {% if rks_field == 'document_reasons_number_cleaned' %}
    `document_reasons_number_striped`,
    `document_reasons_number_letters`,
    `document_reasons_number_figures`,
    `document_reasons_number_cleaned`,
    {% else %}
    `{{ rks_field }}`,
    {% endif %}
    {% endfor %}
    {% if esu_id_columns %}
    `esu_id`,
    {% endif %}
    `service_details_order_id`,
    `container_number`

