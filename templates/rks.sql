{% macro rks(container_by_container_number) %}
{% if container_by_container_number %}
-- SELECT * FROM audit.{{ user }}_rks_cont
CREATE OR REPLACE TABLE audit.{{ user }}_rks_cont
{% else %}
-- SELECT * FROM audit.{{ user }}_rks_eq
CREATE OR REPLACE TABLE audit.{{ user }}_rks_eq
{% endif %}
ENGINE = MergeTree()
ORDER BY tuple()
AS (

WITH 
SVOD AS (
	SELECT DISTINCT `{{ container_field }}` FROM audit.{{ user }}_svod WHERE `{{ container_field }}` <>''
--) SELECT * FROM SVOD    
),
RKS AS (
SELECT
    {% include 'block_select_fields.sql' %} 
FROM 
	(SELECT DISTINCT * FROM history.rks__directly WHERE client_number_id <> '0009309810' AND is_deleted=False) AS RD
	INNER JOIN SVOD ON container_number = SVOD.`{{ container_field }}`
WHERE
    {% for where_condition in where_conditions %}
    {%- if not loop.last or esu_id_columns%}
    {{ where_condition }} AND
    {% else %}
    {{ where_condition }}
    {% endif %}
    {% endfor %}
    {% if esu_id_columns %}
    esu_id IN ('{{ "', '".join(esu_id_columns) }}')
    {% endif %}
GROUP BY
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
HAVING
	(amount_in_rub_without_vat<>0 OR 
	amount_in_contract_currency_without_vat<>0 OR
	amount_in_rub_with_vat<>0 OR
    amount_in_contract_currency_with_vat<>0)
{% if esu_id_columns %}
--) SELECT * FROM RKS 
)
SELECT
	min(Date_E) AS `min_Date_E`,
    `service_details_order_id`,`container_number`,
    {% for rks_field in rks_fields if  "--" not in rks_field %}
    `{{ rks_field }}`,
    {% endfor %}
    {% if esu_id_columns %}
    {% for esu_id in esu_id_columns %}
    sumIf(amount_in_rub_with_vat, esu_id='{{esu_id}}') AS `{{esu_id}}_amount_in_rub_with_vat`,    
    sumIf(amount_in_rub_without_vat, esu_id='{{esu_id}}') AS `{{esu_id}}_amount_in_rub_without_vat`,
    sumIf(amount_in_contract_currency_with_vat, esu_id='{{esu_id}}') AS `{{esu_id}}_amount_in_contract_currency_with_vat`,    
    sumIf(amount_in_contract_currency_without_vat, esu_id='{{esu_id}}') AS `{{esu_id}}_amount_in_contract_currency_without_vat`{% if not loop.last%},{% endif %}    
    {% endfor -%}    
    {% else %}
    sum(amount_in_rub_with_vat) AS `amount_in_rub_with_vat`,    
    sum(amount_in_rub_without_vat) AS `amount_in_rub_without_vat`,
    sum(amount_in_contract_currency_with_vat) AS `amount_in_contract_currency_with_vat`,    
    sum(amount_in_contract_currency_without_vat) AS `amount_in_contract_currency_without_vat`    
    {% endif -%}    
FROM
	RKS
GROUP BY	
    {% for rks_field in rks_fields if  "--" not in rks_field %}
    `{{ rks_field }}`,
    {% endfor %}
    `service_details_order_id`,
    `container_number`
{% else %}
) SELECT * FROM RKS 
{% endif %}

)
{% endmacro %}
{{ rks(container_by_container_number=True) }}
{# #}

{{ rks(container_by_container_number=False) }}
{# #}

