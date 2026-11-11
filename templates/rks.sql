{% macro rks(container_by_container_number) %}
{% if container_by_container_number %}
-- SELECT * FROM audit.{{ project }}_rks_cont
CREATE OR REPLACE TABLE audit.{{ project }}_rks_cont
{% else %}
-- SELECT * FROM audit.{{ project }}_rks_eq
CREATE OR REPLACE TABLE audit.{{ project }}_rks_eq
{% endif %}
ENGINE = MergeTree()
ORDER BY tuple()
AS (

WITH 
SVOD AS (
	SELECT DISTINCT `{{ container_field }}`{% if esu_id_field %}, `{{ esu_id_field }}`{% endif %} FROM audit.{{ project }}_svod WHERE `{{ container_field }}` <>''
--) SELECT * FROM SVOD    
),
RKS AS (
SELECT
    {% include 'block_select_fields.sql' %} 
FROM 
	(SELECT DISTINCT * FROM history.rks__directly WHERE client_number_id <> '0009309810' AND is_deleted=False) AS RD
	INNER JOIN SVOD ON
        container_number = SVOD.`{{ container_field }}`{% if esu_id_field %} AND
        RD.esu_id = SVOD.`{{ esu_id_field }}`{% endif %} 
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
    `{{ rks_field }}`,
    {% endfor %}
    {% if esu_id_columns or esu_id_field %}
    `esu_id`,
    {% endif %}
    `service_details_order_id`,
    `container_number`
HAVING
	(sum_amount_in_rub_without_vat<>0 OR 
	sum_amount_in_contract_currency_without_vat<>0 OR
	sum_amount_in_rub_with_vat<>0 OR
    sum_amount_in_contract_currency_with_vat<>0)
{% if esu_id_columns %}
--) SELECT * FROM RKS 
)
SELECT
	min(Date_E) AS `min_Date_E`,
	`container_number`,`service_details_order_id`,
    {% if esu_id_field %}
    `esu_id`,
    {% endif %}
    {% for rks_field in rks_fields if  "--" not in rks_field %}
    `{{ rks_field }}`,
    {% endfor %}
    {% if esu_id_columns %}
        {% for esu_id in esu_id_columns %}
    sumIf(sum_amount_in_rub_without_vat, esu_id='{{esu_id}}') AS `{{esu_id}}_sum_amount_in_rub_without_vat`,
    anyIf(arr_amount_in_rub_without_vat, esu_id='{{esu_id}}') AS `{{esu_id}}_arr_amount_in_rub_without_vat`,
	sumIf(sum_amount_in_contract_currency_without_vat, esu_id='{{esu_id}}') AS `{{esu_id}}_sum_amount_in_contract_currency_without_vat`,
    anyIf(arr_amount_in_contract_currency_without_vat, esu_id='{{esu_id}}') AS `{{esu_id}}_arr_amount_in_contract_currency_without_vat`{% if not loop.last or with_vat %},
            {% endif %}
            {% if with_vat %}
	sumIf(sum_amount_in_rub_with_vat, esu_id='{{esu_id}}') AS `{{esu_id}}_sum_amount_in_rub_with_vat`,
	anyIf(sum_amount_in_rub_with_vat, esu_id='{{esu_id}}') AS `{{esu_id}}_arr_amount_in_rub_with_vat`,    
	sumIf(sum_amount_in_contract_currency_with_vat, esu_id='{{esu_id}}') AS `{{esu_id}}_sum_amount_in_contract_currency_with_vat`{% if not loop.last%},{% endif %}
            {% endif %}
        {% endfor -%}    
    {% else %}
    sum(sum_amount_in_rub_without_vat) AS `sum_amount_in_rub_without_vat`,
    any(arr_amount_in_rub_without_vat) AS `arr_amount_in_rub_without_vat`,    
	sum(sum_amount_in_contract_currency_without_vat) AS `sum_amount_in_contract_currency_without_vat`,
        {% if with_vat %}    
	sum(sum_amount_in_rub_with_vat) AS `sum_amount_in_rub_with_vat`,
	any(arr_amount_in_rub_with_vat) AS `arr_amount_in_rub_with_vat`,    
	sum(sum_amount_in_contract_currency_with_vat) AS `sum_amount_in_contract_currency_with_vat`
        {% endif %}
    {% endif %}    
FROM
	RKS
GROUP BY	
    {% for rks_field in rks_fields if  "--" not in rks_field %}
    `{{ rks_field }}`,
    {% endfor %}
    `container_number`,
    `service_details_order_id`{% if esu_id_field %},
    `esu_id`
    {% endif %}
{% else %}
) SELECT * FROM RKS 
{% endif %}

);
{% endmacro %}
{{ rks(container_by_container_number=True) }}
{# #}

{{ rks(container_by_container_number=False) }}
{# #}

