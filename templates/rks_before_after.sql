{% macro before_field(field) %}
argMaxIf(`{{ field }}`{{ "".rjust(max_column_len-field|length) }}, RKS.min_Date_E, RKS.min_Date_E<=SVOD.`{{ date_field }}`) AS `B_{{ field }}`
{%- endmacro -%}

{% macro after_field(field) %}
argMinIf(`{{ field }}`{{ "".rjust(max_column_len-field|length) }}, RKS.min_Date_E, RKS.min_Date_E>=SVOD.`{{ date_field }}`) AS `A_{{ field }}`
{%- endmacro -%}

{% macro rks_before_after(container_by_container_number) %}
{% if container_by_container_number %}
-- SELECT * FROM audit.{{ user }}_rks_before_after_cont
CREATE OR REPLACE TABLE audit.{{ user }}_rks_before_after_cont
{% else %}
-- SELECT * FROM audit.{{ user }}_rks_before_after_eq
CREATE OR REPLACE TABLE audit.{{ user }}_rks_before_after_eq
{% endif %}
ENGINE = MergeTree()
ORDER BY tuple()
AS (

WITH 
SVOD AS (
	SELECT DISTINCT `{{ container_field }}`, `{{ date_field }}` FROM audit.{{ user }}_svod WHERE `{{ container_field }}`<>'' AND `{{ date_field }}` IS NOT NULL
--) SELECT * FROM SVOD
),
RKS_BEFORE_AFTER AS (
SELECT
	{{ '-'*174 }}
    {% if before_fields %}
	IF(`B_service_details_order_id`='', NULL, date_diff(DAY, SVOD.`{{ date_field }}`, `B_min_Date_E`)) AS `B_date_diff`,
	IF(`B_service_details_order_id`='', NULL, argMaxIf(RKS.min_Date_E, RKS.min_Date_E, RKS.min_Date_E<=SVOD.`{{ date_field }}`)) AS `B_min_Date_E`,
    {{ before_field(field='service_details_order_id') }},
        {% for rks_field in rks_fields if  "--" not in rks_field %}
    {{ before_field(field=rks_field) }},
        {% endfor %}
        {% if esu_id_columns %}
            {% for esu_id in esu_id_columns %}
    {{ before_field(field=esu_id+'_amount_in_rub_without_vat') }},
    {{ before_field(field=esu_id+'_amount_in_contract_currency_without_vat') }},
                {% if with_vat %}
    {{ before_field(field=esu_id+'_amount_in_rub_with_vat') }},
    {{ before_field(field=esu_id+'_amount_in_contract_currency_with_vat') }},
                {% endif %}
            {% endfor -%}
        {% else %}
    {{ before_field(field='amount_in_rub_without_vat') }},
    {{ before_field(field='amount_in_contract_currency_without_vat') }},
            {% if with_vat %}
    {{ before_field(field='amount_in_rub_with_vat') }},
    {{ before_field(field='amount_in_contract_currency_with_vat') }},
            {% endif %}
        {% endif %}
	{{ '-'*174 }}
    {% endif %}
    {% if after_fields %}
	IF(`A_service_details_order_id`='', NULL, date_diff(DAY, SVOD.`{{ date_field }}`, `A_min_Date_E`)) AS `A_date_diff`,
	IF(`A_service_details_order_id`='', NULL, argMinIf(RKS.min_Date_E, RKS.min_Date_E, RKS.min_Date_E>=SVOD.`{{ date_field }}`)) AS `A_min_Date_E`,
    {{ after_field(field='service_details_order_id') }},
        {% for rks_field in rks_fields if  "--" not in rks_field %}
    {{ after_field(field=rks_field) }},
        {% endfor %}
        {% if esu_id_columns %}
            {% for esu_id in esu_id_columns %}
    {{ after_field(field=esu_id+'_amount_in_rub_without_vat') }},
    {{ after_field(field=esu_id+'_amount_in_contract_currency_without_vat') }},
                {% if with_vat %}
    {{ after_field(field=esu_id+'_amount_in_rub_with_vat') }},
    {{ after_field(field=esu_id+'_amount_in_contract_currency_with_vat') }},
                {% endif %}
            {% endfor -%}
        {% else %}
    {{ after_field(field='amount_in_rub_without_vat') }},
    {{ after_field(field='amount_in_contract_currency_without_vat') }},
            {% if with_vat %}
    {{ after_field(field='amount_in_rub_with_vat') }},
    {{ after_field(field='amount_in_contract_currency_with_vat') }},
            {% endif %}
        {% endif %}
	{{ '-'*174 }}
    {% endif %}
	SVOD.`{{ container_field }}` AS `SVOD.{{ container_field }}`, SVOD.`{{ date_field }}` AS `SVOD.{{ date_field }}`
FROM 
    {% if container_by_container_number %}
	(SELECT * FROM audit.{{ user }}_rks_cont) AS RKS
    {% else %}
	(SELECT * FROM audit.{{ user }}_rks_eq) AS RKS    
    {% endif %}
	RIGHT JOIN SVOD ON SVOD.`{{ container_field }}` = RKS.`container_number`
GROUP BY
	 SVOD.`{{ container_field }}`, SVOD.`{{ date_field }}`
) SELECT * FROM RKS_BEFORE_AFTER

)
{% endmacro %}
{{ rks_before_after(container_by_container_number=True) }}
{# #}
{{ rks_before_after(container_by_container_number=False) }}
{# #}


