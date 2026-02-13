{% macro before_cond(x) %}
{% if x is string %}{{ "".rjust(max_column_len-x|length) }}, RKS.min_Date_E, RKS.min_Date_E<=SVOD.`{{ date_field }}`{% else %}{{ "".rjust(max_column_len-x) }}, RKS.min_Date_E, RKS.min_Date_E<=SVOD.`{{ date_field }}`{% endif %}
{%- endmacro -%}

{% macro after_cond(x) %}
{% if x is string %}{{ "".rjust(max_column_len-x|length) }}, RKS.min_Date_E, RKS.min_Date_E>=SVOD.`{{ date_field }}`{% else %}{{ "".rjust(max_column_len-x) }}, RKS.min_Date_E, RKS.min_Date_E>=SVOD.`{{ date_field }}`{% endif %}
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
	IF(`B_service_details_order_id`='', NULL, date_diff(DAY, SVOD.`{{ date_field }}`, `B_min_Date_E`)) AS `B_date_diff`,
	IF(`B_service_details_order_id`='', NULL, argMaxIf(RKS.min_Date_E, RKS.min_Date_E, RKS.min_Date_E<=SVOD.`{{ date_field }}`)) AS `B_min_Date_E`,
	argMaxIf(`service_details_order_id`{{ "".rjust(max_column_len-'service_details_order_id'|length) }}, RKS.min_Date_E, RKS.min_Date_E<=SVOD.`{{ date_field }}`) AS `B_service_details_order_id`,
    {% for rks_field in rks_fields if  "--" not in rks_field %}
    argMaxIf(`{{ rks_field }}`{{ before_cond(x=rks_field) }}) AS `B_{{ rks_field }}`,
    {% endfor %}
    {% if esu_id_columns %}
        {% for esu_id in esu_id_columns %}
    argMaxIf(`{{ esu_id }}_amount_in_rub_without_vat`{{ before_cond(x=36) }}) AS `B_{{ esu_id }}_amount_in_rub_without_vat`,
    argMaxIf(`{{ esu_id }}_amount_in_contract_currency_without_vat`{{ before_cond(x=50) }}) AS `B_{{ esu_id }}_amount_in_contract_currency_without_vat`,
            {% if with_vat %}
    argMaxIf(`{{ esu_id }}_amount_in_rub_with_vat`{{ before_cond(x=36) }}) AS `B_{{ esu_id }}_amount_in_rub_with_vat`,
    argMaxIf(`{{ esu_id }}_amount_in_contract_currency_with_vat`{{ before_cond(x=50) }}) AS `B_{{ esu_id }}_amount_in_contract_currency_with_vat`,
            {% endif %}
        {% endfor -%}
    {% else %}
    argMaxIf(`amount_in_rub_without_vat`{{ before_cond(x=25) }}) AS `B_amount_in_rub_without_vat`,    
    argMaxIf(`amount_in_contract_currency_without_vat`{{ before_cond(x=39) }}) AS `B_amount_in_contract_currency_without_vat`,
        {% if with_vat %}
    argMaxIf(`amount_in_rub_with_vat`{{ before_cond(x=25) }}) AS `B_amount_in_rub_with_vat`,
    argMaxIf(`amount_in_contract_currency_with_vat`{{ before_cond(x=39) }}) AS `B_amount_in_contract_currency_with_vat`,
        {% endif %}
    {% endif %}
	------------------------------------------------------------------------------------------------            
	IF(`A_service_details_order_id`='', NULL, date_diff(DAY, SVOD.`{{ date_field }}`, `A_min_Date_E`)) AS `A_date_diff`,
	IF(`A_service_details_order_id`='', NULL, argMinIf(RKS.min_Date_E, RKS.min_Date_E, RKS.min_Date_E>=SVOD.`{{ date_field }}`)) AS `A_min_Date_E`,
	argMinIf(`service_details_order_id`{{ "".rjust(max_column_len-'service_details_order_id'|length) }}, RKS.min_Date_E, RKS.min_Date_E<=SVOD.`{{ date_field }}`) AS `A_service_details_order_id`,
    {% for rks_field in rks_fields if  "--" not in rks_field %}
    argMinIf(`{{ rks_field }}`{{ after_cond(x=rks_field) }}) AS `A_{{ rks_field }}`,
    {% endfor %}
    {% if esu_id_columns %}
        {% for esu_id in esu_id_columns %}
    argMinIf(`{{ esu_id }}_amount_in_rub_without_vat`{{ after_cond(x=36) }}) AS `A_{{ esu_id }}_amount_in_rub_without_vat`,
    argMinIf(`{{ esu_id }}_amount_in_contract_currency_without_vat`{{ after_cond(x=50) }}) AS `A_{{ esu_id }}_amount_in_contract_currency_without_vat`,
            {% if with_vat %}
    argMinIf(`{{ esu_id }}_amount_in_rub_with_vat`{{ after_cond(x=36) }}) AS `A_{{ esu_id }}_amount_in_rub_with_vat`,
    argMinIf(`{{ esu_id }}_amount_in_contract_currency_with_vat`{{ after_cond(x=50) }}) AS `A_{{ esu_id }}_amount_in_contract_currency_with_vat`,
            {% endif %}
        {% endfor -%}
    {% else %}
    argMinIf(`amount_in_rub_without_vat`{{ after_cond(x=25) }}) AS `A_amount_in_rub_without_vat`,    
    argMinIf(`amount_in_contract_currency_without_vat`{{ after_cond(x=39) }}) AS `A_amount_in_contract_currency_without_vat`,
        {% if with_vat %}
    argMinIf(`amount_in_rub_with_vat`{{ after_cond(x=25) }}) AS `A_amount_in_rub_with_vat`,
    argMinIf(`amount_in_contract_currency_with_vat`{{ after_cond(x=39) }}) AS `A_amount_in_contract_currency_with_vat`,
        {% endif %}
    {% endif %}
	------------------------------------------------------------------------------------------------  
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


