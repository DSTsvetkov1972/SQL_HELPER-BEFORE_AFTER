--download
WITH
SVOD AS (
SELECT * FROM {{ svod_table_name }}
),
RKS AS (
SELECT * FROM audit.{{ user }}_rks
),
BEFORE_AFTER AS (
SELECT * FROM audit.{{ user }}_rks_before_after
),
ANSWER AS (
SELECT
	SVOD.*,
{% if order_id_field %}
    '<==SVOD RKS==>',
    RKS.`подтянуто по`, RKS.`min_Date_E`,
{% for rks_field in rks_fields if  "--" not in rks_field %}
    RKS.{{ rks_field }},
{% endfor -%}
{% for esu_id in esu_ids %}
	RKS.`{{ esu_id }}_amount_in_rub_with_vat`,
	RKS.`{{ esu_id }}_amount_in_rub_without_vat`,
	RKS.`{{ esu_id }}_amount_in_contract_currency_with_vat`,
	RKS.`{{ esu_id }}_amount_in_contract_currency_without_vat`,
{% endfor -%}
{# #}
    '<==RKS BA==>',
{%- else %}
    '<==SVOD BA==>',
{%- endif %}    
FROM
	SVOD
{%- if order_id_field %}    
	LEFT JOIN RKS ON SVOD.`{{ container_field }}` = RKS.`SVOD.{{ container_field }}` AND SVOD.`{{ order_id_field }}` = RKS.`SVOD.{{ order_id_field }}`
{% endif %}
--) SELECT * FROM ANSWER
),
ANSWER AS (
SELECT
	ANSWER.*,
    BEFORE_AFTER.`B_подтянуто по`, BEFORE_AFTER.`B_date_diff`, BEFORE_AFTER.`B_min_Date_E`,
{% for rks_field in rks_fields if  "--" not in rks_field %}
    BEFORE_AFTER.`B_{{ rks_field }}`,
{% endfor -%}	
{% for esu_id in esu_ids %}
	BEFORE_AFTER.`B_{{ esu_id}}_amount_in_rub_with_vat`,
	BEFORE_AFTER.`B_{{ esu_id}}_amount_in_rub_without_vat`,
	BEFORE_AFTER.`B_{{ esu_id}}_amount_in_contract_currency_with_vat`,
	BEFORE_AFTER.`B_{{ esu_id}}_amount_in_contract_currency_without_vat`,
{% endfor -%}
{# #}
	'<==B A==>',
    BEFORE_AFTER.`A_подтянуто по`, BEFORE_AFTER.`A_date_diff`, BEFORE_AFTER.`A_min_Date_E`,
{% for rks_field in rks_fields if  "--" not in rks_field %}
    BEFORE_AFTER.`A_{{ rks_field }}`,
{% endfor -%}
{% for esu_id in esu_ids %}
	BEFORE_AFTER.`A_{{ esu_id}}_amount_in_rub_with_vat`,
	BEFORE_AFTER.`A_{{ esu_id}}_amount_in_rub_without_vat`,
	BEFORE_AFTER.`A_{{ esu_id}}_amount_in_contract_currency_with_vat`,
	BEFORE_AFTER.`A_{{ esu_id}}_amount_in_contract_currency_without_vat`,
{% endfor -%}  
FROM
	ANSWER
	LEFT JOIN BEFORE_AFTER ON ANSWER.`{{ container_field }}` = BEFORE_AFTER.`SVOD.{{ container_field }}` AND ANSWER.`{{ date_field }}` = BEFORE_AFTER.`SVOD.{{ date_field }}`
)
SELECT * FROM ANSWER
