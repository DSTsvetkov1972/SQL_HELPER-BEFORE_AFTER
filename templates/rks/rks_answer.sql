-- SELECT * FROM audit.rks
CREATE OR REPLACE TABLE audit.rks
ENGINE = MergeTree()
ORDER BY `SVOD.{{ container_field }}`
AS (

WITH 
SVOD AS (
SELECT DISTINCT `{{ container_field }}`, `{{ order_id_field }}` FROM {{ svod_table_name }}
--) SELECT * FROM SVOD
),
RKS AS (
SELECT
	SVOD.*,
	RKS_CONT.*,
	RKS_EQ.*
FROM
	SVOD
	LEFT JOIN audit.rks_cont AS RKS_CONT ON SVOD.`{{ container_field }}`=RKS_CONT.container_number AND SVOD.`{{ order_id_field }}`=RKS_CONT.service_details_order_id
	LEFT JOIN audit.rks_eq AS RKS_EQ ON SVOD.`{{ container_field }}`=RKS_EQ.container_number AND SVOD.`{{ order_id_field }}`=RKS_EQ.service_details_order_id	
--) SELECT * FROM RKS
)
SELECT
    multiIf(
        `RKS_EQ.service_details_order_id`<>'', 'equipment_number',        
        `RKS_CONT.service_details_order_id`<>'', 'container_number',
        ''
    ) AS `подтянуто по`,
	multiIf(
        `RKS_EQ.service_details_order_id`<>'', `RKS_EQ.min_Date_E`,
        `RKS_CONT.service_details_order_id`<>'', `RKS_CONT.min_Date_E`,
        NULL
    ) AS `min_Date_E`,
{% for rks_field in rks_fields if  "--" not in rks_field %}
	IF(`RKS_EQ.service_details_order_id`<>'', `RKS_EQ.{{ rks_field }}`, `RKS_CONT.{{ rks_field}}`) AS `{{ rks_field }}`,
{% endfor -%}   
{% for esu_id in esu_ids %}
	IF(`RKS_EQ.service_details_order_id`<>'', `RKS_EQ.{{ esu_id }}_amount_in_rub_with_vat`, `RKS_CONT.{{ esu_id }}_amount_in_rub_with_vat`) AS `{{ esu_id}}_amount_in_rub_with_vat`,
	IF(`RKS_EQ.service_details_order_id`<>'', `RKS_EQ.{{ esu_id }}_amount_in_rub_without_vat`, `RKS_CONT.{{ esu_id }}_amount_in_rub_without_vat`) AS `{{ esu_id}}_amount_in_rub_without_vat`,
	IF(`RKS_EQ.service_details_order_id`<>'', `RKS_EQ.{{ esu_id }}_amount_in_contract_currency_with_vat`, `RKS_CONT.{{ esu_id }}_amount_in_contract_currency_with_vat`) AS `{{ esu_id}}_amount_in_contract_currency_with_vat`,
	IF(`RKS_EQ.service_details_order_id`<>'', `RKS_EQ.{{ esu_id }}_amount_in_contract_currency_without_vat`, `RKS_CONT.{{ esu_id }}_amount_in_contract_currency_without_vat`) AS `{{ esu_id}}_amount_in_contract_currency_without_vat`,
{% endfor -%} 
{# #}
    `SVOD.{{ container_field }}`,`SVOD.{{ order_id_field }}`
FROM
	RKS

)
{# #}

