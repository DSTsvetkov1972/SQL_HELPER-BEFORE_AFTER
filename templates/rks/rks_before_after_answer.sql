-- SELECT * FROM audit.{{ user }}_rks_before_after
CREATE OR REPLACE TABLE audit.{{ user }}_rks_before_after
ENGINE = MergeTree()
ORDER BY `SVOD.{{ container_field }}`
AS (

WITH 
SVOD AS (
SELECT DISTINCT `{{ container_field }}`, `{{ date_field }}` FROM {{ svod_table_name }}
--) SELECT * FROM SVOD
),
RKS AS (
SELECT
	SVOD.*,
	RKS_CONT.*,
	RKS_EQ.*
FROM
	SVOD
	LEFT JOIN audit.{{ user }}_rks_before_after_cont AS RKS_CONT ON SVOD.`{{ container_field }}`=RKS_CONT.`{{ container_field }}` AND SVOD.`{{ date_field }}`=RKS_CONT.`SVOD.{{ date_field }}`
	LEFT JOIN audit.{{ user }}_rks_before_after_eq AS RKS_EQ ON SVOD.`{{ container_field }}`=RKS_EQ.`{{ container_field }}` AND SVOD.`{{ date_field }}`=RKS_EQ.`SVOD.{{ date_field }}`
--) SELECT * FROM RKS
)
SELECT
    multiIf(
        `RKS_EQ.B_service_details_order_id`<>'', 'equipment_number',        
        `RKS_CONT.B_service_details_order_id`<>'', 'container_number',
        ''
    ) AS `B_подтянуто по`,
	multiIf(
        `RKS_EQ.B_service_details_order_id`<>'', `RKS_EQ.B_date_diff`,
        `RKS_CONT.B_service_details_order_id`<>'', `RKS_CONT.B_date_diff`,
        NULL
    ) AS `B_date_diff`,	
	multiIf(
        `RKS_EQ.B_service_details_order_id`<>'', `RKS_EQ.B_min_Date_E`,
        `RKS_CONT.B_service_details_order_id`<>'', `RKS_CONT.B_min_Date_E`,
        NULL
    ) AS `B_min_Date_E`,
{% for rks_field in rks_fields if  "--" not in rks_field %}
	IF(`RKS_EQ.B_service_details_order_id`<>'', `RKS_EQ.B_{{ rks_field }}`, `RKS_CONT.B_{{ rks_field}}`) AS `B_{{ rks_field }}`,
{% endfor -%}   
{% for esu_id in esu_ids %}
	IF(`RKS_EQ.B_service_details_order_id`<>'', `RKS_EQ.B_{{ esu_id }}_amount_in_rub_with_vat`, `RKS_CONT.B_{{ esu_id }}_amount_in_rub_with_vat`) AS `B_{{ esu_id}}_amount_in_rub_with_vat`,
	IF(`RKS_EQ.B_service_details_order_id`<>'', `RKS_EQ.B_{{ esu_id }}_amount_in_rub_without_vat`, `RKS_CONT.B_{{ esu_id }}_amount_in_rub_without_vat`) AS `B_{{ esu_id}}_amount_in_rub_without_vat`,
	IF(`RKS_EQ.B_service_details_order_id`<>'', `RKS_EQ.B_{{ esu_id }}_amount_in_contract_currency_with_vat`, `RKS_CONT.B_{{ esu_id }}_amount_in_contract_currency_with_vat`) AS `B_{{ esu_id}}_amount_in_contract_currency_with_vat`,
	IF(`RKS_EQ.B_service_details_order_id`<>'', `RKS_EQ.B_{{ esu_id }}_amount_in_contract_currency_without_vat`, `RKS_CONT.B_{{ esu_id }}_amount_in_contract_currency_without_vat`) AS `B_{{ esu_id}}_amount_in_contract_currency_without_vat`,
{% endfor -%}
------------------------------------------------------------------------
    multiIf(
        `RKS_EQ.A_service_details_order_id`<>'', 'equipment_number',        
        `RKS_CONT.A_service_details_order_id`<>'', 'container_number',
        ''
    ) AS `A_подтянуто по`,
	multiIf(
        `RKS_EQ.A_service_details_order_id`<>'', `RKS_EQ.A_date_diff`,
        `RKS_CONT.A_service_details_order_id`<>'', `RKS_CONT.A_date_diff`,
        NULL
    ) AS `A_date_diff`,	
	multiIf(
        `RKS_EQ.A_service_details_order_id`<>'', `RKS_EQ.A_min_Date_E`,
        `RKS_CONT.A_service_details_order_id`<>'', `RKS_CONT.A_min_Date_E`,
        NULL
    ) AS `A_min_Date_E`,
{% for rks_field in rks_fields if  "--" not in rks_field %}
	IF(`RKS_EQ.A_service_details_order_id`<>'', `RKS_EQ.A_{{ rks_field }}`, `RKS_CONT.A_{{ rks_field}}`) AS `A_{{ rks_field }}`,
{% endfor -%}   
{% for esu_id in esu_ids %}
	IF(`RKS_EQ.A_service_details_order_id`<>'', `RKS_EQ.A_{{ esu_id }}_amount_in_rub_with_vat`, `RKS_CONT.A_{{ esu_id }}_amount_in_rub_with_vat`) AS `A_{{ esu_id}}_amount_in_rub_with_vat`,
	IF(`RKS_EQ.A_service_details_order_id`<>'', `RKS_EQ.A_{{ esu_id }}_amount_in_rub_without_vat`, `RKS_CONT.A_{{ esu_id }}_amount_in_rub_without_vat`) AS `A_{{ esu_id}}_amount_in_rub_without_vat`,
	IF(`RKS_EQ.A_service_details_order_id`<>'', `RKS_EQ.A_{{ esu_id }}_amount_in_contract_currency_with_vat`, `RKS_CONT.A_{{ esu_id }}_amount_in_contract_currency_with_vat`) AS `A_{{ esu_id}}_amount_in_contract_currency_with_vat`,
	IF(`RKS_EQ.A_service_details_order_id`<>'', `RKS_EQ.A_{{ esu_id }}_amount_in_contract_currency_without_vat`, `RKS_CONT.A_{{ esu_id }}_amount_in_contract_currency_without_vat`) AS `A_{{ esu_id}}_amount_in_contract_currency_without_vat`,
{% endfor -%} 
{# #}
    `SVOD.{{ container_field }}`,`SVOD.{{ date_field }}`
FROM
	RKS

)
{# #}
