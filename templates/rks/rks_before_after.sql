{% if container_by_container_number %}
-- SELECT * FROM audit.rks_before_after_cont
CREATE OR REPLACE TABLE audit.rks_before_after_cont
{% else %}
-- SELECT * FROM audit.rks_before_after_eq
CREATE OR REPLACE TABLE audit.rks_before_after_eq
{% endif %}
ENGINE = MergeTree()
ORDER BY `{{ container_field }}`
AS (

WITH 
SVOD AS (
	SELECT DISTINCT `{{ container_field }}`, `{{ date_field }}` FROM {{ svod_table_name }} WHERE `{{ container_field }}`<>'' AND `{{ date_field }}` IS NOT NULL
--) SELECT * FROM SVOD
),
RKS_BEFORE_AFTER AS (
SELECT
	IF(`B_service_details_order_id`='', NULL, date_diff(DAY, SVOD.`{{ date_field }}`, `B_min_Date_E`)) AS `B_date_diff`,
	IF(`B_service_details_order_id`='', NULL, argMaxIf(min_Date_E, min_Date_E, min_Date_E<=SVOD.`{{ date_field }}`)) AS `B_min_Date_E`,
	argMaxIf(`service_details_order_id`, min_Date_E, min_Date_E<=SVOD.`{{ date_field }}`) AS `B_service_details_order_id`,
{% for rks_field in rks_fields %}
    argMaxIf(`{{ rks_field }}`, 'min_Date_E', min_Date_E<=SVOD.`{{ date_field }}`) AS `B_{{ rks_field }}`,
{% endfor %}
{% for esu_id in esu_ids %}
    argMaxIf(`{{ esu_id }}_amount_in_rub_with_vat`, min_Date_E, min_Date_E<=SVOD.`{{ date_field }}`) AS `B_{{ esu_id }}_amount_in_rub_with_vat`,
    argMaxIf(`{{ esu_id }}_amount_in_rub_without_vat`, min_Date_E, min_Date_E<=SVOD.`{{ date_field }}`) AS `B_{{ esu_id }}_amount_in_rub_without_vat`,
    argMaxIf(`{{ esu_id }}_amount_in_contract_currency_with_vat`, min_Date_E, min_Date_E<=SVOD.`{{ date_field }}`) AS `B_{{ esu_id }}_amount_in_contract_currency_with_vat`,
    argMaxIf(`{{ esu_id }}_amount_in_contract_currency_without_vat`, min_Date_E, min_Date_E<=SVOD.`{{ date_field }}`) AS `B_{{ esu_id }}_amount_in_contract_currency_without_vat`,	
{% endfor %}
	------------------------------------------------------------------------------------------------
	IF(`A_service_details_order_id`='', NULL, date_diff(DAY, SVOD.`{{ date_field }}`, `A_min_Date_E`)) AS `A_date_diff`,
	IF(`A_service_details_order_id`='', NULL, argMaxIf(min_Date_E, min_Date_E, min_Date_E>=SVOD.`{{ date_field }}`)) AS `A_min_Date_E`,
	argMinIf(`service_details_order_id`, min_Date_E, min_Date_E>=SVOD.`{{ date_field }}`) AS `A_service_details_order_id`,
{% for rks_field in rks_fields %}
    argMinIf(`{{ rks_field }}`, 'min_Date_E', min_Date_E>=SVOD.`{{ date_field }}`) AS `A_{{ rks_field }}`,
{% endfor %}
{% for esu_id in esu_ids %}
    argMinIf(`{{ esu_id }}_amount_in_rub_with_vat`, min_Date_E, min_Date_E>=SVOD.`{{ date_field }}`) AS `A_{{ esu_id }}_amount_in_rub_with_vat`,
    argMinIf(`{{ esu_id }}_amount_in_rub_without_vat`, min_Date_E, min_Date_E>=SVOD.`{{ date_field }}`) AS `A_{{ esu_id }}_amount_in_rub_without_vat`,
    argMinIf(`{{ esu_id }}_amount_in_contract_currency_with_vat`, min_Date_E, min_Date_E>=SVOD.`{{ date_field }}`) AS `A_{{ esu_id }}_amount_in_contract_currency_with_vat`,
    argMinIf(`{{ esu_id }}_amount_in_contract_currency_without_vat`, min_Date_E, min_Date_E>=SVOD.`{{ date_field }}`) AS `A_{{ esu_id }}_amount_in_contract_currency_without_vat`,	
{% endfor %}
	------------------------------------------------------------------------------------------------	
	SVOD.`{{ container_field }}`, SVOD.`{{ date_field }}`
FROM 
    {% if container_by_container_number %}
	(SELECT * FROM audit.rks_cont) AS RKS
    {% else %}
	(SELECT * FROM audit.rks_eq) AS RKS    
    {% endif %}
	RIGHT JOIN SVOD ON SVOD.`{{ container_field }}` = RKS.`container_number`
GROUP BY
	 SVOD.`{{ container_field }}`, SVOD.`{{ date_field }}`
) SELECT * FROM RKS_BEFORE_AFTER

)
