{% if container_by_container_number %}
-- SELECT * FROM audit.rks_cont
CREATE OR REPLACE TABLE audit.rks_cont
{% else %}
-- SELECT * FROM audit.rks_eq
CREATE OR REPLACE TABLE audit.rks_eq
{% endif %}

ENGINE = MergeTree()
ORDER BY container_number
AS (

WITH 
SVOD AS (
	SELECT DISTINCT {{ container_field }} FROM {{ svod_table_name }} WHERE {{ container_field }} <>''
),
RKS AS (
SELECT
    {% include 'block_select_fields.sql' -%} 
FROM 
	(SELECT DISTINCT * FROM rks__directly WHERE client_number_id <> '0009309810') AS RD
	INNER JOIN SVOD ON container_number = {{ container_field }}
WHERE
    {% include 'block_where.sql' %}
    esu_id IN ('{{"', '".join(esu_ids)}}')
GROUP BY
    {% include 'block_group_by.sql' %},
    esu_id
HAVING
	(amount_in_rub_without_vat<>0 OR 
	amount_in_contract_currency_without_vat<>0 OR
	amount_in_rub_with_vat<>0 OR
    amount_in_contract_currency_with_vat<>0) 
--) SELECT * FROM RKS 
)
SELECT
	min(Date_E) AS min_Date_E,service_details_order_id,container_number,client_name,service_provision_place_filial_name,service_details_points_from_catalog_name,service_details_points_to_catalog_name,
    {% for esu_id in esu_ids %}
    sumIf(amount_in_rub_with_vat, esu_id='{{esu_id}}') AS `{{esu_id}}_amount_in_rub_with_vat`,    
    sumIf(amount_in_rub_without_vat, esu_id='{{esu_id}}') AS `{{esu_id}}_amount_in_rub_without_vat`,
    sumIf(amount_in_contract_currency_with_vat, esu_id='{{esu_id}}') AS `{{esu_id}}_amount_in_contract_currency_with_vat`,    
    sumIf(amount_in_contract_currency_without_vat, esu_id='{{esu_id}}') AS `{{esu_id}}_amount_in_contract_currency_without_vat`{% if not loop.last%},{% endif %}    
    {% endfor -%}    
FROM
	RKS
GROUP BY	
	{% include 'block_group_by.sql' %}

	
)
