CREATE OR REPLACE TABLE audit.{{ project }}_answer
ENGINE = MergeTree()
ORDER BY tuple()
AS (

WITH
SVOD AS (
SELECT * FROM audit.{{ project }}_svod
),
{% if order_id_field %}
RKS AS (
SELECT * FROM audit.{{ project }}_rks
),
{% endif %}
{% if date_field %}
BEFORE_AFTER AS (
SELECT * FROM audit.{{ project }}_rks_before_after
),
{% endif %}
ANSWER AS (
SELECT
	SVOD.*,
{% if order_id_field %}
    '<==SVOD RKS==>',
{% else %}
    '<==SVOD BA==>',
{% endif %}
{% if order_id_field %}
    RKS.`подтянуто по`,
    {% if date_field %}
    date_diff(DAY, `{{ date_field }}`, RKS.`min_Date_E`) AS `date_diff`,
    {% endif %}
    RKS.`min_Date_E`,
    {% for rks_field in rks_fields if  "--" not in rks_field %}
    RKS.`{{ rks_field }}`,
    {% endfor %}
    {% if esu_id_columns %}
        {% for esu_id in esu_id_columns %}
    RKS.`{{ esu_id }}_amount_in_rub_without_vat`,
	RKS.`{{ esu_id }}_amount_in_contract_currency_without_vat`{% if date_field or with_vat %},
    {% endif %}
            {% if with_vat %}
	RKS.`{{ esu_id }}_amount_in_rub_with_vat`,
	RKS.`{{ esu_id }}_amount_in_contract_currency_with_vat`{% if date_field %},{% endif %}
            {% endif %}
        {% endfor %}
    {% else %}
    RKS.`amount_in_rub_without_vat`,
	RKS.`amount_in_contract_currency_without_vat`{% if date_field or with_vat %},
    {% endif %}
    {% if with_vat %}
	RKS.`amount_in_rub_with_vat`,
	RKS.`amount_in_contract_currency_with_vat`{% if date_field %},
    {% endif %}
        {% endif %}
    {% endif %}
{% endif %}
{% if order_id_field and date_field %}
    '<==RKS BA==>'
{% endif %}
FROM
	SVOD
    {% if order_id_field %}
	LEFT JOIN RKS ON
        SVOD.`{{ container_field }}`=RKS.`SVOD.{{ container_field }}` AND{% if esu_id_field %} 
        SVOD.`{{ esu_id_field }}`=RKS.`SVOD.{{ esu_id_field }}` AND{% endif %} 
        SVOD.`{{ order_id_field }}`=RKS.`SVOD.{{ order_id_field }}`
    {% endif %}
{% if date_field %}
--) SELECT * FROM ANSWER
),
ANSWER AS (
SELECT
	ANSWER.*,
    {% if before_fields %}
    BEFORE_AFTER.`B_подтянуто по`, BEFORE_AFTER.`B_date_diff`, BEFORE_AFTER.`B_min_Date_E`, BEFORE_AFTER. `B_service_details_order_id`,
        {% for rks_field in rks_fields if  "--" not in rks_field %}
    BEFORE_AFTER.`B_{{ rks_field }}`,
        {% endfor %}
        {% if esu_id_columns %}
            {% for esu_id in esu_id_columns %}
	BEFORE_AFTER.`B_{{ esu_id}}_amount_in_rub_without_vat`,
	BEFORE_AFTER.`B_{{ esu_id}}_amount_in_contract_currency_without_vat`,
                {% if with_vat %}
	BEFORE_AFTER.`B_{{ esu_id}}_amount_in_rub_with_vat`,
	BEFORE_AFTER.`B_{{ esu_id}}_amount_in_contract_currency_with_vat`,
                {% endif %}
            {% endfor %}
        {% else %}
	BEFORE_AFTER.`B_amount_in_rub_without_vat`,
	BEFORE_AFTER.`B_amount_in_contract_currency_without_vat`,
            {% if with_vat %}
	BEFORE_AFTER.`B_amount_in_rub_with_vat`,
	BEFORE_AFTER.`B_amount_in_contract_currency_with_vat`,
            {% endif %}
        {% endif %}
    {% endif %}
	'<==B A==>',
    {% if after_fields %}
    BEFORE_AFTER.`A_подтянуто по`, BEFORE_AFTER.`A_date_diff`, BEFORE_AFTER.`A_min_Date_E`, BEFORE_AFTER. `A_service_details_order_id`,
        {% for rks_field in rks_fields if  "--" not in rks_field %}
    BEFORE_AFTER.`A_{{ rks_field }}`,
        {% endfor %}
        {% if esu_id_columns %}
            {% for esu_id in esu_id_columns %}
	BEFORE_AFTER.`A_{{ esu_id}}_amount_in_rub_without_vat`,
	BEFORE_AFTER.`A_{{ esu_id}}_amount_in_contract_currency_without_vat`,
                {% if with_vat %}
	BEFORE_AFTER.`A_{{ esu_id}}_amount_in_rub_with_vat`,
	BEFORE_AFTER.`A_{{ esu_id}}_amount_in_contract_currency_with_vat`,
                {% endif %}
            {% endfor %}
        {% else %}
	BEFORE_AFTER.`A_amount_in_rub_without_vat`,
	BEFORE_AFTER.`A_amount_in_contract_currency_without_vat`,
            {% if with_vat %}
	BEFORE_AFTER.`A_amount_in_rub_with_vat`,
	BEFORE_AFTER.`A_amount_in_contract_currency_with_vat`
            {% endif %}
        {% endif %}
    {% endif %}
FROM
	ANSWER
	LEFT JOIN BEFORE_AFTER ON
        ANSWER.`{{ container_field }}`=BEFORE_AFTER.`SVOD.{{ container_field }}` AND{% if esu_id_field %} 
        ANSWER.`{{ esu_id_field }}`=BEFORE_AFTER.`SVOD.{{ esu_id_field }}` AND{% endif %} 
        ANSWER.`{{ date_field }}`=BEFORE_AFTER.`SVOD.{{ date_field }}`
{% endif %}
)
SELECT * FROM ANSWER

)

--download
SELECT * FROM audit.{{ project }}_answer