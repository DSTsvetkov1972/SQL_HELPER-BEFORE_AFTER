{% if esu_id_columns %}
    min(date_end) AS Date_E,
{% else %}
    min(date_end) AS min_Date_E,
{% endif %}
    service_details_order_id,
{% if container_by_container_number %}
    replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(upperUTF8(`service_details_container_number`),' ',''),'	',''), ' ', ''), '　', ''), '\n', ''), '\r', ''),'Т','T'),'К','K'),'О','O'),'Е','E'),'Р','P'),'С','C'), 'Н', 'H'), 'В', 'B'), 'Х', 'X') AS `container_number`,
{% else %}
    equipment_number AS `container_number`,
{% endif %}
{% if 'esu_id' not in rks_fields and esu_id_columns %}
    esu_id,
{% endif %}
    ----------------------------------------------------------------------------------------------------------------------------------------------
{% for rks_field in rks_fields if  "--" not in rks_field %}
{% if rks_field == 'Отправка_cleaned' %}
    IF(
    	c=-777,
    	Null,
		replace(concat(
			IF(substringUTF8(replace(replace(replace(replace(replace(replace(upperUTF8(`document_reasons_number`),' ',''),'	',''),' ', ''), '　', ''), '\n', ''), '\r', '') AS `document_reasons_stripped`, 1, 1) AS a BETWEEN 'A' AND 'Z' OR a BETWEEN 'А' AND 'Я', a, '~') AS first_symbol,
			IF(substringUTF8(`document_reasons_stripped`, 2, 1) AS b BETWEEN 'A' AND 'Z' OR b BETWEEN 'А' AND 'Я', b, '~') AS second_symbol,
			toString(toInt32OrDefault(replace(replace(`document_reasons_stripped`, first_symbol, ''),	second_symbol, ''), toInt32(-777)) AS c
		)),'~','')
	) AS `Отправка_cleaned`,
{% elif rks_field == 'provider_name' %}
    dictGet('dict_counterparty', 'name', dictGet('dict_contract','owner',provider_contract_id)) AS `provider_name`,
{% elif rks_field == 'client_name' %}
    dictGet('dict_counterparty', 'name', client_number_id) AS `client_name`,
{% elif rks_field == 'service_details_points_from_catalog_name' %}
    dictGet('dict_location', 'name', service_details_points_from_catalog_id) AS `service_details_points_from_catalog_name`,
{% elif rks_field == 'service_details_points_to_catalog_name' %}
    dictGet('dict_location', 'name', service_details_points_to_catalog_id) AS `service_details_points_to_catalog_name`,
{% elif rks_field == 'service_details_points_from_station_name' %}
    dictGet('dict_stations', 'station_name', service_details_points_from_station_id) AS `service_details_points_from_station_name`,
{% elif rks_field == 'service_details_points_to_station_name' %}
    dictGet('dict_stations', 'station_name', service_details_points_to_station_id) AS `service_details_points_to_station_name`,	
{% elif rks_field == 'service_name' %}
    dictGet('dict_service', 'name', esu_id) AS `service_name`,
{% else %}
    `{{ rks_field }}`,
{% endif %}
{% endfor %}
    --------------------------------------------------------------------------------------------------------------  
    sum(amount_in_rub_without_vat) AS `amount_in_rub_without_vat`,
    sum(amount_in_contract_currency_without_vat) AS `amount_in_contract_currency_without_vat`,
    sum(amount_in_rub_with_vat) AS `amount_in_rub_with_vat`,
    sum(amount_in_contract_currency_with_vat) AS `amount_in_contract_currency_with_vat`  
