    min(date_end) AS Date_E,
    service_details_order_id,
{% if container_by_container_number %}
    replace(replace(replace(replace(replace(replace(replace(upperUTF8(service_details_container_number),' ',''),'Т','T'),'К','K'),'О','O'),'Е','E'),'Р','P'),'С','C') AS `container_number`,
{% else %}
    equipment_number AS `container_number`,
{% endif %}
    esu_id,
    ----------------------------------------------------------------------------------------------------------------------------------------------
{% for rks_field in rks_fields if  "--" not in rks_field %}
{% if rks_field == 'document_reasons_number_cleaned' %}
    replace(`document_reasons_number`, ' ', '') AS `document_reasons_number_striped`,
    multiIf(
        ((substringUTF8(upperUTF8(`document_reasons_number_striped`),1,2) AS first_two BETWEEN 'АА' AND 'ЯЯ') OR
        first_two BETWEEN 'AA' AND 'ZZ') AND lengthUTF8(`document_reasons_number_striped`)>2 AS condition_first_two, first_two,
        ((substringUTF8(upperUTF8(`document_reasons_number_striped`),1,2) AS first_one BETWEEN 'А' AND 'Я') OR 
        first_one BETWEEN 'A' AND 'Z') AND lengthUTF8(`document_reasons_number_striped`)>1 AS condition_first_one, first_one,
        ''
    ) AS `document_reasons_number_letters`,
    multiIf(
        condition_first_two, toInt64OrNull(substringUTF8(`document_reasons_number_striped`, 3, lengthUTF8(`document_reasons_number_striped`)-2)),
        condition_first_one, toInt64OrNull(substringUTF8(`document_reasons_number_striped`, 2, lengthUTF8(`document_reasons_number_striped`)-1)),
        toInt64OrNull(`document_reasons_number_striped`)
    ) AS `document_reasons_number_figures`,
    concat(document_reasons_number_letters, toString(document_reasons_number_figures)) AS `document_reasons_number_cleaned`,
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
