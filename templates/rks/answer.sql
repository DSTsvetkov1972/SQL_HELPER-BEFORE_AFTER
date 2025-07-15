--download
WITH
ANSWER AS (
SELECT
	SVOD.*,
	'<==SVOD RKS==>',
	RKS_CONT.*,
	'<==RKS RKS_EQ==>',
	RKS_EQ.*
FROM
	audit.avoperevozki_soispolniteli_iz_rks AS SVOD
	LEFT JOIN audit.rks_cont AS RKS_CONT ON SVOD.`{{ container_field }}`=RKS_CONT.container_number AND SVOD.`{{ order_id_field }}`=RKS_CONT.service_details_order_id
	LEFT JOIN audit.rks_eq AS RKS_EQ ON SVOD.`{{ container_field }}`=RKS_EQ.container_number AND SVOD.`{{ order_id_field }}`=RKS_EQ.service_details_order_id	
	LEFT JOIN audit.rks_cont_before_after AS RKS_CONT_BA ON SVOD.`{{ container_field }}`=RKS_CONT_BA.`{{ container_field }}` AND SVOD.`{{ date_field }}`=RKS_CONT_BA.`SVOD.{{ date_field }}`
    LEFT JOIN audit.rks_eq_before_after AS RKS_EQ_BA ON SVOD.`{{ container_field }}`=RKS_EQ_BA.`{{ container_field }}` AND SVOD.`{{ date_field }}`=RKS_EQ_BA.`SVOD.{{ date_field }}`    
) SELECT * FROM ANSWER

/*
) ,
ANSWER AS (
SELECT 
	`SVOD.источник`,`SVOD.Строка в исходнике`,`SVOD.Документ`,`SVOD.Наименование услуги`,`SVOD.Номенклатура соисполнителя, ЕСУ`,`SVOD.Номенклатура`,`SVOD.Стоимость (без НДС)`,`SVOD.Стоимость`,`SVOD.Сумма НДС`,`SVOD.'<==USLUGI SHAPKA==>'`,`SVOD.SHAPKA.Начало отчётного периода`,`SVOD.SHAPKA.Окончание отчётного периода`,`SVOD.Дата для анализа`,`SVOD.SHAPKA.№ заказа`,`SVOD.SHAPKA.№ контейнера`,`SVOD.SHAPKA.№ контейнера_cleaned`,`SVOD.SHAPKA.Наименование пункта назначения`,`'<==SVOD RKS==>'`,
	IF(`RKS_CONT.service_details_order_id`<>'', `RKS_CONT.min_Date_E`, `RKS_EQ.min_Date_E`) AS `RKS.min_Date_E`,
	IF(`RKS_CONT.service_details_order_id`<>'', `RKS_CONT.service_details_order_id`, `RKS_EQ.service_details_order_id`) AS `RKS.service_details_order_id`,
	IF(`RKS_CONT.service_details_order_id`<>'', `RKS_CONT.container_number`, `RKS_EQ.equipment_number`) AS `RKS.container_number`,
	IF(`RKS_CONT.service_details_order_id`<>'', `RKS_CONT.client_name`, `RKS_EQ.client_name`) AS `RKS.client_name`,
	IF(`RKS_CONT.service_details_order_id`<>'', `RKS_CONT.service_provision_place_filial_name`, `RKS_EQ.service_provision_place_filial_name`) AS `RKS.service_provision_place_filial_name`,
	IF(`RKS_CONT.service_details_order_id`<>'', `RKS_CONT.service_details_points_from_catalog_name`, `RKS_EQ.service_details_points_from_catalog_name`) AS `RKS.service_details_points_from_catalog_name`,
	IF(`RKS_CONT.service_details_order_id`<>'', `RKS_CONT.service_details_points_to_catalog_name`, `RKS_EQ.service_details_points_to_catalog_name`) AS `RKS.service_details_points_to_catalog_name`,
	IF(`RKS_CONT.service_details_order_id`<>'', `RKS_CONT.0.04.01_amount_in_rub_with_vat`, `RKS_EQ.0.04.01_amount_in_rub_with_vat`) AS `RKS.0.04.01_amount_in_rub_with_vat`,
	IF(`RKS_CONT.service_details_order_id`<>'', `RKS_CONT.0.04.01_amount_in_rub_without_vat`, `RKS_EQ.0.04.01_amount_in_rub_without_vat`) AS `RKS.0.04.01_amount_in_rub_without_vat`,
	IF(`RKS_CONT.service_details_order_id`<>'', `RKS_CONT.0.02.05.03_amount_in_rub_with_vat`, `RKS_EQ.0.02.05.03_amount_in_rub_with_vat`) AS `RKS.0.02.05.03_amount_in_rub_with_vat`,
	IF(`RKS_CONT.service_details_order_id`<>'', `RKS_CONT.0.02.05.03_amount_in_rub_without_vat`, `RKS_EQ.0.02.05.03_amount_in_rub_without_vat`) AS `RKS.0.02.05.03_amount_in_rub_without_vat`
	--,`'<==RKS RKS_EQ==>'`,`RKS_EQ.min_Date_E`,`RKS_EQ.service_details_order_id`,`RKS_EQ.equipment_number`,`RKS_EQ.client_name`,`RKS_EQ.service_provision_place_filial_name`,`RKS_EQ.service_details_points_from_catalog_name`,`RKS_EQ.service_details_points_to_catalog_name`,`RKS_EQ.0.04.01_amount_in_rub_with_vat`,`RKS_EQ.0.04.01_amount_in_rub_without_vat`,`RKS_EQ.0.02.05.03_amount_in_rub_with_vat`,`RKS_EQ.0.02.05.03_amount_in_rub_without_vat`
FROM
	ANSWER
) SELECT * FROM ANSWER


),
ANSWER AS (
SELECT
	*
FROM
	ANSWER
	LEFT JOIN audit.rks_before_after AS RKS_CONT_BA ON ANSWER.`SVOD.SHAPKA.№ контейнера_cleaned`=RKS_CONT_BA.`SHAPKA.№ контейнера_cleaned` AND ANSWER.`SVOD.Дата для анализа`=RKS_CONT_BA.`SVOD.Дата для анализа`
	LEFT JOIN audit.rks_eq_before_after AS RKS_EQ_BA ON ANSWER.`SVOD.SHAPKA.№ контейнера_cleaned`=RKS_EQ_BA.`SHAPKA.№ контейнера_cleaned` AND ANSWER.`SVOD.Дата для анализа`=RKS_EQ_BA.`SVOD.Дата для анализа`	
--) SELECT * FROM ANSWER
)
SELECT
	`ANSWER.SVOD.источник`,`ANSWER.SVOD.Строка в исходнике`,`ANSWER.SVOD.Документ`,`ANSWER.SVOD.Наименование услуги`,`ANSWER.SVOD.Номенклатура соисполнителя, ЕСУ`,`ANSWER.SVOD.Номенклатура`,`ANSWER.SVOD.Стоимость (без НДС)`,`ANSWER.SVOD.Стоимость`,`ANSWER.SVOD.Сумма НДС`,`ANSWER.SVOD.'<==USLUGI SHAPKA==>'`,`ANSWER.SVOD.SHAPKA.Начало отчётного периода`,`ANSWER.SVOD.SHAPKA.Окончание отчётного периода`,`ANSWER.SVOD.Дата для анализа`,`ANSWER.SVOD.SHAPKA.№ заказа`,`ANSWER.SVOD.SHAPKA.№ контейнера`,`ANSWER.SVOD.SHAPKA.№ контейнера_cleaned`,`ANSWER.SVOD.SHAPKA.Наименование пункта назначения`,`ANSWER.'<==SVOD RKS==>'`,`ANSWER.RKS.min_Date_E`,`ANSWER.RKS.service_details_order_id`,`ANSWER.RKS.container_number`,`ANSWER.RKS.client_name`,`ANSWER.RKS.service_provision_place_filial_name`,`ANSWER.RKS.service_details_points_from_catalog_name`,`ANSWER.RKS.service_details_points_to_catalog_name`,`ANSWER.RKS.0.04.01_amount_in_rub_with_vat`,`ANSWER.RKS.0.04.01_amount_in_rub_without_vat`,`ANSWER.RKS.0.02.05.03_amount_in_rub_with_vat`,`ANSWER.RKS.0.02.05.03_amount_in_rub_without_vat`,
	IF(
		`RKS_CONT_BA.B_service_details_order_id`<>'',
		`RKS_CONT_BA.B_date_diff`,
		`RKS_EQ_BA.B_date_diff`
	) AS `RKS_BA.B_date_diff`,
	IF(
		`RKS_CONT_BA.B_service_details_order_id`<>'',
		`RKS_CONT_BA.B_min_Date_E`,
		`RKS_EQ_BA.B_min_Date_E`
	) AS`RKS_BA.B_min_Date_E`,
	IF(
		`RKS_CONT_BA.B_service_details_order_id`<>'',
		`RKS_CONT_BA.B_service_details_order_id`,
		`RKS_EQ_BA.B_service_details_order_id`
	) AS `RKS_BA.B_service_details_order_id`,
	IF(
		`RKS_CONT_BA.B_service_details_order_id`<>'',
		`RKS_CONT_BA.B_client_name`,
		`RKS_EQ_BA.B_client_name`
	) AS `RKS_BA.B_client_name`,
	IF(
		`RKS_CONT_BA.B_service_details_order_id`<>'',
		`RKS_CONT_BA.B_service_provision_place_filial_name`,
		`RKS_EQ_BA.B_service_provision_place_filial_name`
	) AS `RKS_BA.B_service_provision_place_filial_name`,
	IF(
		`RKS_CONT_BA.B_service_details_order_id`<>'',
		`RKS_CONT_BA.B_service_details_points_from_catalog_name`,
		`RKS_EQ_BA.B_service_details_points_from_catalog_name`
	) AS `RKS_BA.B_service_details_points_from_catalog_name`,
	IF(
		`RKS_CONT_BA.B_service_details_order_id`<>'',
		`RKS_CONT_BA.B_service_details_points_to_catalog_name`,
		`RKS_EQ_BA.B_service_details_points_to_catalog_name`
	) AS `RKS_BA.B_service_details_points_to_catalog_name`,
	IF(
		`RKS_CONT_BA.B_service_details_order_id`<>'',
		`RKS_CONT_BA.B_0.04.01_amount_in_rub_with_vat`,
		`RKS_EQ_BA.B_0.04.01_amount_in_rub_with_vat`
	) AS `RKS_BA.B_0.04.01_amount_in_rub_with_vat`,
	IF(
		`RKS_CONT_BA.B_service_details_order_id`<>'',
		`RKS_CONT_BA.B_0.04.01_amount_in_rub_without_vat`,
		`RKS_EQ_BA.B_0.04.01_amount_in_rub_without_vat`
	) AS `RKS_BA.B_0.04.01_amount_in_rub_without_vat`,
	IF(
		`RKS_CONT_BA.B_service_details_order_id`<>'',
		`RKS_CONT_BA.B_0.02.05.03_amount_in_rub_with_vat`,
		`RKS_EQ_BA.B_0.02.05.03_amount_in_rub_with_vat`
	) AS `RKS_BA.B_0.02.05.03_amount_in_rub_with_vat`,
	IF(
		`RKS_CONT_BA.B_service_details_order_id`<>'',
		`RKS_CONT_BA.B_0.02.05.03_amount_in_rub_without_vat`,
		`RKS_EQ_BA.B_0.02.05.03_amount_in_rub_without_vat`
	) AS `RKS_BA.B_0.02.05.03_amount_in_rub_without_vat`,
	---------------------------------------------------------------------------------------------------------
	IF(
		`RKS_CONT_BA.A_service_details_order_id`<>'',
		`RKS_CONT_BA.A_date_diff`,
		`RKS_EQ_BA.A_date_diff`
	) AS `RKS_BA.A_date_diff`,
	IF(
		`RKS_CONT_BA.A_service_details_order_id`<>'',
		`RKS_CONT_BA.A_min_Date_E`,
		`RKS_EQ_BA.A_min_Date_E`
	) AS `RKS_BA.A_min_Date_E`,
	IF(
		`RKS_CONT_BA.A_service_details_order_id`<>'',
		`RKS_CONT_BA.A_service_details_order_id`,
		`RKS_EQ_BA.A_service_details_order_id`
	) AS `RKS_BA.A_service_details_order_id`,
	IF(
		`RKS_CONT_BA.A_service_details_order_id`<>'',
		`RKS_CONT_BA.A_client_name`,
		`RKS_EQ_BA.A_client_name`
	) AS `RKS_BA.A_client_name`,
	IF(
		`RKS_CONT_BA.A_service_details_order_id`<>'',
		`RKS_CONT_BA.A_service_provision_place_filial_name`,
		`RKS_EQ_BA.A_service_provision_place_filial_name`
	) AS `RKS_BA.A_service_provision_place_filial_name`,
	IF(
		`RKS_CONT_BA.A_service_details_order_id`<>'',
		`RKS_CONT_BA.A_service_details_points_from_catalog_name`,
		`RKS_EQ_BA.A_service_details_points_from_catalog_name`
	) AS `RKS_BA.A_service_details_points_from_catalog_name`,
	IF(
		`RKS_CONT_BA.A_service_details_order_id`<>'',
		`RKS_CONT_BA.A_service_details_points_to_catalog_name`,
		`RKS_EQ_BA.A_service_details_points_to_catalog_name`
	) AS `RKS_BA.A_service_details_points_to_catalog_name`,
	IF(
		`RKS_CONT_BA.A_service_details_order_id`<>'',
		`RKS_CONT_BA.A_0.04.01_amount_in_rub_with_vat`,
		`RKS_EQ_BA.A_0.04.01_amount_in_rub_with_vat`
	) AS `RKS__BA.A_0.04.01_amount_in_rub_with_vat`,
	IF(
		`RKS_CONT_BA.A_service_details_order_id`<>'',
		`RKS_CONT_BA.A_0.04.01_amount_in_rub_without_vat`,
		`RKS_EQ_BA.A_0.04.01_amount_in_rub_without_vat`
	) AS `RKS_BA.A_0.04.01_amount_in_rub_without_vat`,
	IF(
		`RKS_CONT_BA.B_service_details_order_id`<>'',
		`RKS_CONT_BA.A_0.02.05.03_amount_in_rub_with_vat`,
		`RKS_EQ_BA.A_0.02.05.03_amount_in_rub_with_vat`
	) AS `RKS_SBA.A_0.02.05.03_amount_in_rub_with_vat`,
	IF(
		`RKS_CONT_BA.A_service_details_order_id`<>'',
		`RKS_CONT_BA.A_0.02.05.03_amount_in_rub_without_vat`,
		`RKS_EQ_BA.A_0.02.05.03_amount_in_rub_without_vat`
	) AS `RKS_BA.A_0.02.05.03_amount_in_rub_without_vat`
	-- `RKS_EQ_BA.B_date_diff`,`RKS_EQ_BA.B_min_Date_E`,`RKS_EQ_BA.B_service_details_order_id`,`RKS_EQ_BA.B_client_name`,`RKS_EQ_BA.B_service_provision_place_filial_name`,`RKS_EQ_BA.B_service_details_points_from_catalog_name`,`RKS_EQ_BA.B_service_details_points_to_catalog_name`,`RKS_EQ_BA.B_0.04.01_amount_in_rub_with_vat`,`RKS_EQ_BA.B_0.04.01_amount_in_rub_without_vat`,`RKS_EQ_BA.B_0.02.05.03_amount_in_rub_with_vat`,`RKS_EQ_BA.B_0.02.05.03_amount_in_rub_without_vat`,`RKS_EQ_BA.A_date_diff`,`RKS_EQ_BA.A_min_Date_E`,`RKS_EQ_BA.A_service_details_order_id`,`RKS_EQ_BA.A_client_name`,`RKS_EQ_BA.A_service_provision_place_filial_name`,`RKS_EQ_BA.A_service_details_points_from_catalog_name`,`RKS_EQ_BA.A_service_details_points_to_catalog_name`,`RKS_EQ_BA.A_0.04.01_amount_in_rub_with_vat`,`RKS_EQ_BA.A_0.04.01_amount_in_rub_without_vat`,`RKS_EQ_BA.A_0.02.05.03_amount_in_rub_with_vat`,`RKS_EQ_BA.A_0.02.05.03_amount_in_rub_without_vat`,`RKS_EQ_BA.SHAPKA.№ контейнера_cleaned`,`RKS_EQ_BA.SVOD.Дата для анализа`
FROM
	ANSWER
WHERE 
`ANSWER.SVOD.SHAPKA.№ контейнера`='TKRU3547547' AND `ANSWER.SVOD.SHAPKA.№ заказа`='31246102'
*/