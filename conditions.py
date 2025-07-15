conditions = {}

conditions['svod_table_name'] = 'audit.avoperevozki_soispolniteli_iz_rks'
           
conditions['container_field'] = '`SHAPKA.№ контейнера_cleaned`'

conditions['date_field'] = '`Дата для анализа`'

conditions['esu_ids'] = ['0.04.01',
                         '0.02.05.03']

conditions['rks_fields'] = [#'document_reasons_number_cleaned',
                            'service_provision_place_filial_name',
                            #'provider_name',
                            #'service_details_wagon_number',
                            'client_name',
                            #'contract_number',
                            #'service_provision_place_filial_name',
                            #'service_provision_place_location_name',
                            #'service_details_points_from_catalog_id',
                            #'service_details_points_to_catalog_id',
                            'service_details_points_from_catalog_name',
                            'service_details_points_to_catalog_name',
                            #'service_details_points_from_station_id',
                            #'service_details_points_to_station_id',
                            'service_details_points_from_station_name',
                            'service_details_points_to_station_name',
                            #'epu_id'
                            ]

# 'esu_id', !!! не использовать в этом скрипте!!!
# 'service_name' !!! не использовать в этом скрипте!!!

conditions['where_conditions'] = ["service_provision_place_filial_name='НКП МСК' AND",
                                  "date_end >= '2023-01-01' AND"]



if __name__ == '__main__':
    print(get_select_fields(rks_fields))

