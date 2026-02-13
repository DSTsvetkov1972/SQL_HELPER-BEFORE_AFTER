from clickhouse_driver import Client
from dotenv import load_dotenv
import os
from colorama import Fore

load_dotenv(r'C:\Users\tsvetkovds\Documents\.Полезности\PYTHON\.env')

CLICK_HOST = os.getenv('CLICK_HOST')
CLICK_PORT = os.getenv('CLICK_PORT')
CLICK_DBNAME = os.getenv('CLICK_DBNAME')
CLICK_USER = os.getenv('CLICK_USER')
CLICK_PWD = os.getenv('CLICK_PWD')


def get_table_info(svod_table):

    connection=Client(
        host=CLICK_HOST,
        port = CLICK_PORT,
        database = CLICK_DBNAME,
        user = CLICK_USER,
        password = CLICK_PWD,
        secure = True,
        verify=False,
        settings=dict(socket_timeout=3000000, send_timeout=3000000, keepAliveTimeout=3000000)
        )
    
    sql = f'DESCRIBE TABLE  { svod_table }'

    print(Fore.BLUE, f'Получаем список колонок таблицы {svod_table}', Fore.RESET)

    with connection:
        res = connection.execute(sql)
        if res:
            return [column[0] for column in res]
        
    
def get_svod_table_columns(svod_table_info, container_field='', order_field=''):

    svod_table_columns = []
    
    for n, info in enumerate(svod_table_info, 1):

        column_name = info.replace('`', '')

        if column_name == container_field.replace('`', ''):

            if n != len(svod_table_info):
                str_end = ',\n\t'
            else:
                str_end = ''

            column_name_processed = f"\n\treplace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(upperUTF8(`{ column_name }`),' ',''),'	',''), ' ', ''), '　', ''), '\\n', ''), '\\r', ''),'Т','T'),'К','K'),'О','O'),'Е','E'),'Р','P'),'С','C'), 'Н', 'H'), 'В', 'B'), 'Х', 'X'), ' GR', '') AS `{ column_name }`{str_end}"
        
        elif column_name == order_field.replace('`', ''):
                        
            if n != len(svod_table_info):
                str_end = ',\n\t'
            else:
                str_end = ''

            column_name_processed = f"\n\treplace(replace(replace(replace(replace(replace(`{ column_name }`,' ',''),'	',''), ' ', ''), '　', ''), '\\n', ''), '\\r', '') AS `{ column_name }`{str_end}"
        
        else:

            if n != len(svod_table_info):
                str_end = ','
            else:
                str_end = ''

            column_name_processed = f'`{ column_name }`{str_end}'

        

        svod_table_columns.append(column_name_processed)

    print(Fore.BLACK, svod_table_columns, Fore.RESET)    

    return svod_table_columns


def get_max_column_len(svod_table_columns, date_field, with_vat, esu_id_columns):

    if esu_id_columns and date_field:
        svod_table_columns.append('A_0.01.01.01_amount_in_contract_currency_without_vat')
    elif esu_id_columns and not date_field:
        svod_table_columns.append('A_0.01.01.01_amount_in_contract_currency_without_vat')
    elif not esu_id_columns and date_field:
        svod_table_columns.append('A_amount_in_contract_currency_without_vat')
    else:
        svod_table_columns.append('amount_in_contract_currency_without_vat')

    lengths = [len(column) for column in svod_table_columns]
    return max(lengths)


if __name__ == '__main__':
    table_info = get_table_info('audit.svod')

    print(table_info)
    
    table_columns = get_svod_table_columns(table_info)
    print(table_columns)




    