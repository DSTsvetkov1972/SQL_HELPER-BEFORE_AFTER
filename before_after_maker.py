from jinja2 import Environment, FileSystemLoader
from pprint import pprint
from click import get_svod_table_columns, get_max_column_len, get_table_info
import pyperclip
from colorama import Fore, init, Style
import json  
import os

init()

while True:

    print(Fore.MAGENTA + "="*41 + Fore.RESET)    
    print(Fore.MAGENTA + "* BEFORE_AFTER_MAKER v.2026-02-13-17:36 *" + Fore.RESET)
    print(Fore.MAGENTA + "="*41 + Fore.RESET)    

    if os.path.exists(os.path.join(os.getcwd(), 'conditions.json')):
        print(Style.BRIGHT + Fore.YELLOW + "Задайте параметры в файле conditions.json и нажмите ввод!" + Fore.RESET)
        os.startfile(os.getcwd())
        input()

        try:
            with open(os.path.join(os.getcwd(), 'conditions.json'), 'r', encoding="utf-8") as file:  
                conditions = json.load(file)
                
                if ("esu_id" in conditions['rks_fields'] or "service_name" in conditions['rks_fields']) and \
                    conditions['esu_id_columns']:
                    print(Fore.RED + "Неправильные параметры JSON: esu_id или service_name не могут быть заданы в качестве rks_fields, если список esu_id_columns не пустой!" + Fore.RESET)
                    pyperclip.copy("Запрос не сформирован!\nНеправильные параметры JSON: esu_id или service_name не могут быть заданы в качестве rks_fields, если список esu_id_columns не пустой!")
                    continue

                if (not conditions['order_id_field'] and not conditions['date_field']):
                    print(Fore.RED + "Неправильные параметры JSON: не задано ни поле order_id_field ни date_field!" + Fore.RESET)
                    pyperclip.copy("Запрос не сформирован!\nНеправильные параметры JSON: не задано ни поле order_id_field ни date_field!")
                    continue

                if conditions["svod_table_name"][:6] != 'audit.':
                    conditions["svod_table_name"] = "audit." + conditions["svod_table_name"]

                conditions["container_field"] = conditions["container_field"].replace("`","")
                conditions["order_id_field"] = conditions["order_id_field"].replace("`","")
                conditions["date_field"] = conditions["date_field"].replace("`","")
                conditions["rks_fields"] = [rks_field for rks_field in conditions["rks_fields"] if rks_field[:2]!='--']

                table_info = get_table_info(
                    conditions["svod_table_name"])
                
                conditions['svod_table_columns'] = get_svod_table_columns(
                    table_info,
                    conditions["container_field"],
                    conditions["order_id_field"])

                conditions['max_column_len'] = get_max_column_len(
                    table_info,
                    conditions["date_field"],
                    conditions["with_vat"],
                    conditions["esu_id_columns"])

                print(Fore.CYAN) 
                pprint(conditions, width=120)
                print(Fore.RESET)

            with open(os.path.join(os.getcwd(), 'conditions.json'), 'r', encoding="utf-8") as file:  
                conditions['json'] = file.read()
                # print(conditions['json'])


            loader = FileSystemLoader(os.path.join(os.getcwd(),'templates'))
            env = Environment(loader=loader, trim_blocks=True, lstrip_blocks=True)

            pyperclip.copy(
                f"{env.get_template('main.sql').render(conditions)}"
            )

            print(Fore.GREEN + "SQL скопирован в буфер обмена!" + Fore.RESET)

        except Exception as e:
        #    print('aaaaaaaaaaaaaaaaa')
            print(Fore.RED, e, Fore.RESET)

    else:   
        print(Fore.RED, "Нет файла conditions.json", Fore.RESET)

    print(Fore.YELLOW + "Нажмите ввод, чтобы продолжить!" + Fore.RESET)
    input()        
    

