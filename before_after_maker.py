from jinja2 import Environment, FileSystemLoader
from pprint import pprint
import pyperclip
from colorama import Fore, init, Style
import json  
import os

init()

while True:

    print(Fore.MAGENTA + "="*41 + Fore.RESET)    
    print(Fore.MAGENTA + "* BEFORE_AFTER_MAKER v.2025-08-18-14:37 *" + Fore.RESET)
    print(Fore.MAGENTA + "="*41 + Fore.RESET)    

    if os.path.exists(os.path.join(os.getcwd(), 'conditions.json')):
        print(Style.BRIGHT + Fore.YELLOW + "Задайте параметры в файле conditions.json и нажмите ввод!" + Fore.RESET)
        os.startfile(os.getcwd())
        input()

        try:
            with open(os.path.join(os.getcwd(), 'conditions.json'), 'r', encoding="utf-8") as file:  
                conditions = json.load(file)

                if conditions["svod_table_name"][:6] != 'audit.':
                    conditions["svod_table_name"] = "audit." + conditions["svod_table_name"]

                conditions["container_field"] = conditions["container_field"].replace("`","")
                conditions["order_id_field"] = conditions["order_id_field"].replace("`","")
                conditions["date_field"] = conditions["date_field"].replace("`","")                
    
                print(Fore.CYAN) 
                pprint(conditions)
                print(Fore.RESET)

            loader = FileSystemLoader(os.path.join(os.getcwd(),'templates'))
            env = Environment(loader=loader, trim_blocks=True, lstrip_blocks=True)

            pyperclip.copy(
                f"{env.get_template('main.sql').render(conditions)}"
            )

            print(Fore.GREEN + "SQL скопирован в буфер обмена!" + Fore.RESET)

        except Exception as e:
            print(Fore.RED, e, Fore.RESET)

    else:   
        print(Fore.RED, "Нет файла conditions.json", Fore.RESET)

    print(Fore.YELLOW + "Нажмите ввод, чтобы продолжить!" + Fore.RESET)
    input()        
    

