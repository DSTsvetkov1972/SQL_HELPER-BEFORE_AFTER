from jinja2 import Environment, FileSystemLoader
from pprint import pprint
import pyperclip
from colorama import Fore, init, Style
import json  
import os

init()

while True:
    if os.path.exists('conditions.json'):
        print(Style.BRIGHT + Fore.YELLOW + "Задайте параметры в файле conditions.json и нажмите ввод!" + Fore.RESET)
        os.startfile(os.getcwd())
        input()

        try:
            with open('conditions.json', 'r', encoding="utf-8") as file:  
                conditions = json.load(file)
                print(Fore.CYAN) 
                pprint(conditions)
                print(Fore.RESET)

            loader = FileSystemLoader(os.path.join(os.getcwd(), 'templates'))
            env = Environment(loader=loader, trim_blocks=True, lstrip_blocks=True)

            pyperclip.copy(
                f"{env.get_template('main.sql').render(conditions)}"
            )

            print(Fore.GREEN + "SQL скопирован в буфер обмена!" + Fore.RESET)

        except Exception as e:
            print(Fore.RED, e, Fore.RESET)

    else:   
        print(Fore.RED, "Нет файла conditions.json", Fore.RESET)

    print(Fore.MAGENTA + "Нажмите ввод, чтобы продолжить!" + Fore.RESET)
    input()        
    

