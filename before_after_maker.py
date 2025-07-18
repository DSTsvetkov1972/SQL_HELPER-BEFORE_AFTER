from jinja2 import Environment, FileSystemLoader
from pprint import pprint
import pyperclip
from colorama import Fore
import json  


with open('conditions.json', 'r', encoding="utf-8") as file:  
    conditions = json.load(file)
    print(Fore.CYAN) 
    pprint(conditions)
    print(Fore.RESET)

loader = FileSystemLoader('templates')
env = Environment(loader=loader, trim_blocks=True, lstrip_blocks=True)

pyperclip.copy(
    f"{env.get_template('main.sql').render(conditions)}"
)

print(Fore.GREEN + "SQL скопирован в буфер обмена!" + Fore.RESET)
