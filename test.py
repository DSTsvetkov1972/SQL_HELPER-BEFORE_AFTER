import os

conditions={}
with open(os.path.join(os.getcwd(), 'conditions.json'), 'r', encoding="utf-8") as file:  
    conditions['json'] = file.read()
    print(conditions['json'])