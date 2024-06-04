import pyperclip
from colorama import Fore
   
#source = input('Введите список полей: ')#"order_id,container_number,`SVOD.Номер контейнера`,points_from_catakog_name,points_to_catalog_namer,station_name_from,station_name_to,date_end,amount_in_rub_without_vat,amount_in_contract_currency_without_vat"
source  = "`Date_pop`,EsrOper,`DS.country_code`"

argument_left  = "`Date_pop`".replace('`','')
argument_right = "``SVOD.Дата документа``".replace('`','')

fields_list = source.replace("`","").split(",")

field_list_min_length = min(map(lambda x: len(x), fields_list))
print(field_list_min_length)

field_list_max_length = max(map(lambda x: len(x), fields_list))
print(field_list_max_length)





before_list = [f"argMaxIf(`{field}`" + " "*(field_list_max_length - len(field)) +  f", `{argument_left}`, `{argument_left}` <= `{argument_right}`) AS " + f"`before_{field}`,\n" for field in fields_list]
after_list = [f"argMinIf(`{field}`" + " "*(field_list_max_length - len(field)) +  f", `{argument_left}`, `{argument_left}` >= `{argument_right}`) AS " + f"`after_{field}`,\n" for field in fields_list]

before_date_diff = f"IF(`before_{argument_left}` = '1970-01-01 03:00:00', Null, date_diff( DAY,`{argument_right}`,`before_{argument_left}`)) AS before_date_diff,\n"
after_diff_after = f"IF(`after_{argument_left}` = '1970-01-01 03:00:00', Null, date_diff( DAY,`{argument_right}`,`after_{argument_left}`)) AS after_date_diff,\n"

line_max_length = max(map(lambda x: len(x), before_list))

sql = ('-'*line_max_length + '\n' + 
       "'BEFORE-->'," + '\n' +
       '-'*line_max_length + '\n' +             
       before_date_diff + 
       "".join(before_list) + 
       '-'*line_max_length + '\n' + 
       "'<--BEFORE AFTER-->'," + '\n' +   
       '-'*line_max_length + '\n' +            
       after_diff_after + 
       "".join(after_list) + 
       '-'*line_max_length + '\n' +       
       "'<--AFTER'," + '\n' +      
       '-'*line_max_length + '\n')
print(Fore.GREEN, sql, Fore.WHITE)


pyperclip.copy(sql)
