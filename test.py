from jinja2 import Environment, FileSystemLoader
import os
import pyperclip
from conditions import conditions, rks_fields




loader = FileSystemLoader(['templates','mapping_fields'])
env = Environment(loader=loader, trim_blocks=True, lstrip_blocks=True)
template = env.get_template('select_fields.sql')

print(template.render(rks_fields=rks_fields))

