from jinja2 import Environment, FileSystemLoader
import os
import pyperclip
from conditions import conditions




loader = FileSystemLoader('templates/rks')
env = Environment(loader=loader, trim_blocks=True, lstrip_blocks=True)
template = env.get_template('rks.sql')

pyperclip.copy(template.render(conditions, container_by_container_number=False))

