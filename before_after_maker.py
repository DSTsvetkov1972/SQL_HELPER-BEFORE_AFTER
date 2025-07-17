from jinja2 import Environment, FileSystemLoader
import os
import re
import pyperclip
from conditions import conditions




loader = FileSystemLoader('templates/rks')
env = Environment(loader=loader, trim_blocks=True, lstrip_blocks=True)

pyperclip.copy(
    f"{env.get_template('rks.sql').render(conditions, container_by_container_number=True)}"
    f"\n\n"
    f"\n\n"
    f"{env.get_template('rks.sql').render(conditions, container_by_container_number=False)}"
    f"\n\n"
    f"\n\n"
    f"{env.get_template('rks_answer.sql').render(conditions)}"
    f"\n\n"
    f"\n\n"
    f"{env.get_template('rks_before_after.sql').render(conditions, container_by_container_number=True)}"
    f"\n\n"
    f"\n\n"
    f"{env.get_template('rks_before_after.sql').render(conditions, container_by_container_number=False)}"
    f"\n\n"
    f"\n\n"   
    f"{env.get_template('rks_before_after_answer.sql').render(conditions)}"
    f"\n\n"
    f"\n\n"   
    f"{env.get_template('answer.sql').render(conditions)}"
)