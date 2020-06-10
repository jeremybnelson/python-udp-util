#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
Generate CDC merge statement code.

table_object.schema_name = schema_name
table_object.table_name = table_name
table_object.column_names = '*'
merge_cdc = MergeCDC(table_object, table_pk)
sql = merge_cdc.merge()

# Notes
- DONE: if cdc=None; drop <table>
- DONE: create table <table> optionally with extended columns for udp_pk, udp_nk, etc
- create table <#table> = drop_temp_table(), create_temp_table
- insert into <#table> = insert_into_table
- update <#table> set udp_nk = concat_dl(pk_columns)
- merge <#table> into <table> based on pk = this file
- drop <#table> = drop_temp_table()

Optionally: insert merge stats into a table we can query for our activity log.

"""


# standard lib
import logging
import pathlib


# common lib
from common import delete_blank_lines
from common import expand
from common import spaces
from common import split


# udp lib
import config


# module level logger
logger = logging.getLogger(__name__)


def q(items):
	"""Decorates item/items with double-quotes to protect table/column names that may be reserved words."""
	if isinstance(items, (list, tuple, set)):
		# don't double double-quote items that are already double-quoted
		return [item if item.startswith('"') else f'"{item}"' for item in items]
	elif items.startswith('"'):
		# don't double double-quote items that are already double-quoted
		return items
	else:
		return f'"{items}"'


def add_alias(column_name, table_alias):
	"""Adds table_alias (if missing) and double-quotes table alias and column name."""
	column_name = column_name.replace('"', '')
	if '.' in column_name:
		table_alias, separator, column_name = column_name.partition('.')
	column_name = f'{q(table_alias)}.{q(column_name)}'
	return column_name


def add_aliases(column_names, table_alias='s'):
	"""Performs add_alias() on a list of column names."""
	return [add_alias(column_name, table_alias) for column_name in column_names]


def indent(text):
	"""Protect logical indentation of indented multi-line text values."""
	output = []
	for line in text.strip().splitlines():
		line = line.strip()
		if line.startswith('_ '):
			line = line[1:]
		elif line.startswith('__'):
			line = line[2:].strip()
		output.append(line)
	return '\n'.join(output)


class TestTableObject:

	def __init__(self, table_name):
		self.schema_name = ''
		self.table_name = table_name
		self.column_names = 'col1 col2 col3 col4 col5 col6'.split()


class MergeCDC:

	merge_template = '''
	__ -- s:source, t:target
	__ merge {schema_name}.{table_name} with (serializable) as t
	_  using {schema_name}._{table_name} as s
	_    on {match_condition}
	_  when matched then
	_    -- t.column1 = s.column1, ...
	_    update set
	__ {column_assignments}
	_  when not matched by target then
	_    insert
	_      -- (column1, column2, ...)
	_      ({column_names})
	_      values
	_      -- (s.column1, s.column2, ...)
	_      ({source_column_names});
	'''

	def __init__(self, table, extended_definitions=None):
		# indent template text
		self.merge_template = indent(self.merge_template)

		# object scope properties
		self.table = table

		# add extended_definition column names to table.column_names
		if extended_definitions:
			for column_definition in extended_definitions:
				column_name = column_definition.split()[0]
				if column_name not in self.table.column_names:
					self.table.column_names.append(column_name)

	def column_names(self):
		return ', '.join(q(self.table.column_names))

	def source_column_names(self):
		column_names = add_aliases(self.table.column_names, 's')
		return ', '.join(column_names)

	def column_assignments(self):
		output = []
		for column_name in self.table.column_names:
			target_column_name = add_alias(column_name, 't')
			source_column_name = add_alias(column_name, 's')
			assignment = f'{spaces(6)}{target_column_name} = {source_column_name}'
			output.append(assignment)
		return ',\n'.join(output)

	@staticmethod
	def match_condition(nk):
		"""FUTURE: t.udp_nk = s.udp_nk"""
		output = []
		for nk_column in split(nk):
			source_nk_column = add_alias(nk_column, 's')
			target_nk_column = add_alias(nk_column, 't')
			output.append(f'{target_nk_column}={source_nk_column}')
		return ' and '.join(output)

	@staticmethod
	def set_nk_value(nk):
		nk_columns = split(nk)
		nk_column_names = ', '.join(add_aliases(nk_columns, 't'))
		return f"concat_ws(':', {nk_column_names})"

	# noinspection PyUnusedLocal
	def merge(self, schema_name, nk):
		table_name = self.table.table_name
		match_condition = self.match_condition(nk)
		column_assignments = self.column_assignments()
		column_names = self.column_names()
		source_column_names = self.source_column_names()

		sql = expand(self.merge_template)
		return delete_blank_lines(sql.strip())


# test code
def main():

	for tables_file in sorted(pathlib.Path('conf/').glob('*.tables')):
		print(f'-- {tables_file}')
		schema_name = pathlib.Path(tables_file).stem

		table_config = config.Config(str(tables_file), config.TableSection)
		for table_name, table_object in table_config.sections.items():
			if table_name == 'default' or table_object.ignore_table:
				continue

			table_object = TestTableObject(table_name)
			merge_cdc = MergeCDC(table_object)

			# sql = merge_cdc.merge(schema_name, 'col1')
			sql = merge_cdc.merge(schema_name, 'col1, col3')
			print(f'{sql}\n')
			print(merge_cdc.set_nk_value('col1'))
			print(merge_cdc.set_nk_value('col1, col3, col5'))
			print()


# test code
if __name__ == '__main__':
	main()
