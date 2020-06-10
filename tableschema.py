#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
Class for capturing table schema information.

Note: Moved here from database.py to create a pickle-able class that doesn't require database.py imports.

"""


# standard lib
from collections import OrderedDict


class Column:

	def __init__(self, column=None):
		if column:
			self.column_name = column.column_name
			self.data_type = column.data_type
			self.is_nullable = column.is_nullable
			self.character_maximum_length = column.character_maximum_length
			self.numeric_precision = column.numeric_precision
			self.numeric_scale = column.numeric_scale
			self.datetime_precision = column.datetime_precision
			self.character_set_name = column.character_set_name
			self.collation_name = column.collation_name
		else:
			self.column_name = ''
			self.data_type = ''
			self.is_nullable = 'YES'
			self.character_maximum_length = None
			self.numeric_precision = None
			self.numeric_scale = None
			self.datetime_precision = None
			self.character_set_name = None
			self.collation_name = None

	def __str__(self):
		return f'Column {self.column_name}: {self.data_type}'


class TableSchema:

	def __init__(self, table_name, columns):
		self.table_name = table_name
		self.columns = OrderedDict()
		for column in columns:
			self.columns[column.column_name] = Column(column)

	def add_definition(self, definition):
		attributes = definition.split()

		column = Column()
		column.column_name = attributes[0]
		column.data_type = attributes[1]
		if len(attributes) > 2:
			column.character_maximum_length = int(attributes[2])

		# add column definition
		self.columns[column.column_name] = column

	def column_definitions(self, extended_definitions=None):
		# add extended definitions to dict of current definitions
		if extended_definitions:
			for definition in extended_definitions:
				self.add_definition(definition)

		# create a list of column specific definitions
		column_definitions = list()
		for column_name, column in self.columns.items():
			null_mode = 'null'
			if column.is_nullable == 'NO':
				null_mode = 'not null'

			details = ''
			if column.character_maximum_length:
				if column.character_maximum_length == -1:
					details = '(max)'
				else:
					details = f'({column.character_maximum_length})'

			if column.data_type == 'datetime2':
				# force highest precision
				details = '(7)'
			elif column.data_type in ('decimal', 'numeric', 'money', 'smallmoney'):
				details = f'({column.numeric_precision}, {column.numeric_scale})'
			elif column.data_type in ('float', 'real'):
				details = f'({column.numeric_precision})'

			# note indentation for visual debugging
			column_definitions.append(f'  "{column.column_name}" {column.data_type}{details} {null_mode}')

		return ',\n'.join(column_definitions)
