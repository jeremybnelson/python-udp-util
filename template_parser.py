#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
Template parser: udp.template
Also identify {expressions}
"""

import arrow
import arrow.parser
import collections
import re


def to_datetime(text):
	try:
		value = arrow.get(text)
	except arrow.parser.ParserError:
		value = None
	return value


def to_int(text):
	try:
		value = int(text)
	except ValueError:
		value = None
	return value


class Parser:

	def __init__(self):
		self.commands = ('#define ', '#eof')
		self.comment_chars = ('/', '#', ';')
		self.file_name = ''
		self.defines = collections.defaultdict(str)
		self.error_count = 0

	@staticmethod
	def trace(text):
		print(text)

	def is_comment(self, line):
		comment_status = False

		line = line.lower()
		if line.startswith(self.commands):
			comment_status = False
		elif not line or line.startswith(self.comment_chars):
			comment_status = True

		return comment_status

	def load(self, file_name):
		# reset error count
		self.error_count = 0

		self.file_name = file_name
		with open(file_name) as input_stream:
			lines = input_stream.readlines()

		# force last define to be processed
		lines.append('#eof')

		define_name = ''
		define_value = ''

		for line_number, line in enumerate(lines, 1):
			# preserve left indentation
			line = line.rstrip()
			if self.is_comment(line):
				continue

			if not line.strip().lower().startswith(self.commands):
				# add line to current define
				define_value = f'{define_value}\n{line}'
			else:
				# new defines always save current definition
				command = line.partition(' ')[0].lower()
				if define_name:
					define_value = define_value.strip()
					self.defines[define_name] = define_value.strip()

				# start a new define
				define_name = line.partition(' ')[2].lower().strip()
				define_value = ''

				if command == '#eof':
					break

	def list_keys(self, sequence=None, ignore_empty_values=True):
		if not sequence:
			sequence = self.defines.keys()

		print(f'\nFile: {self.file_name}')
		for key in sequence:
			value = self.defines[key]
			if not value and ignore_empty_values:
				continue

			print(f'#DEFINE {key.lower()}')

	def list_values(self, sequence=None, ignore_empty_values=True):
		if not sequence:
			sequence = self.defines.keys()

		print(f'\nFile: {self.file_name}')
		for key in sequence:
			value = self.defines[key]
			if not value and ignore_empty_values:
				continue

			if '\n' in value:
				print(f'#DEFINE {key.lower()}\n{value}\n')
			else:
				print(f'#DEFINE {key.lower()} = {value}')

	def list_expressions(self):
		expressions = set()
		for key, value in self.defines.items():
			for expression in re.findall(r'({.*?\})', value):
				expressions.add(expression)
		for expression in sorted(expressions):
			print(expression)

	def error(self, message, bad_value=None):
		self.error_count += 1
		if bad_value is None:
			print(f'{self.file_name}: {message}')
		else:
			print(f'{self.file_name}: {message} ({bad_value})')


class TableFileParser(Parser):

	def clean(self, key):
		key = key.strip().lower()
		self.defines[key] = self.defines[key].strip().lower()
		return self.defines[key]

	def validate(self):
		# clean non-validated values
		self.clean('table_prefix')
		self.clean('table_suffix')
		self.clean('cdc_select')

		# must have a table_name
		self.clean('table_name')
		if not self.defines['table_name']:
			self.error('Missing table_name value')

		# must have valid cdc_type
		cdc_type = self.clean('cdc_type')
		if cdc_type not in 'timestamp rowversion replace update drop'.split():
			self.error(f'Illegal cdc_type value', cdc_type)

		# must have cdc_naturalkey for timestamp/rowversion cdc
		cdc_naturalkey = self.clean('cdc_naturalkey')
		if cdc_type in 'timestamp rowversion'.split() and not cdc_naturalkey:
			self.error(f'Missing cdc_naturalkey for cdc_type = {cdc_type}')

		# timestamp and rowversion cdc must have matching cdc expression
		cdc_timestamp = self.clean('cdc_timestamp')
		if cdc_type == 'timestamp' and not cdc_timestamp:
			self.error('Missing cdc_timestamp value for cdc_type = timestamp')
		cdc_rowversion = self.clean('cdc_rowversion')
		if cdc_type == 'rowversion' and not cdc_rowversion:
			self.error('Missing cdc_rowversion value for cdc_type = rowversion')

		# first_timestamp must be a valid datetime or blank
		first_timestamp = self.clean('first_timestamp')
		if first_timestamp and to_datetime(first_timestamp) is None:
			self.error('Illegal first_timestamp value', first_timestamp)
		else:
			if not first_timestamp:
				first_timestamp = '1900-01-01'
			first_timestamp = str(to_datetime(first_timestamp))
			self.defines['first_timestamp'] = first_timestamp

		# first_rowversion must be a valid integer or blank
		first_rowversion = self.clean('first_rowversion')
		if first_rowversion and to_int(first_rowversion) is None:
			self.error('Illegal first_rowversion value', first_rowversion)
		else:
			first_rowversion = '0'
			self.defines['first_rowversion'] = first_rowversion

		# auto-discover column definitions if not supplied

		# auto-discover missing table schema


# test code - re-test 2018-04-10
def main():

	table_key_sequence = '''
	table_name
	table_prefix
	table_suffix
	cdc_type
	cdc_naturalkey
	cdc_select
	cdc_timestamp
	cdc_timezone
	first_timestamp
	cdc_rowversion
	first_rowversion
	column_definitions
	table_schema
	'''.split()

	# project file
	namespace = 'amc-amp-01-sales'
	sdlc_phase = 'uat'
	connection = f'{namespace}_{sdlc_phase}'

	# application code knows its database and schema
	# database = 'udp_staging'
	# schema = {namespace} or 'udp_admin'

	# parser = Parser()
	# parser.load('udp.template')
	# parser.list_values()
	# parser.list_expressions()

	parser = TableFileParser()
	parser.load('conf/credit_cards_1.table')
	parser.validate()
	# parser.list_keys(table_key_sequence)
	parser.list_values(table_key_sequence)

	# column_definitions = autodiscover and translate to target
	# table_schema = autodiscover and cache


# test code
if __name__ == '__main__':
	main()
