#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
option.py

Option management class.

Collects options from following sources:
- command line via --<option>[=<value>]
- environment variable specified at Option create time
- code based option injections, eg. [project].options = --<option>[=<value>] ...

Priority (highest to lowest): command line > environment variable > code injection.

Note:
- option names are non-case sensitive, eg. --log, --Log, and --LOG are considered equivalent
- option values are always string based
- option values with spaces are supported when values are delimited with double quotes, eg. --name="First Last"
- --<option>'s specified without [=<value>] are given values of '1', eg. --debug is equivalent to --debug=1
- all options track their final source of input for diagnostic purposes
"""


# standard lib
import logging
import os
import shlex


# common lib
from common import log_setup
from common import log_session_info
from common import option_value
from common import sys


# module level logger
logger = logging.getLogger(__name__)


class Option:

	def __init__(self, environ_var='', options=''):
		"""
		Loads option values in reverse priority order.
		Priority: command line > environment variable > code injection
		Load order: code injection > environment variable > command line
		"""

		# dict of option=value assignments
		self.option = dict()
		self.option_source = dict()

		# input into options
		environ_var = environ_var.upper()
		options = options.strip()
		command_line = str(sys.argv[1:])


		# lowest priority: code injected options (--option=value ...)
		context = f'program injection ({options})'
		for option in shlex.split(options):
			self.set_option(option, context)

		# medium priority: env var option (udp_<script> = --option=value ...)
		environ_options = os.getenv(environ_var, '').strip()
		# print(f'environ_var = {environ_var}')
		# print(f'environ val = {os.getenv(environ_var)}')
		# print(f'environ_options = {environ_options}')
		context = f'environment variable ({environ_var}={environ_options})'
		for option in shlex.split(environ_options):
			self.set_option(option, context)

		# highest priority: command line settings (--option=value ...)
		context = f'command line ({command_line})'
		for option in sys.argv[1:]:
			self.set_option(option, context)

	# def get(self, key, default=None):
	def __call__(self, key, default=''):
		"""Returns value of option key as a string; if not matched and no default, returns empty string."""

		# normalize key names to lowercase with whitespace and dashes stripped
		key = key.strip().strip('-').lower()

		# determine the option value
		if key in self.option:
			source = self.option_source[key]
			value = self.option[key]
		elif default:
			source = 'program default'
			value = str(default)
		else:
			source = 'no value set'
			value = ''

		# cleanup value
		value = value.strip()

		# diagnostic info on option lookups
		logger.info(f'Using option: {key}={value}; source: {source}')
		return value

	def dump(self):
		"""Dump option settings with source of each option value."""
		logger.info('Dumping options:')
		for key in sorted(self.option):
			logger.info(f'{key} = {self.option[key]}: source: {self.option_source[key]}')

	def set_option(self, option, context):
		"""Parse and validate option assignment. If valid, set option and track its context."""
		key, value = option_value(option)
		if key:
			self.option[key] = value
			self.option_source[key] = context


# temp test harness ...


# test code
def main():

	# mock command line options
	sys.argv.append('--debug=0')
	sys.argv.append('--test')
	logger.info(f'Command line: {sys.argv[1:]}')

	# mock environ variable options
	environ_var = 'test_env_var'

	# update os.environ[]; os.putenv() only updates environ for spawned child processes
	# Ref: https://stackoverflow.com/questions/17705419/
	os.environ[environ_var] = '--debug=2 --env=dev --codec=utf8 --file="this filename has spaces"'
	logger.info(f'Env var: {os.getenv(environ_var)}')

	# create an option object
	option = Option(environ_var, options='--debug --nowait --log=test.log --name="Malcolm Greene"')

	# query for option values
	option('debug')
	option('--NOWAIT')
	option('\t -log  ')
	option('--user', 'default-user')
	option('undefined-option')
	option('test')
	option('env')
	option('codec')

	# diagnostic dump
	option.dump()


# test code
if __name__ == '__main__':
	log_setup()
	log_session_info()
	main()
