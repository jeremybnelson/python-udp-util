#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
event.py

Track event metrics for SIEM telemetry.

Event: An unit of work with a duration, row count and size in bytes processed.
Future: Save in a SIEM standard CEF and/or LEEF file format.
"""


# standard lib
import datetime
import logging
import os
import socket
import time


# common lib
from common import save_jsonpickle
from common import split


# module level logger
logger = logging.getLogger(__name__)


class Stat:

	"""Capture event metrics."""

	def __init__(self, event_stage, event_name, event_type=None):
		self.event_stage = event_stage
		self.event_name = event_name
		self.event_type = event_type
		self.start_time = None
		self.end_time = None
		self.run_time = 0
		self.row_count = 0
		self.data_size = 0

	def start(self):
		self.start_time = datetime.datetime.now()
		logger.info(f'{self.event_name.capitalize()} started ...')

	def stop(self, row_count=0, data_size=0):
		self.end_time = datetime.datetime.now()
		self.run_time = (self.end_time - self.start_time).total_seconds()
		self.row_count = row_count
		self.data_size = data_size
		rows_and_bytes = f'({self.row_count:,} rows, {self.data_size:,} bytes)'
		logger.info(f'{self.event_name.capitalize()} complete in {self.run_time} secs {rows_and_bytes}')

	def row(self):
		""""Return event metrics as a dict that can be round tripped via JSON."""

		# row['event_name'] = self.event_name
		# row['event_type'] = self.event_type
		# row['start_time'] = self.start_time
		# row['end_time'] = self.end_time
		# row['run_time'] = self.run_time
		# row['row_count'] = self.row_count
		# row['data_size'] = self.data_size

		# build row dict based on private prefixed attributes
		row = dict()
		for key, value in self.__dict__.items():
			# ignore hidden attributes
			if key.startswith('_'):
				continue
			else:
				row[key] = value
		return row


class Events:

	"""
	Container for tracking a collection of events. Extends individual event rows with session context.

	Hierarchy
	- script
	- version
	- instance
	- server name
	- user name
	- dataset id
	- stage name
	- job id
	- step_name
	- step_type (job, step, table)
	"""

	def __init__(self, file_name=None, script_name=None, dataset_id=None, job_id=None, script_instance=None):

		# collection of events
		self.stats = dict()

		# file_name is default file that save() method writes event data to
		self.file_name = file_name

		# system info
		self.script_name = script_name
		self.script_instance = script_instance
		self.server_name = socket.gethostname()
		self.user_name = os.getlogin()

		# current job info
		self.dataset_id = dataset_id
		self.job_id = job_id

		# extra columns added to output
		extra_columns = 'script_name, script_version, script_instance, server_name, user_name, dataset_id, job_id'
		self.extra_columns = [column_name for column_name in split(extra_columns)]

	# stat_type = job, step (extract, compress, upload)
	def start(self, stat_name, stat_type=None):
		self.stats[stat_name] = Stat(stat_name, stat_type)
		self.stats[stat_name].start()

	def stop(self, stat_name, row_count=0, data_size=0):
		self.stats[stat_name].stop(row_count, data_size)

	# save stat info in a json file format to preserve data types
	def save(self, file_name=None):
		# make name and path of log output an option
		# also allow saving in a json vs csv format (with column header row)

		# TODO: Use common.py save_json or export_json for this type of info ???

		if not file_name and not self.file_name:
			file_name = 'job.log'
		elif not file_name:
			file_name = self.file_name

		rows = []
		for stat_name, stat in self.stats.items():
			row = dict()

			# session wide properties
			row['script_name'] = self.script_name
			row['script_instance'] = self.script_instance
			row['server_name'] = self.server_name
			row['user_name'] = self.user_name
			row['dataset_id'] = self.dataset_id
			row['job_id'] = self.job_id

			# merge in stat properties
			row = {**row, **stat.row()}

			# save the row for output
			rows.append(row)

		# save the output
		save_jsonpickle(file_name, rows)


# temp test harness ...


# test code
def test():
	# file_name=None, dataset_id=None, job_id=None, script_instance=None
	events = Events('test.log', 'test_script', 9000, 900, 1)
	events.start('job_event', 'job')
	events.start('table_event', 'table')
	events.start('step_event', 'step')
	time.sleep(1)
	events.stop('step_event', 10, 100)
	events.stop('table_event', 20, 200)
	events.stop('job_event', 30, 300)


# test code
if __name__ == '__main__':
	test()
