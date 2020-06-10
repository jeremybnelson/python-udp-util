#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
metric.py - see event_old.py and capture.py for original

Track metrics for SIEM telemetry.

Metric: Unit of work with a duration, count (row or other), size (in bytes processed)

Ties metric to a session object which carries session specific context.

###

Ideas:
- Save in a SIEM standard CEF and/or LEEF file format ???
- Capture warning, error, critical (exceptions) counts that occurred during a metric ???

###

TODO: Have events track by metric_id or metric object vs metric name so we can have
multiple events of the same name ???

TODO: Do we need a Metrics() class to act as a container ??? yes; for save events

TODO: Split out static session tracking into a session id and session row/record.

TODO: Add metric, session tracking to capture_2, archive_2, stage_2
TODO: Update udp.py with metric_* vs stat_* attributes
TODO: Update mssql.cfg with metric_* and session_* vs stat_* attributes

Instrumenting
Four types of metric are offered:
- Counter
- Gauge
- Summary
- Histogram

Doc on metric types and instrumentation best practices on how to use them
http://prometheus.io/
http://prometheus.io/docs/concepts/metric_types/
http://prometheus.io/docs/practices/instrumentation/#counter-vs.-gauge,-summary-vs.-histogram

"""


# standard lib
import datetime


# common lib
from common import save_jsonpickle


# module level logger
import logging
from common import log_setup
from common import log_session_info
logger = logging.getLogger(__name__)


class Metric:

	"""Metric tracking start, end, duration and other optional attributes."""

	metrics = list()

	def __init__(self, metric_type, metric_name):
		"""Define a new metric of metric_type, metric_name."""
		# metric context
		self.metric_type = metric_type
		self.metric_name = metric_name

		# metric timing
		self.start_time = None
		self.end_time = None
		self.run_time = 0

		# start the metric
		self.start()

	def __call__(self, **kwargs):
		"""Stop the current metric."""
		self.stop(**kwargs)

	def __enter__(self):
		"""Initialize a metric object."""

		# initialize, open, setup, acquire, etc
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		# TODO: Must also handle exception handling here !!!
		self.stop()

	@classmethod
	def clear(cls):
		"""Clear collection of global metrics."""
		cls.metrics.clear()

	@classmethod
	def dump(cls):
		"""Dump global metrics to debug log stream."""
		if not cls.metrics:
			logger.debug('No metrics to dump')
		else:
			for metric in cls.metrics:
				logger.debug(metric)

	def start(self):
		"""(Re)start a metric. Automatically invoked at object creation."""
		self.start_time = datetime.datetime.now()
		self.end_time = None
		self.run_time = 0
		logger.info(f'Metric({self.metric_type}.{self.metric_name}): started')

	def stop(self, **kwargs):
		"""Stop a metric and capture its **kwargs based attributes."""
		self.end_time = datetime.datetime.now()
		self.run_time = (self.end_time - self.start_time).total_seconds()

		values = list()
		for key, value in kwargs.items():
			setattr(self, key, value)
			if isinstance(value, int):
				values.append(f'{key}={value:,}')
			elif isinstance(value, float):
				values.append(f'{key}={value:,}')
		values = '; '.join(values)

		logger.info(f'Metric({self.metric_type}.{self.metric_name}): stopped in {self.run_time:,.2f} secs; {values}')

		# save metric to list of accumulated metrics
		self.metrics.append(self.__dict__)

	@classmethod
	def save(cls, file_name):
		"""Save global data to specified file in json format."""
		save_jsonpickle(file_name, cls.metrics)


# temp test harness ...


# test code
def main():
	"""Temporary test harness code for this module."""
	import time
	from random import randrange

	app_metric = Metric('app', 'capture')

	# noinspection PyUnusedLocal
	with Metric('step', 'test') as step_metric:
		delay = randrange(3)
		time.sleep(delay)
	app_metric(row_count=randrange(1000), data_size=randrange(10)*1000)

	cleanup_metric = Metric('step', 'cleanup')
	time.sleep(1)
	cleanup_metric(errors=randrange(5), warnings=randrange(10), jobs=randrange(20))

	# dump values
	Metric.dump()
	Metric.save('test.json')
	Metric.clear()
	Metric.dump()


# test code
if __name__ == '__main__':
	log_setup(log_level=logging.DEBUG)
	log_session_info()
	main()
