#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
schedule class:

schedule.wait() until next scheduled time or poll cycle.

Check schedule.is_valid value; if False, then schedule has errors; do not use.
Schedule validation will output warnings to log stream.

Background
- schedule is accurate to the minute
- all hour values entered in military 24-hour format, eg. 1pm = 13

Basic configuration option
- poll_frequency = secs (int); default 5
- daily_at = 0 or more HH:MM times to run (str, list of str)
- hourly_at = 0 or more minute values to run on the hour (int, list of int)

Both daily_at and hourly_at values can be set.

If no daily_at or hourly_at values set, then schedule triggers on every poll cycle that passes skip conditions.

Options to skip() triggered events on specific date/time conditions
- hours_of_day = 0 or more hours in 24-hour format (int, list of int)
- days_of_week = 0 or more lowercase 3-char day abbreviations, eg. sun, mon ... sat (str, list of str)
- days_of_month = 0 or more N values between 1-31 or 0, -N for days relative to end-of-month (int, list of int)
- days_of_year = 0 or more YYYY-MM-DD values (str, list of str)

Subclass
- is_stopped() - check for external stop or pause/resume conditions
- current_time - return date/timestamps from alternate time sources, eg. a database server vs current server

Future
- support specific run time (vs skip) conditions for execution on specific day-of-[week, month, year]
- specific run time examples - weekly_at (day[s] of week and time[s]), monthly_at (day[s] of month and time[s]), etc

"""


# standard lib
import calendar
import datetime
import logging
import time


# common lib
from common import delete_file
from common import load_lines
from common import log_setup
from common import log_session_info
from common import now
from common import script_name
from common import to_int
from common import to_list
from common import trim_seconds


# udp class
from section import SectionSchedule


# module level logger
logger = logging.getLogger(__name__)


class Schedule:

	def __init__(self, schedule_section):
		"""Initialize a Schedule object."""

		# schedule settings
		self.poll_frequency = to_int(schedule_section.poll_frequency)
		self.daily_at = self.format_daily_times(schedule_section.daily_at)
		self.hourly_at = to_int(to_list(schedule_section.hourly_at))

		# skip conditions
		self.skip_hours_of_day = to_int(to_list(schedule_section.skip_hours_of_day))
		self.skip_days_of_week = to_list(schedule_section.skip_days_of_week)
		self.skip_days_of_month = to_int(to_list(schedule_section.skip_days_of_month))
		self.skip_days_of_year = to_list(schedule_section.skip_days_of_year)

		# lowercase skip days of week
		self.skip_days_of_week = [day.lower() for day in self.skip_days_of_week]

		# track state
		self.start_time = None
		self.last_time = None
		self.poll_count = 0
		self.run_count = 0

		# optional poll_message
		self.poll_message = ''

		# validate schedule config
		self.is_valid = True
		self.validate_schedule()

	def validate_schedule(self):
		"""Validate schedule configuration."""
		# validate poll_frequency 0 ... N
		self.validate([self.poll_frequency], context='poll_frequency', min_value=0, max_value=300)

		# validate daily_at hh:mm
		self.validate(self.daily_at, context='daily_at', is_valid_func=self.is_hh_mm)

		# validate hourly_at 0 ... 59
		self.validate(self.hourly_at, context='hourly_at', min_value=0, max_value=59)

		# validate skip_hours_of_day 0 ... 23
		self.validate(self.skip_hours_of_day, context='skip_hours_of_day', min_value=0, max_value=23)

		# skip days of week sun ... sat
		days_of_week = 'sun mon tue wed thu fri sat'.split()
		self.validate(self.skip_days_of_week, context='skip_days_of_week', legal_values=days_of_week)

		# validate skip_days_of_month 1...31, -1 ... -7
		self.validate(self.skip_days_of_month, context='skip_days_of_month', min_value=-7, max_value=31)

		# validate skip_days_of_year mm-dd (must exist, plus 02_29)
		self.validate(self.skip_days_of_year, context='skip_days_of_year', is_valid_func=self.is_mm_dd)

	@staticmethod
	def format_daily_times(daily_times):
		"""Format daily time values as HH:MM adding leading/trailing 0's and colon if missing."""

		# make sure daily_times is a list
		daily_times = to_list(daily_times)

		output = []
		for daily_time in daily_times:
			# if missing :MM, add :MM as suffix
			if ':' not in daily_time:
				daily_time = f'{daily_time}:00'

			# pad single H: as 0H:
			if daily_time[1] == ':':
				daily_time = f'0{daily_time}'

			output.append(daily_time)
		return output

	@staticmethod
	def current_time():
		"""Subclass to use timestamps from other sources such as database servers."""

		# trim seconds/microseconds from time to return consistent HH:MM:00.0 values
		return trim_seconds(now())

	@staticmethod
	def is_stopped():
		""""Subclass with logic to check for external condition that stops or pauses/resumes wait() loop."""
		return False

	def is_skipped(self, current_time):
		"""Return true if current time block (yyyy-mm-dd-hh-mm) should be skipped."""

		# poll only schedule check
		poll_only = (not self.daily_at) and (not self.hourly_at)

		# only perform last time skip test if we're not on a poll-only schedule
		if not poll_only and self.last_time == current_time:
			# logger.info(f'Skipping repeat within same time block; last_time = {self.last_time}')
			return True

		# all other skip conditions apply to both poll-only and specific daily/hourly schedules

		# skip hours
		hour_of_day = current_time.hour
		if hour_of_day in self.skip_hours_of_day:
			logger.info(f'Skipping hour {hour_of_day}')
			return True

		# skip_days_of_week (as sun ... sat strings)
		day_of_week = f'{current_time:%a}'.lower()
		if day_of_week in self.skip_days_of_week:
			logger.info(f'Skipping day of week {day_of_week}')
			return True

		# convert negative skip_days_of_month to days relative to end-of-month (0=last-day-of-month)
		# Note: We do this each test to because current year and month will change for long running sessions.
		# skip_days_of_month = [day if day > 0 else last_day_of_month + day for day in self.skip_days_of_month]
		last_day_of_month = calendar.monthrange(current_time.year, current_time.month)[1]
		skip_days_of_month = []
		for day in self.skip_days_of_month:
			if day < 1:
				# we add day because it's already a negative value
				day = last_day_of_month + day
			skip_days_of_month.append(day)

		# supports both N and -N day values to skip
		day_of_month = current_time.day
		if day_of_month in skip_days_of_month:
			logger.info(f'Skipping day of month {day_of_month}')
			return True

		# skip_days_of_year (as mm-dd)
		day_of_year = f'{current_time:%m-%d}'
		if day_of_year in self.skip_days_of_year:
			logger.info(f'Skipping day of year {day_of_year}')
			return True

		# if no skip conditions matched, then return False
		return False

	def ready(self, current_time, trigger):
		self.last_time = current_time
		self.run_count += 1
		logger.info(f'Returning ready ({trigger}); run_count = {self.run_count}')
		return True

	def wait(self):
		# remember our start time
		if not self.start_time:
			self.start_time = self.current_time()

		while True:
			self.poll_count += 1
			for loop in range(self.poll_frequency):
				time.sleep(1)
				if self.is_stopped():
					return False

			current_time = self.current_time()

			if self.poll_message:
				logger.info(f'{current_time:%Y-%m-%d %H:%M}: {self.poll_message}')
			else:
				logger.info(f'{current_time:%Y-%m-%d %H:%M} - {script_name()}: {self}')

			# check skip conditions
			if self.is_skipped(current_time):
				continue

			# if no daily_at or hourly_at rules then trigger match
			if (not self.daily_at) and (not self.hourly_at):
				return self.ready(current_time, 'poll trigger')

			# check daily at hh:mm match
			hour_minute = f'{current_time:%H:%M}'
			if hour_minute in self.daily_at:
				return self.ready(current_time, f'daily at: {hour_minute}')

			# check hourly at mm match
			minute = current_time.minute
			if minute in self.hourly_at:
				return self.ready(current_time, f'hourly at: {minute}')

	def __str__(self):
		"""Summarize schedule into a succinct string based on non-empty property values."""
		output = []

		# if self.start_time:
		# 	output.append(f'start time={self.start_time:%Y-%m-%d %H:%M}')

		if self.poll_frequency:
			output.append(f'poll={self.poll_frequency}')
		if self.daily_at:
			output.append(f'daily={self.daily_at}')
		if self.hourly_at:
			output.append(f'hourly={self.hourly_at}')
		if self.skip_hours_of_day:
			output.append(f'skip hours={self.skip_hours_of_day}')
		if self.skip_days_of_week:
			output.append(f'skip days of week={self.skip_days_of_week}')
		if self.skip_days_of_month:
			output.append(f'skip days of month={self.skip_days_of_month}')
		if self.skip_days_of_year:
			output.append(f'skip days of year={self.skip_days_of_year}')
		return '; '.join(output).replace("'", '')

	# noinspection PyMethodMayBeStatic
	def validate(self, value_list, context='', min_value=None, max_value=None, legal_values=None, is_valid_func=None):
		for item in value_list:
			if min_value is not None and item < min_value:
				logger.warning(f'{context}: {item} < {min_value}')
				self.is_valid = False
			if max_value is not None and item > max_value:
				logger.warning(f'{context}: {item} > {max_value}')
				self.is_valid = False
			if legal_values and item not in legal_values:
				logger.warning(f'{context}: {item} not in {legal_values}')
				self.is_valid = False
			if is_valid_func and not is_valid_func(item):
				logger.warning(f'{context}: {item} is bad value')
				self.is_valid = False

	@staticmethod
	def is_hh_mm(hh_mm_value):
		"""Return True hh_mm_value matches a legal hour:minute value."""
		hh_value, separator, mm_value = hh_mm_value.partition(':')
		hh_value = to_int(hh_value, -1)
		mm_value = to_int(mm_value, -1)
		return (0 <= hh_value <= 23) and (0 <= mm_value <= 59)

	@staticmethod
	def is_mm_dd(mm_dd_value):
		"""
		Return True if mm_dd_value matches a legal month-day value.
		Accepts 02-29 as a leap year day as well.
		"""
		mm_value, separator, dd_value = mm_dd_value.partition('-')
		mm_value = to_int(mm_value, 0)
		dd_value = to_int(dd_value, 0)

		# allow 02-29 (leap year) as legal value
		if mm_value == 2 and dd_value == 29:
			is_valid = True

		else:
			try:
				# year value does not matter; 2000 chosen as an arbitrary year
				# print(f'datetime(2000, {mm_value}, {dd_value}) = {datetime.datetime(2000, mm_value, dd_value)}')
				datetime.datetime(2000, mm_value, dd_value)
				is_valid = True
			except ValueError:
				is_valid = False

		return is_valid

	def dump(self):
		"""Diagnostic information about current schedule."""
		logger.info(f'Schedule: poll frequency = {self.poll_frequency}')
		logger.info(f'Schedule: daily at = {self.daily_at}')
		logger.info(f'Schedule: hourly at = {self.hourly_at}')
		logger.info(f'Schedule: skip hours of day = {self.skip_hours_of_day}')
		logger.info(f'Schedule: skip days of week = {self.skip_days_of_week}')
		logger.info(f'Schedule: skip days of month = {self.skip_days_of_month}')
		logger.info(f'Schedule: skip days of year = {self.skip_days_of_year}')
		logger.info(f'Schedule: last time = {self.last_time}')
		logger.info(f'Schedule: poll count = {self.poll_count}')
		logger.info(f'Schedule: run count = {self.run_count}')


# temporary test harness ...


class CommandFileSchedule(Schedule):

	def is_stopped(self):
		"""Sample is_stopped() implementation checking command file for command to execute."""

		stop_status = False
		command_file_name = f'{script_name()}.command'
		command = load_lines(command_file_name, line_count=1)
		command = command.strip().lower()
		if command:
			logger.info(f'Command: {command}')
			delete_file(command_file_name)
			if command in ('die', 'exit', 'kill', 'quit', 'stop'):
				stop_status = True
			if command in ('diagnostics', 'dump', 'info'):
				self.dump()

		return stop_status


# test code
def main():
	# schedule = TestSchedule()
	# schedule = CommandFileSchedule)

	test_schedule = SectionSchedule()
	test_schedule.daily_at = '0, 3, 5:05, 16:24'
	test_schedule.hourly_at = '3, 5, 19, 21, 22, 23, 25'
	test_schedule.hours_of_day = '15'
	test_schedule.days_of_week = 'Sat'
	test_schedule.days_of_month = '29, -3'
	schedule = CommandFileSchedule(test_schedule)

	# logger.info(f'Schedule: {schedule}')
	schedule.dump()
	while True:
		if schedule.wait():
			logger.info('Working ...')
		else:
			logger.info('Stop condition detected')
			schedule.dump()
			break


# test code
if __name__ == '__main__':
	log_setup()
	log_session_info()
	main()
