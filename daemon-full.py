#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
TODO: 2018-09-19 Temporarily replaced with daemon.py (daemon-lite).

TODO: 2018-08-15 - restart should re-spawn vs loop?
TODO: 2018-08-10 - integrate with app loader/publisher logic
TODO: 2018-08-01 - replace print with logging (2 handlers)


Daemon class.

Monitors a *.listen file for daemon commands
TODO: *.listen file should be in sessions/* folder structure

Job control:
- stop
- restart
- cancel
- pause
- continue

Monitoring:
- counters
- uptime

Plus custom commands added via subclassing (doc on wiki)
"""


# standard lib
import collections
import datetime
import os
import pathlib
import sys
import time


# common lib
from common import duration


class DaemonStop(Exception):
	"""Raise to stop the daemon."""
	pass


class DaemonRestart(Exception):
	"""Raise to restart daemon."""
	pass


class DaemonCancel(Exception):
	"""Raise to cancel current process without restarting or stopping."""
	pass


class Daemon:

	def __init__(self):
		self.trace_flag = False
		self.start_time = 0
		self.counters = collections.defaultdict(int)

		'''
		# listener file name defaults to class name + '.listen'
		self.listener_file_name = self.__class__.__name__.lower() + '.listen'
		'''

		# listener file name defaults to script name + '.listen'
		script_name = sys.argv[0]
		self.listener_file_name = pathlib.Path(script_name).stem + '.listen'

	def get_command(self):
		listener_file_name = self.listener_file_name

		command = ''
		if os.path.exists(listener_file_name):
			# ignore IOError's when attempting to read or delete a locked command file
			try:
				with open(listener_file_name) as input_stream:
					command = input_stream.read()
				os.remove(listener_file_name)
			except IOError:
				pass

		return command

	def listen(self):
		command = self.get_command()
		command = ' '.join(command.split())
		command, separator, message = command.partition(' ')
		command = command.lower()

		if command:
			self.trace(f'Command: {command}({message})')

		method_name = f'do_{command}'
		if hasattr(self, method_name):
			self.count(command)
			getattr(self, method_name)(message)
		elif command:
			self.count('unknown')
			self.unhandled_command(command, message)
		return

	def trace(self, message):
		if self.trace_flag:
			print(message)

	def count(self, counter_name):
		self.counters[counter_name] += 1

	def run(self, *args, **kwargs):
		self.trace(f'Starting daemon - monitoring {self.listener_file_name}')
		self.start_time = int(time.time())
		self.setup(args, kwargs)
		while True:
			self.trace(f'Daemon (re)starting')
			self.start()
			try:
				while True:
					try:
						self.main()
						self.count('run')
					except DaemonCancel:
						pass

			except DaemonRestart:
				pass

			except DaemonStop:
				break

		self.cleanup()
		self.trace(f'Daemon stopped')

	def do_stop(self, message):
		raise DaemonStop

	def do_restart(self, message):
		raise DaemonRestart(message)

	def do_cancel(self, message):
		raise DaemonCancel(message)

	def do_pause(self, message):
		while True:
			time.sleep(0.5)
			command = self.get_command()
			if command in ['stop', 'restart', 'continue']:
				break
			elif command:
				self.trace(f'Command: {command}({message}) (while paused)')

	# noinspection PyUnusedLocal
	def do_uptime(self, message):
		"""Report uptime and running since timestamp."""
		up_time = time.time() - self.start_time
		start_time = datetime.datetime.fromtimestamp(self.start_time)
		self.trace(f'Uptime: {duration(up_time)}; running since {start_time}')

	def do_counters(self, message=''):
		counters = message.split()
		if not counters:
			counters = 'restart cancel run'.split()
		elif counters[0] == '-':
			counters = sorted(self.counters.keys())

		for counter_name in counters:
			self.trace(f'{counter_name.capitalize()}: {self.counters[counter_name]}')

	# noinspection PyUnusedLocal
	def do_help(self, message):
		"""Display commands or help for a specific command."""
		commands = [command[3:] for command in dir(self) if command.startswith('do_')]
		# self.trace(f'Help: {", ".join(commands)}')

		for command in commands:
			method = getattr(self, f'do_{command}')
			command_help = getattr(method, '__doc__')
			if command_help:
				self.trace(f'{command}: {command_help}')

	def unhandled_command(self, command, message):
		self.trace(f'Unhandled command: {command}({message})')

	def setup(self, *args, **kwargs):
		"""Override: Optional setup code (called once)."""
		pass

	def start(self):
		"""Override: Code called on initial start and subsequent restarts."""
		pass

	def main(self):
		"""Override: Main code goes here."""
		pass

	def cleanup(self):
		"""Override: Optional cleanup code."""
		pass


# test code
class TestDaemon(Daemon):

	def setup(self, *args, **kwargs):
		self.trace_flag = True

	def main(self):
		for step in range(10):
			print(f'Processing {step}')
			time.sleep(1)
			self.listen()

	def do_echo(self, message):
		"""Echo back message (for testing)."""
		self.trace(f'Echo: {message}!')


# test code
def main():
	if len(sys.argv) == 1:
		# create test daemon if no command line option
		test_daemon = TestDaemon()
		test_daemon.run()

	elif len(sys.argv) >= 3 and sys.argv[1] == '-c':
		# send command to test daemon via -c <command> via command line
		command = ' '.join(sys.argv[2:])
		try:
			listener_file_name = 'daemon.listen'
			with open(listener_file_name, 'w') as output_stream:
				output_stream.write(command)

		except IOError:
			pass


# test code
if __name__ == '__main__':
	main()
