#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
mutex.py

Manages a local file system based pid file as a mutex
Used to prevent multiple instances of the same *logical* process name
Pid files are an industry convention - see notes below.

Features stale pid file detection and cleanup
- deletes pid files older than last system boot time
- deletes pid files with non-active process ids

Limitations: Pid files will not work on network file systems, nor across machines.

Pid file conventions

Ref: Filesystem Hierarchy Standard
http://www.pathname.com/fhs/pub/fhs-2.3.html

Paraphrased notes
- The naming convention for pid files is <process-name>.pid
- Pid files file must consist of the process identifier (pid) in ASCII-encoded decimal, followed by a newline character
- For example, if an acquiring process id (pid) is 25, <process-name>.pid would contain 3 characters: 2, 5, \n
- Programs that read pid files should be somewhat flexible in what they accept
- Ignore extra whitespace, leading zeroes, absence of the trailing newline, or additional lines in the pid file

Ref: Pid and lock files
https://unix.stackexchange.com/questions/12815/what-are-pid-and-lock-files-for

Pid files are written by programs to record their process id while they are starting.

Pid files
- Serve as a signal to other processes that that particular program is running, or at least started successfully
- Allow one to check if a specific process is running and optionally issue a plain kill command to terminate it
- Provide a technique to determine if a previous running instance did not exit successfully

Future: Alternative implementations
- Win32 CreateMutex API (no pid files to cleanup; not cross platform)
- os.mkdir() folder based mutex (requires separate file to track pid owner; have to cleanup manually)
"""


# standard lib
import logging
import os
import time


# common lib
from common import boot_datetime
from common import delete_file
from common import file_modify_datetime
from common import is_file
from common import is_process
from common import load_lines
from common import log_session_info
from common import log_setup


# module level logger
logger = logging.getLogger(__name__)


class FileMutex:

	"""Cross platform pid file based mutex."""

	def __init__(self, file_name, debug_id=None):
		"""Initialize a mutex object. Optional debug_id used for tracking caller id."""
		self.debug_id = debug_id
		self.file_name = file_name

		# these are reset by reset()
		self.file_handle = None
		self.pid = None

	def acquire(self):
		"""Attempt to acquire a mutex. Returns True if mutex is acquired."""
		self.reset()
		try:
			# handle stale pids (older than boot time, pid not a process)
			if self.is_stale():
				logger.debug(f'{self} - deleting stale pid file')
				delete_file(self.file_name, ignore_errors=True)

			# open() with exclusive access ('x') is an atomic condition
			# Note: We keep this file handle open until we close our mutex.
			pid = os.getpid()
			self.file_handle = open(self.file_name, 'x')

			# by convention: pid files consist of the pid and a newline character
			self.file_handle.write(f'{pid}\n')
			self.file_handle.flush()

			# if we made it this far then mutex successfully acquired
			self.pid = pid
			logger.debug(f'{self}:acquire')
			return True

		except OSError:
			# failed to acquire mutex
			self.reset()
			logger.debug(f'{self}:acquire')
			return False

	def get_pid(self):
		"""Return int pid from a potential pid file. Returns None if pid file doesn't exist."""
		pid = load_lines(self.file_name, line_count=1).strip()
		if pid:
			return int(pid)
		else:
			return None

	def is_acquired(self):
		"""Returns True if mutex has been acquired."""
		return bool(self.file_handle)

	def is_stale(self):
		"""Returns True if pid file is stale (older than boot time or contains inactive pid)."""
		reason = ''

		if is_file(self.file_name):
			# pid file exists so check if its stale
			pid = self.get_pid()
			pid_datetime = file_modify_datetime(self.file_name)
			if pid_datetime < boot_datetime():
				reason = f' - pid file older ({pid_datetime}) than boot time ({boot_datetime()})'
			elif not is_process(pid):
				reason = f' - pid ({pid}) does not exist'

		status = bool(reason)
		logger.debug(f'{self}:is_stale({status}){reason}')
		return status

	def release(self):
		"""Release a mutex."""
		logger.debug(f'{self}:release')
		if self.file_handle:
			self.file_handle.close()
			self.reset()
			delete_file(self.file_name, ignore_errors=True)

	def reset(self):
		"""Reset non-static mutex properties."""
		self.file_handle = None
		self.pid = self.get_pid()

	def __del__(self):
		"""Insure that mutex gets released when its instance is destroyed."""
		logger.debug(f'{self}:destroy')
		self.release()

	def __str__(self):
		debug_id = ''
		if self.debug_id:
			debug_id = f'[{self.debug_id}]'

		return f'{self.__class__.__name__}({self.file_name}{debug_id}, pid={self.pid}, acquired={self.is_acquired()})'


# temp test harness ...


def test_file_mutex():
	logger.debug('Starting mutex test')
	mutex_file_name = 'test.pid'
	mutex = FileMutex(mutex_file_name)
	if not mutex.acquire():
		# acquire failure - exit immediately
		pass
	else:
		# acquire success - keep running so other processes can try to acquire the mutex
		delay = 30
		logger.debug(f'Sleeping for {delay} secs')
		time.sleep(delay)

		# mutex.release()
		del mutex
		logger.debug('Finished mutex test')


# test code
def main():
	test_file_mutex()


# test code
if __name__ == '__main__':
	log_setup()
	log_session_info()
	main()
