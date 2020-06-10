#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
Listener.py

Listen on specified endpoint.
"""


# module name here ...
import logging
import time


# common
from common import delete_file
from common import is_file
from common import load_text
from common import log_setup
from common import log_session_info
from common import split


# udp classes


# module level logger
logger = logging.getLogger(__name__)


class Listener:

	def __init__(self, endpoint=None):
		self.endpoint = endpoint

	def _listen(self):
		"""Subclass based on endpoint."""
		pass

	def clear(self):
		"""Subclass based on endpoint."""
		pass

	def listen(self):
		return self._listen().strip().lower()


class ListenerFile(Listener):

	def _listen(self):
		if not self.endpoint:
			return ''
		else:
			if is_file(self.endpoint):
				return load_text(self.endpoint, '')
			else:
				return ''

	def clear(self):
		if self.endpoint:
			delete_file(self.endpoint, ignore_errors=True)


# TODO: Endpoint should be config section name for endpoint
# TODO: How to specify container/key for endpoint ???
class ListenerAzureBlob(Listener):
	pass


# temp test harness ...


# test code
def main():
	listener = ListenerFile('listener.txt')
	listener.clear()
	while True:
		time.sleep(1)
		command = listener.listen()
		if command in split('bye end exit quit stop'):
			logger.info(f'Exit command: {command}')
			listener.clear()
			break


# test code
if __name__ == '__main__':
	log_setup()
	log_session_info()
	main()
