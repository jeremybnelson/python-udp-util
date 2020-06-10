#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
cloud_az.py

Azure abstracted cloud services.
"""

# standard lib
import json
import logging
import time
import filecmp
import base64
import re


# Azure party lib
from azure.storage.blob import BlockBlobService
from azure.storage.queue import QueueService, QueueMessageFormat
from azure.common import AzureException

# common lib
from common import log_setup
from common import log_session_info
from common import make_fdqn


# module level logger
logger = logging.getLogger(__name__)


class Connect:
	"""
	Abstracted Azure cloud connection class.

	"""

	def __init__(self, resource_name, connection):
		# generic attributes
		self.client = None
		self.resource_type = ''
		self.resource_name = make_fdqn(resource_name)
		self.connection = connection

		# resource specific attributes
		self.account_name = ''
		self.objectstore_name = ''
		self.queue_name = ''
		self.queue_url = ''
		self.queue_seen_messages = set()

		# setup and _connect
		self._pre_connect()
		self._connect()
		self._post_connect()

	def _pre_connect(self):
		pass

	def _post_connect(self):
		pass

	# TODO: Add logic to assume a role if non-empty role in connection object.
	# Untested: Setting the cloud public/private key to '' or None should work w/out code changes.
	def _connect(self):
		"""Connect to a specific resource type with logging and exception handling."""

		self.client = None
		logger.info(f'azure._connect.{self.resource_type}')
		try:
			# self.client = self.resource_type(account_name=self.resource_name, account_key=self.connection.storage_key)
			# noinspection PyCallingNonCallable
			# ToDo: Logic is needed here to enforce encryption
			self.client = self.resource_type(account_name=self.resource_name, sas_token=self.connection.sas_token)
			# self.client.require_encryption = self.resource_type.require_encryption
			# self.client.re

		# exception handling
		except AzureException as e:
			logger.error(e)
		except Exception as e:
			logger.exception(f'client _connect failed for resource ({self.resource_type}): {e}')
			raise


class Objectstore(Connect):
	"""
	Abstracted Azure objectstore class.

	"""

	def _describe(self, method_name, file_name=None, object_key=None):
		""""Describe the method being invoked. For diagnostic purposes."""
		object_method = f'{self.objectstore_name}.{method_name}'
		if file_name:
			return f'{object_method}(file_name={file_name}, object_key={object_key})'
		else:
			return f'{object_method}(object_key={object_key})'

	def _pre_connect(self):
		self.resource_type = BlockBlobService
		# self.resource_type.require_encryption = True
		self.account_name = self.resource_name
		self.objectstore_name = self.connection.capture_objectstore

	def delete(self, object_key):
		"""Delete file associated with object_key with logging and exception handling."""
		logger.info(self._describe('delete', object_key=object_key))
		try:
			# parameters(Bucket=, Key=)
			self.client.delete_blob(self.objectstore_name, object_key)
			return True

		# exception handling
		except AzureException as e:
			logger.error(e)
			return False
		except Exception as e:
			logger.exception(f'client.delete_object() failed: {e}')
			raise

	def get(self, file_name, object_key):
		"""Get file associated with object_key with logging and exception handling."""
		logger.info(self._describe('get', file_name, object_key))
		try:
			# downloads a file using a multi-threaded, multi-part downloader
			# parameters(container_name=, object_key=, full_file_path=)
			self.client.get_blob_to_path(self.objectstore_name, object_key, file_name)
			return True

		# exception handling
		except AzureException as e:
			logger.error(e)
			return False
		except Exception as e:
			logger.exception(f'client.download_file() failed: {e}')
			raise

	def put(self, file_name, object_key):
		"""Put file to object_key with logging and exception handling."""
		logger.info(self._describe('put', file_name, object_key))
		try:
			# uploads file using a multi-threaded, multi-part uploader
			# parameters(Filename=, Bucket=, Key=)
			# self.client.upload_file(file_name, self.objectstore_name, object_key)
			self.client.create_blob_from_path(self.objectstore_name, object_key, file_name)
			return True

		# exception handling
		except AzureException as e:
			logger.error(e)
			return False
		except Exception as e:
			logger.exception(f'client.upload_file() failed: {e}')
			raise


class Queue(Connect):
	"""
	Abstracted queue class.

	Ref: https://boto3.readthedocs.io/en/latest/reference/services/sqs.html
	"""

	def _describe(self, method_name, arg=None):
		""""Describe the method being invoked. For diagnostic purposes."""
		object_method = f'{self.queue_name}.{method_name}'
		if arg:
			return f'{object_method}({arg})'
		else:
			return f'{object_method}()'

	def _dump(self):
		logger.debug(f'aws.sqs({self.queue_name}) message count = {len(self.queue_seen_messages)}')
		for message_id in self.queue_seen_messages:
			logger.info(f'aws.sqs.seen_message = {message_id[0:16]}')

	def _list_queue_names(self):
		"""List available queue names with logging and exception handling."""
		logger.info(self._describe('list_queue_names'))
		try:
			queues = self.client.list_queues()
			return [queue.name for queue in queues]

		except AzureException as e:
			logger.error(e)
			return None
		except Exception as e:
			logger.exception(f'client.list_queues() failed: {e}')
			raise

	def _pre_connect(self):
		self.resource_type = QueueService
		# switch to base64 encoding instead of XML encoding
		# Note: This is not working as expected
		# self.resource_type.encode_function = QueueMessageFormat.text_base64encode
		# self.resource_type.decode_function = QueueMessageFormat.text_base64decode
		self.queue_name = self.connection.capture_queue

	def delete(self, notification):
		"""Delete message_id from queue with logging and exception handling."""

		# make sure we have a valid queue assignment before executing delete
		# if not self.queue_url:
		# logger.warning(f'{self._describe("delete", message_id[0:16])}: no queue url assigned; delete() ignored')
		# return False

		# ignore blank message ids
		# if not message_id:
		# return True

		message_id = notification.message_id
		pop_receipt = notification.pop_receipt

		# delete message_id with logging and exception handling
		logger.info(self._describe('delete', message_id))
		try:
			self.client.delete_message(queue_name=self.queue_name, message_id=message_id, pop_receipt=pop_receipt, timeout=None)
			return True

		# exception handling
		except AzureException as e:
			logger.error(e)
			return False
		except Exception as e:
			logger.exception(f'client.delete_message() failed: {e}')
			raise

	'''
	Background on SQS messages:

	The receipt handle is associated with a specific instance of receiving the message. 

	If you receive a message more than once, the receipt handle you get each time you receive the message is 
	different.

	If you don't provide the most recently received receipt handle for the message when you use the DeleteMessage 
	action, the request succeeds, but the message might not be deleted.

	For standard queues, it is possible to receive a message even after you delete it. This might happen on rare 
	occasions if one of the servers storing a copy of the message is unavailable when you send the request to delete 
	the message. The copy remains on the server and might be returned to you on a subsequent receive request. 

	You should ensure that your application is idempotent, so that receiving a message more than once does not  
	cause issues.

	We comply with the above requirements by tracking seen message ids via .queue_seen_messages (a set) and 
	ignoring duplicate messages.
	'''

	def get(self):
		"""Get message from queue with logging and exception handling."""

		# make sure we have a valid queue assignment before executing get
		if not self.queue_name:
			logger.warning(f'{self._describe("get")}: no queue url assigned; get() ignored')
			return None

		logger.info(self._describe('get'))
		try:
			response = self.client.get_messages(
				queue_name=self.queue_name,
				num_messages=1,
				visibility_timeout=None
			)

		# exception handling
		except AzureException as e:
			logger.error(e)
			response = None
		except Exception as e:
			logger.exception(f'client.receive_message() failed: {e}')
			raise

		# Check if the response from Azure is empty. If so, return None
		if not response:
			logger.debug(f'The {self.queue_name} is empty')
			response = None
		# If the response is not empty, set the response equal to the QueueMessage object in the list
		else:
			response = response[0]
			# str_response = str(response.content)
			# decoded_response = base64.b64decode(str_response)

		return response

	def put(self, message):
		"""Put message to queue with logging and exception handling."""

		# make sure we have a valid queue assignment before executing put
		if not self.queue_name:
			logger.warning(f'{self._describe("put", message)}: no queue url assigned; put() ignored')
			return False

		# Validate that the message is in json format
		try:
			json.loads(message)
		# find the specific json exception and replace this
		except ValueError as e:
			logger.error(f' Message Input is not valid {message} Error: {e}')
			return False

		# Todo: Add logic to encode message is it passes the json validation step

		logger.info(self._describe('put', message))
		try:
			# Note: This is needed to enforce same behavior as automatic messages
			self.client.encode_function = QueueMessageFormat.text_base64encode
			response = self.client.put_message(
				queue_name=self.queue_name,
				content=message,
				visibility_timeout=None,
				time_to_live=None,
				timeout=None
			)
			return response

		# exception handling
		except AzureException as e:
			logger.error(e)
			return False
		except Exception as e:
			logger.exception(f'client.send_message() failed: {e}')
			raise


class ObjectstoreNotification:
	"""
	S3 notification message structure
	Ref: https://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html
	"""

	def __init__(self, response):
		self.message_id = ''  # response[0].id
		self.message = ''
		self.timestamp = ''
		self.ip_address = ''
		self.objectstore_name = ''
		self.object_key = ''
		self.object_size = 0
		self.event_type = ''
		self.pop_receipt = ''  # response[0].pop_receipt
		self.dequeue_count = 0

		# check if we have a response
		if response:
			message = response.content

			# decodes the content section of the azure.storage.queue.models.QueueMessage object
			decoded_message = base64.b64decode(message).decode('utf-8')
			try:
				body = json.loads(decoded_message)
			# find the specific json exception and replace this
			except ValueError as e:
				logger.warning(f' Unexpected Message: {message} Error: {e}')
				self.message_id = response.id
				self.pop_receipt = response.pop_receipt
				self.message = decoded_message
				return

			if decoded_message:
				# body = json.loads(message)
				self.message_id = response.id
				self.pop_receipt = response.pop_receipt
				self.message = decoded_message
				self.timestamp = body['eventTime']
				# Clean this up. Probably can just use split instead of re
				self.objectstore_name = re.split(' - |/', body['subject'])[4]
				self.object_key = body['subject'].replace(
					f'/blobServices/default/containers/{self.objectstore_name}/blobs/', '')
				self.timestamp = body['eventTime']
				self.event_type = body['eventType']
				self.client_id = body['data']['clientRequestId']
				self.request_id = body['data']['requestId']
				self.url = body['data']['url']
			# add storage account name property to this class
		else:
			pass

	def __str__(self):
		if self.decoded_message:
			return f'[id:{self.message_id[0:16]}] {self.message}'
		else:
			return f'[id:{self.message_id[0:16]}] {self.objectstore_name}: {self.object_key} (size={self.object_size}, via {self.ip_address} at {self.timestamp})'


def main():
	from config import ConfigSectionKey, ConfigSection

	# load cloud specific values
	config = ConfigSectionKey('conf', 'local')
	# config.load('connect.ini')
	config.load_file('../conf/azure.ini')
	cloud = config('cloud')

	# Load cloud test specific values
	cloud_config = ConfigSection('conf', 'local')
	cloud_config.load_file('../conf/cloud.cfg')
	# test_queue_message = cloud_config('test_queue_message_1')

	"""objectstore logic"""
	# objectstore = Objectstore(cloud.account_id, cloud)
	# objectstore.put('C:/test/test.txt', 'test.txt')
	# objectstore.get('../tmp/readwrite_downloaded.txt', 'readwrite.txt')
	# objectstore.get('C:/udp_utils-master/dev/tests/working', 'test.txt')
	# objectstore.delete('test.txt')
	# file_check = filecmp.cmp(f1='C:/test/test.txt', f2='C:/test/get/test.txt', shallow=False)
	# print(file_check)

	"""Queue logic"""
	queue = Queue(cloud.account_id, cloud)
	# queue_names = queue._list_queue_names()
	# print(queue_names)

	# IMPORTANT: The Queue delete method parameter should now be an
	# ObjectstoreNotification object instead of just the message_id of the object.
	#queue.delete(notification)
	# queue.encode_function = QueueMessageFormat.text_base64encode
	queue_message = '{"Message":"Hello World"}'
	# encoded_message = str(base64.b64encode(queue_message.encode('utf-8')))

	queue_message = {
	"topic": "test_queue_message",
	"subject": "/This/Is/A/Test/Message/TestMessage",
	"eventType": "test_queue_message",
	"eventTime": "",
	"id": "",
	"data":{
		"api": "",
		"clientRequestId": "",
		"requestId": "",
		"eTag": "",
		"contentType": "",
		"contentLength": 0,
		"blobType": "",
		"url": "",
		"sequencer": "",
		"storageDiagnostics": {
			"batchId": ""
		}
	},
		"dataVersion": "",
		"metadataVersion": "1"
	}

	json_queue_message = json.dumps(queue_message)


	# response = queue.get()
	# notification = ObjectstoreNotification(response)
	# queue.put(json_queue_message)
	# queue.put('Hello World')
	response = queue.get()
	notification = ObjectstoreNotification(response)
	queue.delete(notification)
	"""
	while True:
		time.sleep(1)
		response = queue.get()
		if response:
			notification = ObjectstoreNotification(response)
			queue.delete(notification)
			logger.info(f'Test mode: notification message = {notification}')
		else:
			break
	"""


if __name__ == '__main__':
	log_setup(log_level=logging.INFO)
	log_session_info()
	main()
