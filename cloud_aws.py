#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
cloud_aws.py

AWS abstracted cloud services.
"""

# standard lib
import json
import logging
import time

# 3rd party lib
import boto3
from botocore.exceptions import ClientError

# common lib
from common import decode_uri
from common import log_setup
from common import log_session_info
from common import make_fdqn
from common import now
from common import save_text

# module level logger
logger = logging.getLogger(__name__)


class Connect:
	"""
	Abstracted cloud connection class.

	Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/_connect.html
	"""

	def __init__(self, resource_name, connection):
		# generic attributes
		self.client = None
		self.resource_type = ''
		self.resource_name = make_fdqn(resource_name)
		self.connection = connection

		# resource specific attributes
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
		logger.info(f'aws._connect.{self.resource_type}')
		try:
			self.client = boto3.client(
				self.resource_type,
				aws_access_key_id=self.connection.public_key,
				aws_secret_access_key=self.connection.private_key,
				region_name=self.connection.region
			)

		# exception handling
		except ClientError as e:
			logger.error(e)
		except Exception as e:
			logger.exception(f'client _connect failed for resource ({self.resource_type}): {e}')
			raise


class Objectstore(Connect):
	"""
	Abstracted objectstore class.

	Ref: https://boto3.readthedocs.io/en/latest/reference/services/s3.html
	"""

	def _describe(self, method_name, file_name=None, object_key=None):
		""""Describe the method being invoked. For diagnostic purposes."""
		object_method = f'{self.objectstore_name}.{method_name}'
		if file_name:
			return f'{object_method}(file_name={file_name}, object_key={object_key})'
		else:
			return f'{object_method}(object_key={object_key})'

	def _pre_connect(self):
		self.resource_type = 's3'
		self.objectstore_name = self.resource_name

	def delete(self, object_key):
		"""Delete file associated with object_key with logging and exception handling."""
		logger.info(self._describe('delete', object_key=object_key))
		try:
			# parameters(Bucket=, Key=)
			self.client.delete_object(Bucket=self.objectstore_name, Key=object_key)
			return True

		# exception handling
		except ClientError as e:
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
			# parameters(Bucket=, Key=, Filename=)
			self.client.download_file(self.objectstore_name, object_key, file_name)
			return True

		# exception handling
		except ClientError as e:
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
			self.client.upload_file(file_name, self.objectstore_name, object_key)
			return True

		# exception handling
		except ClientError as e:
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
			response = self.client.list_queues()
			return response['QueueUrls']
		except ClientError as e:
			logger.error(e)
			return None
		except Exception as e:
			logger.exception(f'client.list_queues() failed: {e}')
			raise

	def _pre_connect(self):
		self.resource_type = 'sqs'
		self.queue_name = self.resource_name

	def _post_connect(self):
		# lookup queue url

		# translate queue name to queue url with logging and exception handling
		logger.info(self._describe(f'get_queue_url({self.queue_name})'))
		try:
			response = self.client.get_queue_url(QueueName=self.queue_name)
			self.queue_url = response['QueueUrl']
		except ClientError as e:
			logger.error(e)
			self.queue_url = None
		except Exception as e:
			logger.exception(f'client.get_queue_url({self.queue_name}) failed: {e}')
			raise

	def delete(self, message_id):
		"""Delete message_id from queue with logging and exception handling."""

		# make sure we have a valid queue assignment before executing delete
		if not self.queue_url:
			logger.warning(f'{self._describe("delete", message_id[0:16])}: no queue url assigned; delete() ignored')
			return False

		# ignore blank message ids
		if not message_id:
			return True

		# delete message_id with logging and exception handling
		logger.info(self._describe('delete', message_id[0:16]))
		try:
			self.client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=message_id)
			return True

		# exception handling
		except ClientError as e:
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
		if not self.queue_url:
			logger.warning(f'{self._describe("get")}: no queue url assigned; get() ignored')
			return None

		logger.info(self._describe('get'))
		try:
			response = self.client.receive_message(
				QueueUrl=self.queue_url,
				MaxNumberOfMessages=1,
				MessageAttributeNames=['All'],
				VisibilityTimeout=0,
				WaitTimeSeconds=0
			)

		# exception handling
		except ClientError as e:
			logger.error(e)
			response = None
		except Exception as e:
			logger.exception(f'client.receive_message() failed: {e}')
			raise

		# make sure test messages are filtered out
		if 's3:TestEvent' in str(response):
			logger.info('Deleting AWS S3:SQS linkage test message')
			message_id = response['Messages'][0]['ReceiptHandle']
			self.delete(message_id)
			return None

		# track seen messages and ignore duplicate messages
		if response and 'Messages' in response:
			message_id = response['Messages'][0]['ReceiptHandle']
			if message_id in self.queue_seen_messages:
				response = None
			else:
				self.queue_seen_messages.add(message_id)

		return response

	def put(self, message):
		"""Put message to queue with logging and exception handling."""

		# make sure we have a valid queue assignment before executing put
		if not self.queue_url:
			logger.warning(f'{self._describe("put", message)}: no queue url assigned; put() ignored')
			return False

		logger.info(self._describe('put', message))
		try:
			response = self.client.send_message(QueueUrl=self.queue_url, MessageBody=message, DelaySeconds=0)
			return response

		# exception handling
		except ClientError as e:
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
		self.message_id = ''
		self.message = ''
		self.timestamp = ''
		self.ip_address = ''
		self.objectstore_name = ''
		self.object_key = ''
		self.object_size = 0

		# get first message
		if not response or 'Messages' not in response:
			pass

		# ignore s3:TestEvent's posted in target queue when S3-to-queue notifications activated
		elif 'Event' in response and response['Event'] == 's3:TestEvent':
			pass

		# legitimate message; process it
		else:
			message = response['Messages'][0]['Body']
			self.message_id = response['Messages'][0]['ReceiptHandle']

			# decodes UTF8 encoded bytes escaped with URI quoting to UTF8
			message = decode_uri(message)

			# convert message's first record to a notification dict
			if message:
				if not message.startswith(('"', '{', '[')):
					self.message = message
				else:
					body = json.loads(message)
					if 'Records' in body:
						notification = body['Records'][0]
						self.timestamp = notification['eventTime']
						self.ip_address = notification['requestParameters']['sourceIPAddress']
						self.objectstore_name = notification['s3']['bucket']['name']
						self.object_key = notification['s3']['object']['key']
						self.object_size = notification['s3']['object']['size']

			# filter out S3 inventory file messages.
			if 'csv.gz' in self.object_key or 'inventory' in self.object_key:
				self.message_id = ''
				self.message = ''
				self.timestamp = ''
				self.ip_address = ''
				self.objectstore_name = ''
				self.object_key = ''
				self.object_size = 0

	def __str__(self):
		if self.message:
			return f'[id:{self.message_id[0:16]}] {self.message}'
		else:
			return f'[id:{self.message_id[0:16]}] {self.objectstore_name}: {self.object_key} (size={self.object_size}, via {self.ip_address} at {self.timestamp})'


# test code
def main():
	from config import ConfigSectionKey

	# test data
	config = ConfigSectionKey('conf', 'local')
	config.load('bootstrap.ini', 'bootstrap')
	config.load('init.ini')
	config.load('connect.ini')

	# for testing purposes:
	# - test with both cloud connection values (*capture and *archive)
	# - these connections have different permissions and will yield different results

	# cloud_connection_name = 'cloud:amc_aws_capture_01_etl'
	cloud_connection_name = 'cloud:udp_aws_archive_01_etl'
	cloud = config(cloud_connection_name)
	capture_objectstore_name = cloud.capture_objectstore
	capture_queue_name = cloud.capture_queue
	cloud.dump()

	# create test files (must have *.zip extension for S3:SQS notification)
	test_folder = 'test_folder_1'
	test_file_1 = f'{test_folder}/test1.zip'
	test_file_2 = f'{test_folder}/test2.zip'
	save_text(f'Test @{now()}', test_file_1)

	# object store put, get, delete
	objectstore = Objectstore(capture_objectstore_name, cloud)
	objectstore.put(test_file_1, 'test/test1.zip')
	objectstore.get(test_file_2, 'test/test1.zip')
	objectstore.delete('test/test1.zip')

	# sleep for 3 seconds to give notification message time to post to queue
	time.sleep(3)

	# queue get, remove
	queue = Queue(capture_queue_name, cloud)
	queue.put('Test message 1')
	time.sleep(2)
	queue.put('Test message 2')
	time.sleep(2)

	while True:
		time.sleep(1)
		response = queue.get()
		notification = ObjectstoreNotification(response)
		queue.delete(notification.message_id)
		if notification.message_id:
			logger.info(f'Test mode: notification message = {notification}')
		else:
			break

	# debugging info
	logger.info(f'Available queues: {queue._list_queue_names()}')
	queue._dump()


# test code
if __name__ == '__main__':
	log_setup(log_level=logging.INFO)
	log_session_info()
	main()
