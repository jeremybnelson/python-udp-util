#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
keyvault.py

Tip: Use an application global Secret() without a key vault to look for the first key vault secret
- an environment variable SECRET_<secret_name>
- as a command line option --secret:

# requirements.txt
azure-keyvault
azure-mgmt-authorization
azure-mgmt-keyvault
azure-mgmt-resource
azure-mgmt-storage=
azure-storage-blob

"""


# standard lib
import json
import logging
import os
import sys
import zipfile


# standard lib
from abc import ABC
from abc import abstractmethod


# common lib
from common import delete_file
from common import debug_log_method_return_value
from common import is_file
from common import key_value
from common import left_trim
from common import log_session_info
from common import log_setup
from common import make_name
from common import now
from common import save_text


# resource lib
from resource import ensure_connected
from resource import ResourceException


# 3rd party libs
import jsonpickle


# module level logger
logger = logging.getLogger(__name__)


# exceptions
class KeyVaultException(ResourceException):
	pass


class KeyVaultABC(ABC):

	@abstractmethod
	def __enter__(self):
		pass

	@abstractmethod
	def __exit__(self, exc_type, exc_val, exc_tb):
		pass

	@abstractmethod
	def _authenticate(self):
		pass

	@abstractmethod
	def _context(self, message):
		pass

	@abstractmethod
	def _load_file(self):
		pass

	@abstractmethod
	def _save_file(self):
		pass

	@abstractmethod
	def _resource_name(self, name):
		pass

	@classmethod
	@abstractmethod
	def item_name(cls, item_name):
		pass

	@abstractmethod
	def create(self, name, password):
		pass

	@abstractmethod
	def remove(self, name):
		pass

	@abstractmethod
	def connect(self, name, password):
		pass

	@abstractmethod
	def disconnect(self):
		pass

	@abstractmethod
	def get(self, secret_name):
		pass

	@abstractmethod
	def set(self, secret_name, secret_value):
		pass

	@abstractmethod
	def list(self):
		pass

	@abstractmethod
	def delete(self, secret_name):
		pass

	@abstractmethod
	def clear(self):
		pass


class KeyVault(KeyVaultABC):

	def __init__(self, name='', password=''):
		# status
		self.is_connected = False
		self.is_updated = False

		# name and password
		self.name = name
		self.password = password

		# implementation specific attributes
		self.exception = KeyVaultException
		self.password_key = '__password__'
		self.vault_file_ext = 'vault'
		self.vault_json_file = 'vault.json'
		self.secrets = dict()

		# auto-connect if name and password provided
		if name and password:
			self.connect(name, password)

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		if not exc_type:
			self.disconnect()
		else:
			# log diagnostics on unexpected exception
			exception_description = f'{exc_type}({exc_val})'
			exception_message = f'Exception raised on vault __exit__ ({self.name})'
			logger.warning(self._context(f'{exception_message}: {exception_description}'))

		# return True to indicate we handled exception on context manager exit
		return True

	def _authenticate(self):
		vault_password = self.secrets.get(self.password_key, '')
		return vault_password == self.password

	def _context(self, message):
		return f'{self.__class__.__name__}({self.name}): {message}'

	def _file_name(self, name=''):
		if name:
			return f'{name}.{self.vault_file_ext}'
		else:
			return f'{self.name}.{self.vault_file_ext}'

	# 1-127 chars, containing only 0-9, a-z, A-Z, and -
	# Note: We force to lower case for consistency.
	@classmethod
	def item_name(cls, item_name):
		return make_name(item_name, '-').lower()

	# globally unique, 3-24 chars, containing only 0-9, a-z, A-Z, and -
	# Note: We force to lower case for consistency.
	def _resource_name(self, name):
		return make_name(name, '-').lower()

	def _load_file(self):
		# alternative _load_file(): self.secrets = load_jsonpickle(self.file_name)

		# reset is_updated status
		self.is_updated = False

		# extract json file from *.vault zip file and unpickle to secrets dict
		try:
			zip_file = zipfile.ZipFile(self._file_name(), mode='r')
			json_data = zip_file.read(self.vault_json_file).decode()
			self.secrets = jsonpickle.loads(json_data)
			is_loaded = True

		# detect corrupted zip and json files
		except (zipfile.BadZipFile, json.decoder.JSONDecodeError):
			is_loaded = False

		return is_loaded

	def _save_file(self):
		# alternative _save_file(): save_jsonpickle(self.file_name, self.secrets)

		# only save vault if it was updated
		if self.is_updated:
			# add key vault password to saved key vault file
			self.secrets[self.password_key] = self.password

			# pickle the vault dict as a json string and save it to a json file in a zip file
			json_data = jsonpickle.dumps(self.secrets)
			zip_file = zipfile.ZipFile(self._file_name(), mode='w', compression=zipfile.ZIP_DEFLATED)
			zip_file.writestr(self.vault_json_file, json_data.encode())

			# clear is_updated status
			self.is_updated = False

	def create(self, name, password):
		# if already connected, then disconnect first
		if self.is_connected:
			self.disconnect()

		if is_file(self._file_name(name)):
			# update status
			logger.warning(self._context('Vault already exists'))
		else:
			# update status
			self.is_updated = True
			self.is_connected = True

			# create new key vault
			self.name = name
			self.password = password
			self.secrets = dict()
			self._save_file()
			logger.debug(self._context('Creating vault'))

	def remove(self, name):
		logger.debug(self._context(f'Removing vault'))
		if not is_file(self._file_name(name)):
			logger.warning(self._context(f'Vault does not exist'))
		else:
			delete_file(self._file_name(name), ignore_errors=True)
			if is_file(self._file_name(name)):
				logger.warning('Unable to remove vault')

	def connect(self, name, password):
		# assume failure
		self.name = name
		self.is_connected = False
		self.is_updated = False

		if not is_file(self._file_name(name)):
			warning_message = 'Vault does not exist'
			logger.warning(self._context(warning_message))
			raise self.exception(warning_message)
		else:
			logger.debug(self._context('Opening vault'))
			if not self._load_file():
				warning_message = self._context('Vault corrupt')
				logger.warning(warning_message)
				raise self.exception(warning_message)

			# key vault authentication
			if not self._authenticate():
				warning_message = self._context('Vault password authentication failed')
				logger.warning(warning_message)
				raise self.exception(warning_message)
			else:
				# we successfully connected to vault
				self.is_connected = True

				# remove password from secrets; we add back when we disconnect
				del self.secrets[self.password_key]

	@ensure_connected
	def disconnect(self, is_exception=False):
		if is_exception:
			logger.warning(self._context(f'disconnect() skipped due to exception'))
		else:
			# logger.debug(self._context('Saving vault'))
			self._save_file()

		# clear name and status
		self.name = ''
		self.is_connected = False
		self.is_updated = False

	@ensure_connected
	@debug_log_method_return_value
	def get(self, secret_name):
		secret_name = self.item_name(secret_name)
		logger.debug(self._context(f'Retrieving secret ({secret_name})'))
		return self.secrets.get(secret_name, '')

	@ensure_connected
	def set(self, secret_name, secret_value):
		# remember that we updated vault
		self.is_updated = True

		secret_name = self.item_name(secret_name)
		secret_value = str(secret_value).strip()
		logger.debug(self._context(f'Saving secret ({secret_name})'))
		self.secrets[secret_name] = secret_value

	@ensure_connected
	def delete(self, secret_name):
		# remember that we updated vault
		self.is_updated = True

		secret_name = self.item_name(secret_name)
		if secret_name not in self.secrets:
			logger.warning(self._context(f'Request to delete non-existent secret ({secret_name})'))
		else:
			logger.debug(self._context(f'Deleting secret ({secret_name})'))
			del self.secrets[secret_name]

	@ensure_connected
	@debug_log_method_return_value
	def list(self):
		logger.debug(self._context('Retrieving list of secret names'))
		return sorted(self.secrets.keys())

	@ensure_connected
	def clear(self):
		logger.debug(self._context(f'Clearing secrets'))

		# remember that we updated vault
		self.is_updated = True
		self.secrets.clear()


class Secret:

	secret_prefix = 'secret'

	"""
	Note: The following description based on secret_prefix='secret'.
	
	Retrieves secrets specified as 'secret:{secret_name}' vs literal secret values.

	secret = Secret([key_vault_name, key_vault_password])
	secret_value = secret(secret_name)

	If secret_value lacks a 'secret:' prefix:
	Then its a literal secret value so return it as-is.

	If secret_value has 'secret:{secret_name}' format:
	Extract {secret_name} and search for its corresponding secret value in the following:
	- a command line option: --secret:{secret_name}={secret_value}
	- an environment variable: SECRET_{SECRET_NAME}={secret_value}
	- within optional key_vault_name provided to Secret()
	Then return the first matched {secret_value} or '' if secret_name not found.
	"""

	def __init__(self, key_vault_name='', key_vault_password=''):
		self.key_vault_name = key_vault_name
		self.key_vault_password = key_vault_password

	def _context(self, message):
		if self.key_vault_name:
			key_vault_name = f'{self.key_vault_name}'
		else:
			key_vault_name = ''
		return f'{self.__class__.__name__}({key_vault_name}): {message}'

	def _secret_via_env(self, secret_name):
		secret_name = KeyVault.item_name(secret_name)

		# look for environment variable {SECRET_PREFIX}_{SECRET_NAME}
		return os.environ.get(f'{self.secret_prefix.upper()}_{secret_name.upper()}', '')

	def _secret_via_cli(self, secret_name):
		secret_name = KeyVault.item_name(secret_name)

		# look for --{secret_prefix}:{secret_name}={secret_value}
		for option in sys.argv[1:]:
			if option.startswith(f'--{self.secret_prefix}:'):
				# remove the leading --{secret_prefix}:
				option_secret = left_trim(option, f'--{self.secret_prefix}:')

				# split into secret name and secret value
				option_secret_name, option_secret_value = key_value(option_secret)

				# normalize secret name
				option_secret_name = KeyVault.item_name(option_secret_name)

				# look for a secret name match
				if secret_name == option_secret_name:
					secret_value = option_secret_value
					break
		else:
			secret_value = ''

		return secret_value

	def _secret_via_kv(self, secret_name):
		secret_name = KeyVault.item_name(secret_name)

		if not self.key_vault_name:
			secret_value = ''
		else:
			with KeyVault(self.key_vault_name, self.key_vault_password) as key_vault:
				secret_value = key_vault.get(secret_name)
		return secret_value

	@debug_log_method_return_value
	def __call__(self, secret_value):
		if not secret_value.lower().startswith(f'{self.secret_prefix}:'):
			return secret_value
		else:
			secret_name = left_trim(secret_value, f'{self.secret_prefix}:', case_sensitive=False)
			secret_name = KeyVault.item_name(secret_name)

			# priority of secret lookup: check command line > environment variable > key vault

			# command line
			actual_secret_value = self._secret_via_cli(secret_name)
			if actual_secret_value:
				logger.debug(self._context(f'secret({secret_name}) retrieved from cli'))
				return actual_secret_value

			# environment variable
			actual_secret_value = self._secret_via_env(secret_name)
			if actual_secret_value:
				logger.debug(self._context(f'secret({secret_name}) retrieved from env'))
				return actual_secret_value

			# key vault
			actual_secret_value = self._secret_via_kv(secret_name)
			if actual_secret_value:
				logger.debug(self._context(f'secret({secret_name}) retrieved from kv'))

			return actual_secret_value


# test code
def test():
	# secret prefix used to indicate secret lookup
	secret_prefix = 'secret'

	# non-existent key vault
	try:
		key_vault = KeyVault()
		key_vault.connect('missing', '$password$')
	except KeyVaultException as e:
		logger.debug(f'Expected exception: {e}')

	# corrupt key vault
	save_text('corrupt.vault', '~!@#$%^&*')
	key_vault = KeyVault()
	try:
		key_vault.connect('corrupt', '$password$')
	except KeyVaultException as e:
		logger.debug(f'Expected exception: {e}')

	# remove the corrupt vault we just created
	key_vault.remove('corrupt')

	# valid key vault testing

	# key vault name and password
	key_vault_name = 'test'
	os.environ[f'{secret_prefix.upper()}_UDP_KV'] = '$key-vault-password$'
	logger.debug(f'os.environ(SECRET_UDP_KV) = {os.environ.get("SECRET_UDP_KV", "")}')
	key_vault_password = Secret()(f'{secret_prefix}:UDP_KV')
	logger.debug(f'key_vault_password = {key_vault_password}')

	# create a key vault
	key_vault = KeyVault()
	key_vault.create(key_vault_name, key_vault_password)
	key_vault.remove(key_vault_name)
	key_vault.create(key_vault_name, key_vault_password)

	# key vault testing
	key_vault = KeyVault(key_vault_name, key_vault_password)
	key_vault.set('date_password', now())
	key_vault.set('user-name', 'Malcolm')
	key_vault.disconnect()

	# test authentication failure (bad password)
	try:
		bad_key_vault = KeyVault(key_vault_name, 'bad-password')
		bad_key_vault.disconnect()
	except KeyVaultException as e:
		logger.debug(f'Expected exception: {e}')

	# test using a disconnected key vault
	try:
		key_vault.list()
	except KeyVaultException as e:
		logger.debug(f'Expected exception: {e}')

	with KeyVault(key_vault_name, key_vault_password) as key_vault:
		# key_vault.clear()
		key_vault.set('AMP_database_password', '$amp-password$')
		key_vault.set('RTP_DATABASE_PASSWORD', '$rtp-password$')
		key_vault.set('bye_database_password', 'bad-password$')
		key_vault.list()
		key_vault.delete('bye_database_password')
		key_vault.delete('bad_secret_name')
		key_vault.get('amp-database-PASSWORD')
		key_vault.get('$rtp-database-password$')
		key_vault.list()

	# test JIT secret expansion
	secret = Secret(key_vault_name, key_vault_password)
	secret('$amp_password$')
	secret(f'{secret_prefix}:amp-DATABASE-password$$$')
	secret(f'{secret_prefix}:@rtp-DATABASE_password')
	secret(f'{secret_prefix}:@USER-NAME:')
	secret(f'{secret_prefix}:bad_secret_name')


# test code
if __name__ == '__main__':
	log_setup(log_level=logging.DEBUG)
	log_session_info()
	test()
