#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
vault.py

Emulates a 3rd party vault to testing vault-based secret management.

TODO: Extend to wrap Azure Key Vault or Hashicorp Vault.

"""


# standard lib
import logging
import sys


# common lib
from common import hash_str
from common import load_jsonpickle
from common import save_jsonpickle
from common import log_setup
from common import log_session_info


# module level logger
logger = logging.getLogger(__name__)


class Vault:

	def __init__(self, vault_name, authentication_token=None):
		self.vault_name = vault_name
		self.vault_key_lookup = dict()
		self.vault_placeholder_lookup = dict()

	def __call__(self, placeholder):
		return self._get_secret(placeholder)

	def _get_key_name(self, placeholder):
		return self.vault_placeholder_lookup.get(placeholder, None)

	def _get_placeholder(self, key_name):
		return hash_str(key_name)

	def _get_secret(self, placeholder):
		key_name = self._get_key_name(placeholder)
		if key_name:
			secret = self.vault_key_lookup.get(key_name, None)
		else:
			secret = None
		return secret

	def add(self, key_name, secret):
		if key_name in self.vault_key_lookup:
			logger.info(f'{self.vault_name}[{key_name}] updated')
		else:
			logger.info(f'{self.vault_name}[{key_name}] added')

		placeholder = self._get_placeholder(key_name)
		self.vault_key_lookup[key_name] = secret
		self.vault_placeholder_lookup[placeholder] = key_name
		return placeholder

	def clear(self):
		self.vault_key_lookup.clear()
		self.vault_placeholder_lookup.clear()
		logger.info(f'{self.vault_name} cleared')

	def delete(self, key_name):
		if key_name not in self.vault_key_lookup:
			logger.warning(f'{self.vault_name}[{key_name}] delete failed - key not found')
			return False
		else:
			placeholder = self._get_placeholder(key_name)
			if placeholder not in self.vault_placeholder_lookup:
				logger.warning(f'{self.vault_name}[{key_name}] delete failed - placeholder not found')
			else:
				del self.vault_key_lookup[key_name]
				del self.vault_placeholder_lookup[placeholder]
				logger.info(f'{self.vault_name}[{key_name}] deleted')
			return True

	def dump(self):
		for key_name in sorted(self.vault_key_lookup):
			placeholder = self._get_placeholder(key_name)
			secret = self._get_secret(placeholder)
			logger.info(f'{self.vault_name}[{key_name}] = {secret} [{placeholder}]')

	def load(self, file_name=None):
		file_name = file_name or f'{self.vault_name}.vault'
		restored_vault = load_jsonpickle(file_name)
		self.vault_key_lookup = restored_vault.vault_key_lookup
		self.vault_placeholder_lookup = restored_vault.vault_placeholder_lookup
		logger.info(f'{self.vault_name}: vault loaded ({file_name})')

	def save(self, file_name=None):
		file_name = file_name or f'{self.vault_name}.vault'
		save_jsonpickle(file_name, self)
		logger.info(f'{self.vault_name}: vault saved ({file_name})')


# test code
def main():
	vault = Vault('vault_capture_dev')
	rtp_secret_id = vault.add('database:amc_rtp.password', '<database-password>')

	# double add to detect add vs update
	capture_secret_id = vault.add('objecstore:capture', '<big-sas-token>')
	capture_secret_id = vault.add('objecstore:capture', '<big-sas-token>')
	vault.dump()

	logger.info(f'rpt_secret_id = {rtp_secret_id}')
	logger.info(f'capture_secret_id = {capture_secret_id}')
	logger.info(f'rtp_secret = {vault(rtp_secret_id)}')
	logger.info(f'capture_secret = {vault(capture_secret_id)}')
	logger.info(f'bad_key_secret = {vault("bad_key_secret_id")}')

	vault.save()
	vault.delete('database:amc_rtp.password')
	vault.delete('bad_key_name')

	vault.dump()
	vault.clear()
	vault.dump()
	vault.load()
	vault.dump()


# test code
if __name__ == '__main__':
	log_setup()
	log_session_info()
	main()

