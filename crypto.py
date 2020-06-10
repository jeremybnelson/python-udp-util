#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
crypto.py

Encrypt/decrypt text/byte strings/arrays and files.

OPEN: Convert base64 (a stub) to true crypto library.
OPEN: Perform all file tests in test_folder vs source root folder

OPEN: sign file (hash > HMAC > signature), verify signature

###

FUTURE: Verifying the provenance of assets and secrets.
Simple > Best practice: checksum/hash > HMAC (requires shared secret) > GPG public/private keys

python-gnupg - A Python wrapper for GnuPG (keyring)
Based on GPG.py, written by Andrew Kuchling.
https://pythonhosted.org/python-gnupg/

Note: This technique replaces cryptography module which lacks signatures, key management, key rings, etc.

The gnupg module allows Python programs to make use of the functionality provided by the GNU Privacy Guard
(abbreviated GPG or GnuPG). Using this module, Python programs can encrypt and decrypt data, digitally sign
documents and verify digital signatures, manage (generate, list and delete) encryption keys, using proven
Public Key Infrastructure (PKI) encryption technology based on OpenPGP.

Requires a home folder to store:
- public keyring
- private keyring
- trust database

Features
- keys: generate, list, delete, import, export; search/scan, send
- encryption, decryption: using public/private keys
- signing, verification w/trust levels (TRUST_UNDEFINED, TRUST_NEVER, TRUST_MARGINAL, TRUST_FULLY, TRUST_ULTIMATE)

Signing
- data intended for digital signing is signed with the private key of the signer
- each recipient can verify the signed data using the corresponding public key
- signatures can embedded in data or detached as a separate file
- keys can expire and be revoked

Note that even if you have a valid signature, you may want to not rely on that validity, if the key used for
signing has expired or was revoked. If this information is available, it will be in the key_status attribute.


Signing AND encryption together

The resulting encrypted data contains the signature. When decrypting the data, upon successful decryption,
signature verification is also performed (assuming the relevant public keys are available at the recipient end).

Windows: Recommendation is to use GPG 1.4 executables vs 2.x

1.4 distribution: Standalone gpg.exe and iconv.dll. No registry or other runtime files required.

2.x distribution Simple deployment does not work; there are more dependent files which you have to ship.

Recommendation is to stick with GnuPG 1.4.x on Windows, unless you specifically need 2.0 features.
In which case, you may have to do a full GPG installation rather than just relying on a couple of files.

"""


# standard lib
import base64
import logging


# common lib
from common import delete_file
from common import force_file_ext
from common import hash_file
from common import hash_str
from common import is_file
from common import just_file_name
from common import load_text
from common import log_setup
from common import log_session_info
from common import save_text


# module level logger
logger = logging.getLogger(__name__)


class Signature:

	"""
	Support signing files and verifying files by their signatures.
	File signatures are stored in *.sig files.

	# example
	signature = Signature('readme.txt', 'my-secret')
	signature.sign()
	if signature.verify():
		print('Signature verify: passed')
	else:
		print('Signature verify: failed')

	"""

	def __init__(self, file_name, secret=''):
		self.file_name = file_name
		self.secret = secret
		if not secret:
			logger.warning(f'Signature.secret: blank secret ({self.file_name})')

	def signature(self):
		"""
		Generates a signature for a file based on:
		- file contents
		- file name (minus path since files can be moved)
		- optional secret
		"""
		if not is_file(self.file_name):
			logger.warning(f'Signature.signature(): file does not exist ({self.file_name})')
			full_hash = ''
		else:
			file_hash = hash_file(self.file_name)
			full_hash = hash_str(just_file_name(self.file_name) + file_hash + self.secret)
		return full_hash

	def signature_file_name(self):
		return force_file_ext(self.file_name, 'sig')

	def sign(self):
		"""Creates a *.sig file containing a signature for file_name."""
		if not is_file(self.file_name):
			logger.warning(f'Signature.sign(): file does not exist ({self.file_name})')
		else:
			file_signature = self.signature()
			save_text(self.signature_file_name(), file_signature)

	def verify(self):
		""""Verifies a file's *.sig file matches its signature."""
		if not is_file(self.file_name):
			logger.warning(f'Signature.verify(): file does not exist ({self.file_name})')
			return False
		elif not is_file(self.signature_file_name()):
			logger.warning(f'Signature.verify(): signature does not exist ({self.signature_file_name()})')
			return False
		else:
			file_signature = self.signature()
			saved_signature = load_text(self.signature_file_name(), '')
			return file_signature == saved_signature


# this is a placeholder for actual encryption
def encrypt_data(key, data):
	"""Encrypt data with given key; returns bytes/utf-8 text based on data's type."""
	if isinstance(data, bytes):
		return base64.b64encode(data)
	elif isinstance(data, str):
		# convert bytes to str representation and trim off leading "b'" and trailing "'"
		return str(base64.b64encode(data.encode()))[2:-1]
	else:
		return data


# this is a placeholder for actual encryption
def decrypt_data(key, data):
	"""Decrypt data with given key; returns bytes/utf-8 text based on data's type."""
	if isinstance(data, bytes):
		return base64.b64decode(data)
	elif isinstance(data, str):
		return base64.b64decode(data).decode()
	else:
		return data


def encrypt_text_file(key, source_file_name, target_file_name=None):
	"""Encrypt text file to target file name."""
	if not target_file_name:
		# default target file name is source file name with '_e' added to file extension
		target_file_name = source_file_name + '_e'

	decrypted_data = load_text(source_file_name)
	encrypted_data = encrypt_data(key, decrypted_data)
	save_text(encrypted_data, target_file_name)


def decrypt_text_file(key, source_file_name, target_file_name=None):
	"""Decrypt text file to target file name."""
	if not target_file_name:
		# if source file extension ends with '_e', decrypt to source file name minus this suffix
		if source_file_name.lower().endswith('_e'):
			target_file_name = source_file_name[:-2]
		else:
			raise Exception('target_file_name not specified')

	encrypted_data = load_text(source_file_name)
	decrypted_data = decrypt_data(key, encrypted_data)
	save_text(decrypted_data, target_file_name)


def _output_encrypt_decrypt_results(original, encrypted, decrypted):
	"""Output comparison of original, encrypted, decrypted results."""
	logging.info(f'Original:  [{original}]')
	logging.info(f'Encrypted: [{encrypted}]')
	logging.info(f'Decrypted: [{decrypted}]')
	logging.info(f'Original == decrypted: {original == decrypted}')


def test_encrypt_decrypt_data():
	"""Test encrypt/decrypt of text/byte strings/arrays."""

	key = 'abc'

	original = 'This is text data.'
	encrypted = encrypt_data(key, original)
	decrypted = decrypt_data(key, encrypted)
	_output_encrypt_decrypt_results(original, encrypted, decrypted)

	original = b'This is binary data.'
	encrypted = encrypt_data(key, original)
	decrypted = decrypt_data(key, encrypted)
	_output_encrypt_decrypt_results(original, encrypted, decrypted)


def test_encrypt_decrypt_text_file():
	"""Test text file encryption/decryption."""

	# setup
	key = 'abc'
	original = 'This is text file data.'
	original_file_name = 'test_encryption_file.txt'
	encrypted_file_name = original_file_name + '_encrypted'
	decrypted_file_name = original_file_name + '_decrypted'
	save_text(original, original_file_name)

	# test
	encrypt_text_file(key, original_file_name, encrypted_file_name)
	encrypted = load_text(encrypted_file_name)
	decrypt_text_file(key, encrypted_file_name, decrypted_file_name)
	decrypted = load_text(decrypted_file_name)
	_output_encrypt_decrypt_results(original, encrypted, decrypted)

	# cleanup
	delete_file(original_file_name)
	delete_file(encrypted_file_name)
	delete_file(decrypted_file_name)


def test():
	"""Test suite for this module."""
	test_encrypt_decrypt_data()
	test_encrypt_decrypt_text_file()


# test code
def main():
	# default and custom basicConfig()
	log_setup()
	log_session_info()
	logger.info('Started')
	test()


# test code
if __name__ == '__main__':
	main()
