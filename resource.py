#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
resource.py

Wrapper for creating resources from config based section objects.

"""


# standard lib
import functools
import logging


# common lib
from common import create_folder
from common import force_file_ext
from common import is_file
from common import load_jsonpickle
from common import log_session_info
from common import log_setup
from common import save_jsonpickle
from common import force_local_path


# udp classes
from config import ConfigSectionKey


# module level logger
logger = logging.getLogger(__name__)


# exceptions
class ResourceException(Exception):
	pass


# https://realpython.com/primer-on-python-decorators/
# https://stackoverflow.com/questions/1367514/how-to-decorate-a-method-inside-a-class
def ensure_connected(func):
	"""
	Assumes wrapped object/class exposes .name and .exception attributes.
	"""
	# preserve information about the function/method being wrapped
	@functools.wraps(func)
	def check_connection(self, *args, **kwargs):
		if self.is_connected:
			return func(self, *args, **kwargs)
		else:
			method_name = f'{type(self).__name__}.{func.__name__}()'
			error_message = f'Resource not connected ({self.name}): {method_name}'
			logger.warning(error_message)
			raise self.exception(error_message)
	return check_connection


class CloudResource:

	def __init__(self, resource):
		self.resource = resource
		self.name = resource.resource_name

	def __call__(self, message=None):
		print(f'In class {self.__class__.__name__}')
		self.resource.dump(False)
		print()


class BlobStoreAzure(CloudResource):
	pass


class BlobStoreLocal(CloudResource):
	pass


class KeyVaultAzure(CloudResource):
	pass


class KeyVaultLocal(CloudResource):
	pass


class Resource:

	def __call__(self, ad, section_name):
		class_names = 'BlobStore KeyVault StorageQueue'
		config = ConfigSectionKey('../src')
		config.load('cloud.ini')
		resource = config(section_name)
		print(resource)
		resource_type = propercase_by_example(resource.resource_type, class_names)
		resource_class = f'{resource_type}{resource.platform.title()}'
		obj = eval(resource_class)(resource)

		# verify ad.identity has been authenticated
		if ad.identity is None:
			logger.warning('Identity not authenticated')
			raise ResourceException('Identity not authenticated')

		# verify access to resource
		if ad.has_access(obj):
			return obj
		else:
			logger.warning(f'Identity {ad.identity_name} not authorized for {resource.name}')
			return None


'''
def _resource_id(self, resource):
	return f'{resource.__class__.__name__}.{resource.name}'
'''


class AD:

	class Identity:

		def __init__(self, identity_name, secret='', password='', use_certificate=False):
			self.identity_name = identity_name.lower()
			self.secret = secret
			self.password = password
			self.use_certificate = use_certificate
			self.resources = set()

	"""
	Store local authentication in tenant authentication file.
	"""

	def __init__(self, tenant):
		# tenant
		self.tenant = force_local_path(tenant.lower())

		# make sure we have a tenant
		create_folder(self.tenant)

		self.ad_file_name = force_file_ext(f'{self.tenant}/ad', 'json')

		# start in a non-authenticated state
		self.identity = None
		self.is_authenticated = False

		# load our directory
		if is_file(self.ad_file_name):
			self._load()
		else:
			self.identities = dict()

	def _load(self):
		self.identities = load_jsonpickle(self.ad_file_name)

	def _save(self):
		save_jsonpickle(self.ad_file_name, self.identities)

	def __str__(self):
		if not self.identity:
			return 'No authenticated identity'
		else:
			return f'Identity({self.identity.identity_name}); grants: {", ".join(self.identity.resources)}'

	def create_identity(self, identity_name, secret='', password='', use_certificate=False):
		identity_name = identity_name.lower()
		identity = self.Identity(identity_name, secret=secret, password=password, use_certificate=use_certificate)
		self.identities[identity_name] = identity
		self._save()

	def grant_access(self, identity_name, resource_name):
		identity_name = identity_name.lower()
		resource_name = resource_name.lower()
		if identity_name not in self.identities:
			logger.warning(f'Unknown identity ({identity_name})')
		else:
			self.identities[identity_name].resources.add(resource_name)
			self._save()

	def check_access(self, identity_name, resource_name):
		identity_has_access = False
		identity_name = identity_name.lower()
		resource_name = resource_name.lower()

		if identity_name not in self.identities:
			logger.warning(f'Unknown identity ({identity_name})')
		else:
			identity = self.identities[identity_name]
			if resource_name in identity.resources:
				identity_has_access = True

		logger.debug(f'{identity_name} access to {resource_name}: {identity_has_access}')
		return identity_has_access

	def OLD_has_access(self, resource):
		if not hasattr(resource, 'name'):
			identity_has_access = False
		else:
			resource_name = resource.name.lower()
			identity_has_access = resource_name in self.identity.resources
		return identity_has_access

	def has_access(self, resource):
		if not self.identity:
			identity_has_access = False
		else:
			identity_has_access = resource.resource_name in self.identity.resources
			# logger.info(f'{resource.resource_name}; {self.identity.resources}')
		return identity_has_access

	def authenticate(self, identity_name, secret='', password=''):
		identity = None

		identity_name = identity_name.lower()
		if identity_name not in self.identities:
			logger.warning(f'Unknown identity ({identity_name})')
			is_authenticated = False
		else:
			identity = self.identities[identity_name]
			if identity.use_certificate:
				is_authenticated = True
				logger.debug(f'{identity_name} authenticated via certificate')
			elif not identity.secret and not identity.password:
				is_authenticated = True
				logger.debug(f'{identity_name} authenticated via managed identity')
			elif identity.secret:
				is_authenticated = identity.secret == secret
				if is_authenticated:
					logger.debug(f'{identity_name} authenticated via secret')
				else:
					logger.warning(f'{identity_name} failed secret authentication')
			elif identity.password:
				is_authenticated = identity.password == password
				if is_authenticated:
					logger.debug(f'{identity_name} authenticated via password')
				else:
					logger.warning(f'{identity_name} failed password authentication')
			else:
				logger.warning(f'Unexpected authentication failure ({identity_name})')
				is_authenticated = False

		# track our authentication status
		self.is_authenticated = is_authenticated
		if is_authenticated:
			self.identity = identity
		else:
			self.identity = None

		return is_authenticated


# TODO: How do we discover list of class names in modules or do we hard code?
def propercase_by_example(name, str_of_examples):
	lookup = dict()
	for example in str_of_examples.split():
		lookup[example.lower()] = example
	return lookup.get(name.lower(), name)


def test():
	# authentication emulator does not enforce permissions

	"""
	# TODO: Convert '*' in identity names and resource names to '-<SDLC>' (<sdlc-type>[-<sdlc-name>])
	#
	# TODO: How are app vs sp identities different and why would we use an app identity vs just sp ???
	#
	# Note: Use explicit identity/resource names to access resources across SDLC boundaries.
	#
	# AD.connect('user:admin*', ...)
	# AD.allow('sp:capture', 'kv:capture*', 'bs:landing*')

	# create identities
	AD.create_identity('user:admin-dev', guid='guid-user-admin', password='$admin-user-password$')
	AD.create_identity('sp:capture-dev', guid='guid-sp-capture', secret='$capture-sp-secret$')
	AD.create_identity('sp:capture-dev', guid='guid-sp-capture', use_certificate=True)
	AD.create_identity('mi:stage-dev')

	# sas tokens are a resource based identity
	AD.set_identity('sas:bs_landing-dev', sas='guid-bs-landing')

	# grant named identities access to named resources
	AD.grant_access('sp:capture', 'kv:capture-dev', 'bs:landing-dev')
	AD.grant_access('mi:archive', 'bs:landing-dev', 'bs:archive-dev', 'db:datalake')
	AD.grant_access('sas:123-456', 'bs:landing-dev')

	# authenticate a named identity
	auth = AD.authenticate('sp:capture', secret='...')

	# use resources
	key_vault = KeyVault('kv-capture-dev', auth)
	key_vault.connect('kv-capture-dev', auth)
	blob_store_landing = BlobStore('bs-landing-dev', auth)

	# resources validate auth objects during .connect() call
	# allowed(self) does name normalization and lower() before matching
	# bs:landing-dev should match bs-landing-dev or bslandingdev in some scenarios
	if not auth.is_allowed(self):
		raise self.Exception('authentication failed for {auth.identity}')

	"""

	# TODO: This needs to be updated per above.
	#
	# [resource:bs_landing-dev]
	#
	# identity|capture = sp:capture (authenticate via sp)
	# identity|archive = sp:archive (authenticate via mi)
	#
	# -OR-
	#
	# ; resources that support SAS tokens can generate multiple SAS tokens for different ACL permissions
	# identity|sas_capture = secret:<sas-for-capture> (authenticate via named sas token)
	# identity|sas_archive = secret:<sas-for-archive> (authenticate via named sas token)

	ad = AD('local')

	# create identities
	ad.create_identity('sp:capture', secret='capture/123')
	ad.create_identity('mi:stage')

	# grant access
	ad.grant_access('sp:capture', 'bs_landing_dev')
	ad.grant_access('sp:capture', 'kv_capture_dev')
	ad.grant_access('mi:stage', 'bs_landing_dev')

	ad.check_access('sp:capture', 'bs_landing_dev')
	ad.check_access('sp:capture', 'bs_landing_prod')
	ad.check_access('mi:stage', 'bs_landing_dev')
	ad.check_access('sp:bad', 'bs_landing_dev')

	# authenticate
	ad.authenticate('mi:stage')
	ad.authenticate('sp:capture', secret='bad_secret')
	ad.authenticate('sp:capture', secret='capture/123')

	# output info
	logger.info(f'ad = {ad}')

	if not ad.identity:
		logger.info('Not authenticated')
	else:
		logger.info(f'Identity: {ad.identity.identity_name}')
		logger.info(f'Grants: {", ".join(ad.identity.resources)}')

	# resource wrapper
	resource = Resource()
	azure_resource = resource(ad, 'resource:bs_landing_dev_azure')
	local_resource = resource(ad, 'resource:bs_landing_dev_local')
	key_vault = resource(ad, 'resource:kv_capture_dev_azure')
	azure_resource()
	local_resource()
	key_vault()

	logger.info(f'ad.has_access(azure_resource) = {ad.has_access(azure_resource)}')
	logger.info(f'ad.has_access(local_resource) = {ad.has_access(local_resource)}')


# test code
if __name__ == '__main__':
	log_setup(log_level=logging.DEBUG)
	log_session_info()
	test()

	# class_names = 'BlobStore CosmosDB KeyVault StorageQueue'
	# print(propercase_by_example('blobstore', class_names))
	# print(propercase_by_example('cosmosdb', class_names))
	# print(propercase_by_example('keyvault', class_names))
	# print(propercase_by_example('storagequeue', class_names))
