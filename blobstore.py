#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
blobstore.py

Basic blobstore emulator using local storage.

Ignores tenant and subscription settings.

Azure naming conventions
- storage account: globally unique, 3-24 chars, containing only a-z (lowercase), 0-9
- container name: 3-63 chars, containing only a-z (lowercase), 0-9 and -
- blob name: 1-1024 chars, any URL-safe char

Example blobstore references/resource names
- blobstore_landing = resource:bs_landing_local
- blobstore_archive = resource:bs_archive_local

"""


# standard lib
import functools
import glob
import logging


# common lib
from common import clear_folder
from common import copy_file_if_exists
from common import create_folder
from common import delete_file
from common import delete_folder
from common import force_trailing_slash
from common import is_file
from common import is_folder
from common import just_path
from common import log_session_info
from common import log_setup
from common import save_text
from common import force_local_path


# udp classes
from config import ConfigSectionKey


# module level logger
logger = logging.getLogger(__name__)


# Decorator references:
# https://realpython.com/primer-on-python-decorators/
# https://stackoverflow.com/questions/1367514/how-to-decorate-a-method-inside-a-class
#
# Python exceptions:
# https://docs.python.org/3/library/exceptions.html
#
def ensure_connected(func):
    """
	Ensures an object is connected before executing a method.
	"""
    # preserve information about the function/method being wrapped
    @functools.wraps(func)
    def check_connection(self, *args, **kwargs):
        # require a non-empty resource
        if self.resource:
            return func(self, *args, **kwargs)
        else:
            # gather exception diagnostics
            method_name = f"{type(self).__name__}.{func.__name__}()"
            error_message = (
                f"Resource not connected ({self.resource_name}): {method_name}"
            )
            logger.error(error_message)
            raise ConnectionError(error_message)

    return check_connection


class BlobStore:
    def __init__(self):
        # emulated cloud root folder
        self.cloud_folder = "../sessions/cloud"

        # resource object that describes endpoint
        self.resource = None

        # blobstore attributes used by emulator
        self.tenant_id = ""
        self.subscription_id = ""
        self.account_name = ""
        self.container_name = ""
        self.resource_name = ""

    def _blob_file(self, blob_name):
        """Returns a physical path to blob name based on account/container names."""
        return f"{self._blob_folder()}/{blob_name}"

    def _blob_folder(self):
        """Returns a physical folder path blob account/container."""

        # extend with tenant_id and/or subscription_id if multi-tenant/subscription support required
        return f"{self.cloud_folder}/{self.account_name}/{self.container_name}"

    def _load_resource(self, resource):
        """Load relevant attributes from resource into our object namespace."""
        self.resource = resource
        self.tenant_id = resource.tenant_id
        self.subscription_id = resource.subscription_id
        self.account_name = resource.account_name
        self.container_name = resource.container_name
        self.resource_name = f"{self.account_name}:{self.container_name}"

    def _context(self, message, blob_name=None):
        """Provide context for log messages."""

        # extend with tenant_id and/or subscription_id if multi-tenant/subscription support required
        context = f"blobstore({self.account_name}:{self.container_name})"
        if blob_name:
            context = f"{context}({blob_name})"
        return f"{context}: {message}"

    def create(self, resource):
        """Create a container within resource's storage account."""
        self._load_resource(resource)
        blob_folder = self._blob_folder()
        if is_folder(blob_folder):
            logger.warning(self._context("Container already exists"))
        else:
            # create new container
            logger.info(self._context("Creating container"))
            create_folder(blob_folder)

        self.disconnect()
        return is_folder(blob_folder)

    def remove(self, resource):
        """Remove a container from resource's storage account."""
        self._load_resource(resource)
        blob_folder = self._blob_folder()
        if not is_folder(blob_folder):
            logger.warning(self._context(f"Container does not exist"))
            is_success = False
        else:
            logger.info(self._context(f"Removing container"))
            delete_folder(blob_folder)
            is_success = True

        self.disconnect()
        return is_success

    def connect(self, resource):
        """Connect to container within resource's storage account."""
        self._load_resource(resource)
        blob_folder = self._blob_folder()

        # make sure folder exists
        if is_folder(blob_folder):
            logger.debug(self._context("Connecting to container"))
        else:
            logger.warning(self._context("Container does not exist"))
            self.disconnect()
        return self.resource is not None

    def disconnect(self):
        self.resource = None
        self.tenant_id = ""
        self.subscription_id = ""
        self.account_name = ""
        self.container_name = ""
        self.resource_name = ""
        return True

    @ensure_connected
    def get(self, target_file_name, blob_name):
        """Download blob to target file name."""
        blob_folder = self._blob_folder()
        source_file_name = f"{blob_folder}/{blob_name}"
        if not is_file(source_file_name):
            logger.warning(self._context("Blob name does not exist", blob_name))
            is_success = False
        else:
            logger.debug(self._context(f"Getting {target_file_name}", blob_name))
            copy_file_if_exists(source_file_name, target_file_name)
            is_success = True
        return is_success

    @ensure_connected
    def put(self, source_file_name, blob_name):
        """"Upload source file name to blob."""
        if not is_file(source_file_name):
            warning_message = f"Source file does not exist ({source_file_name})"
            logger.warning(self._context(warning_message, blob_name))
            is_success = False
        else:
            logger.debug(self._context(f"Putting {source_file_name}", blob_name))

            # build blob target file and folder names
            blob_folder = self._blob_folder()
            target_file_name = f"{blob_folder}/{blob_name}"
            target_folder = just_path(target_file_name)

            # make sure the blob's target folder exists
            create_folder(target_folder)

            # then copy source file to blob container
            copy_file_if_exists(source_file_name, target_file_name)
            is_success = True
        return is_success

    @ensure_connected
    def delete(self, blob_name):
        """Delete blob."""
        target_file_name = self._blob_file(blob_name)
        if not is_file(target_file_name):
            warning_message = f"Blob name does not exist"
            logger.warning(self._context(warning_message, blob_name))
            is_success = False
        else:
            logger.debug(self._context(f"Deleting blob", blob_name))
            delete_file(target_file_name)
            is_success = True
        return is_success

    @ensure_connected
    def list(self, glob_pattern=""):
        """
		Return a sorted list of blob names (and blob folder names) based on optional glob_pattern.
		Blob folder refers to the concept of logical folders which are implemented via slash delimited prefixes.

		Behavior (blob names refers to both blobs and blob folder names)
		- glob_pattern == '': return blob names in container's root
		- glob_pattern == <folder> or <folder>/: return blob names in specified folder
		- glob_pattern == <specific-blob-name>: return specific blob name
		- glob_pattern == <glob_pattern>: returns blob names matching glob_pattern
		"""
        # strip relative path so we don't step outside our emulated storage area
        glob_pattern = force_local_path(glob_pattern)

        # analyze glob_pattern to determine how to return blob names

        # if glob_pattern is a folder
        if not glob_pattern:
            # default to all blobs at the root level
            glob_pattern = "*"
        elif is_folder(f"{self._blob_folder()}/{glob_pattern}"):
            # if glob_pattern is a folder, return all blobs within folder
            glob_pattern = f"{force_trailing_slash(glob_pattern)}*"
        else:
            # use glob_pattern as-is
            pass

        # retrieve sorted blob names
        target_path = f"{self._blob_folder()}/{glob_pattern}"

        # build list of blob names with local parent path stripped from names
        blob_names = list()
        for blob_name in sorted(glob.glob(target_path)):
            # format name using Linux path delimiters
            blob_name = blob_name.replace(chr(92), "/")
            blob_name = blob_name.replace(f"{self._blob_folder()}/", "")
            blob_names.append(blob_name)

        blob_count = len(blob_names)
        logger.debug(
            self._context(f"list({glob_pattern}) returned {blob_count} blob names")
        )
        logger.debug(self._context(f"list({glob_pattern}) = {blob_names}"))
        return blob_names

    @ensure_connected
    def clear(self):
        """Remove all blobs from resource's container."""
        logger.info(self._context("Clearing container"))
        clear_folder(self._blob_folder())
        return True


# temporary test harness


def test():

    # configuration driven support
    config = ConfigSectionKey("../conf", "../local")
    config = config
    config.load("bootstrap.ini", "bootstrap")
    config.load("init.ini")
    config.load("connect.ini")

    bs_test = BlobStore()
    resource = config("resource:bs_test_local")
    bs_test.create(resource)
    bs_test.connect(resource)
    bs_test.remove(resource)
    bs_test.create(resource)

    # # good things
    save_text("testfile-1.txt", "test file")
    delete_file("testfile-2.txt", ignore_errors=True)

    # expected Connection exception
    try:
        bs_test.put("testfile-1.txt", "downloads/testfile-1.txt")
    except ConnectionError as e:
        logger.info(f"Non-connected resource raised ConnectionError as expected: {e}")

    bs_test.connect(resource)
    assert bs_test.put("testfile-1.txt", "downloads/testfile-1.txt")
    assert bs_test.put("testfile-1.txt", "downloads/testfile-2.txt")
    assert bs_test.put("testfile-1.txt", "downloads/testfile-3.txt")
    assert bs_test.get("testfile-2.txt", "downloads/testfile-2.txt")

    downloads_folder_only = ["downloads"]
    downloads_folder_files = [
        "downloads/testfile-1.txt",
        "downloads/testfile-2.txt",
        "downloads/testfile-3.txt",
    ]
    # assert bs_test.list() == downloads_folder_only
    # assert bs_test.list('*') == downloads_folder_only
    # assert bs_test.list('/') == downloads_folder_only
    # assert bs_test.list('/downloads') == downloads_folder_files
    # assert bs_test.list('downloads') == downloads_folder_files
    # assert bs_test.list('downloads/') == downloads_folder_files

    bs_test.list("downloads")
    bs_test.list("downloads/")
    bs_test.list("downloads/*")
    bs_test.delete("downloads/testfile-1.txt")
    bs_test.list("downloads/*")

    # bad things
    assert not bs_test.list("bad-path*")
    assert not bs_test.put("bad-file-1.txt", "downloads/bad-file.txt")
    assert not bs_test.get("bad-file-2.txt", "downloads/bad-file.txt")
    assert not bs_test.delete("downloads/bad-file.txt")
    bs_test.clear()


# test code
if __name__ == "__main__":
    log_setup(log_level=logging.DEBUG)
    log_session_info()
    test()
