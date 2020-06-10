#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
section.py

Defines custom section blocks referenced by conf.py.

Section properties are validated unless <section>._is_validated = False.
"""


# standard lib
import logging


# module level logger
logger = logging.getLogger(__name__)


class Section:

    """Subclass for each section type block of properties."""

    def __init__(self, section_key=''):
        # common private attributes
        self._is_validated = True
        self._section_key = section_key.strip().lower()

        # common attributes across all sections
        self.description = ''
        self.note = ''
        self.test_data = ''
        self.setup()

    def is_validated(self):
        """Public interface to ._is_validated attribute value."""
        return self._is_validated

    def setup(self):
        """Subclass with section properties and default values."""
        pass

    def section_key(self):
        """Public interface to section key value."""
        return self._section_key

    def dump(self, dump_blank_values=True):
        # dump section attributes
        logger.info(f'[{self._section_key}]')
        for key, value in self.__dict__.items():
            # ignore hidden attributes
            if key.startswith('_'):
                pass
            elif dump_blank_values:
                logger.info(f'{key} = {value}')
            elif not dump_blank_values and value:
                logger.info(f'{key} = {value}')


class SectionAccess(Section):

    # noinspection PyAttributeOutsideInit
    def setup(self):
        self.allow = list()
        self.block = list()


class SectionBootstrap(Section):

    # noinspection PyAttributeOutsideInit
    def setup(self):
        self._is_validated = False


# DEPRECATED: 2019-02-01
class SectionCloud0(Section):

    # noinspection PyAttributeOutsideInit
    def setup(self):
        self.platform = ''
        self.account_id = ''
        self.account_alias = ''
        self.region = ''

        # security
        self.role = ''
        self.username = ''
        self.public_key = ''
        self.private_key = ''

        # resources
        self.admin_objectstore = ''
        self.archive_objectstore = ''
        self.capture_objectstore = ''
        self.system_objectstore = ''
        self.capture_queue = ''


class SectionDatabase(Section):

    # noinspection PyAttributeOutsideInit
    def setup(self):
        self.platform = ''
        self.driver = ''
        self.host = ''
        self.port = ''
        self.timezone = ''
        self.database = ''
        self.schema = ''

        # non-standard options used at connection time
        self.options = ''

        # optional command executed at connection time
        self.on_connect = ''

        # if key_vault provided, then key_vault value used to lookup [keyvault:<name>]
        # and authentication values used as key vault secret names/ids to retrieve secrets
        self.key_vault = ''

        # authentication
        self.username = ''
        self.password = ''


class SectionNamespace(Section):

    # noinspection PyAttributeOutsideInit
    def setup(self):
        # primary key for datasets
        self.dataset = ''

        # optional schema name override (does not change file/folder names)
        self.schema = ''

        # namespace components; extend as necessary
        self.entity = ''
        self.location = ''
        self.system = ''
        self.instance = ''
        self.subject = ''
        self.sdlc = ''


class SectionEnvironment(Section):

    # noinspection PyAttributeOutsideInit
    def setup(self):
        self.sdlc_type = ''
        self.sdlc_name = ''
        self.sdlc_date = ''


class SectionFile(Section):

    # noinspection PyAttributeOutsideInit
    def setup(self):
        self.copy_file = ''
        self.move_file = ''
        self.delete_file = ''
        self.ignore_file = ''
        self.column = dict()


# NEW: 2019-02-01 Cloud/local resources
class SectionIdentity(Section):

    # noinspection PyAttributeOutsideInit
    def setup(self):
        """Supports MI, SP (secret/certificate), keys and tokens."""

        # types: mi, sp, cert (sp w/cert), key, sas
        self.authentication_type = ''

        # tenant id required for authentication
        self.tenant_id = ''

        # SP secret authentication
        self.client_id = ''
        self.client_oid = ''
        self.client_secret = ''

        # SP certificate based authentication
        self.certificate_pem = ''
        self.certificate_thumbprint = ''

        # treat keys and tokens as named identities
        self.account_key = ''
        self.sas_token = ''


# NEW: 2019-02-01 Cloud/local resources
class SectionPlatform(Section):

    # noinspection PyAttributeOutsideInit
    def setup(self):
        self.platform_name = ''
        self.tenant_id = ''


class SectionProject(Section):
    # noinspection PyAttributeOutsideInit
    def setup(self):
        # not validated, but we pre-define common properties ...
        self._is_validated = False

        # set to 1 to ignore a project when processing multiple projects
        self.ignore_project = ''

        # capture, archive, stage, transform, ... (replaces script)
        self.type = ''

        # orchestrators execute groups of projects (also known as a run_group)
        self.group = ''

        # runtime option
        self.options = ''
        self.batch_size = ''

        # cloud and database resources
        self.key_vault = ''
        self.database_source = ''
        self.database_target = ''

        # blobstores
        self.blobstore_landing = ''
        self.blobstore_archive = ''
        self.blobstore_recovery = ''
        self.blobstore_system = ''


# NEW: 2019-02-01 Cloud/local resources
class SectionResource(Section):

    # noinspection PyAttributeOutsideInit
    def setup(self):
        # platform (local, azure, aws, etc)
        self.sdlc = ''
        self.platform = ''

        # context
        self.tenant_id = ''
        self.subscription_id = ''

        # resource identity
        self.resource_type = ''
        self.resource_name = ''
        self.account_name = ''
        self.container_name = ''

        # self.resource_group = ''
        # self.region = ''
        #
        # # identity
        # self.client_id = ''
        # self.client_secret = ''
        # self.client_oid = ''
        #
        # # identity
        # self.principal_id = ''
        # self.username = ''
        #
        # # authentication
        # self.authentication_type = ''
        # self.certificate = ''
        # self.password = ''
        # self.secret = ''
        # self.public_key = ''
        # self.private_key = ''
        # self.account_key = ''
        # self.sas_token = ''


class SectionSchedule(Section):

    # noinspection PyAttributeOutsideInit
    def setup(self):
        # schedule header
        self.schedule_name = ''

        # set to 1 to ignore schedule
        self.ignore_schedule = ''

        # timezone (see pytz.all_timezones for Olson timezone names)
        # https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
        #
        # Use tz database name with Canonical vs. Alias or Deprecated status values.
        # Be specific where timezone nuances exist, eg. AZ, MI, eastern IN, etc.
        #
        # Examples (not complete; intended to cover early use-cases)
        #
        # Deprecated    Canonical (common US and CA timezones)
        #
        # UTC         > Etc/UTC
        # US/Eastern  > America/New_York or America/Toronto
        # US/Central  > America/Chicago or America/Winnipeg
        # US/Mountain > America/Denver or America/Edmonton
        # US/Pacific  > America/Los_Angles or America/Vancouver
        self.timezone = ''

        # polling frequency in seconds (simplest schedule)
        self.poll_frequency = '5'

        # daily scheduling
        self.daily_at = ''
        self.hourly_at = ''

        # additional filters (daily and hourly applied to these conditions)
        self.on_days_of_week = ''
        self.on_days_of_month = ''
        self.on_days_of_year = ''

        # exceptions (skip rules)
        self.skip_hours_of_day = ''
        self.skip_days_of_week = ''
        self.skip_days_of_month = ''
        self.skip_days_of_year = ''


class SectionTable(Section):

    # noinspection PyAttributeOutsideInit
    def setup(self):
        self.schema_name = ''

        # optional catalog attributes
        self.table_comment = ''

        # tags are searchable tags to identify tables via an agile classification scheme
        # Future: Tags could be a single space/comma delimited string, a list, or a dict
        # Tag examples: customer, product, sale, usage-lift, usage-lodging, finance, reference, ...
        # Dict syntax would be tag|<tag-name>=1 or tag|<tag-name>= (to clear specific tag)
        # Dict syntax would allow inheriting/clone multi-value tags; adding/deleting individual tags with precision
        self.table_tags = ''

        # table_type: <blank> (default), columnar, memory, or columnar-memory; stage uses when creating table
        self.table_type = ''
        self.table_name = ''
        self.table_prefix = ''
        self.table_suffix = ''
        self.drop_table = ''
        self.ignore_table = ''

        # optional tables don't raise an exception if they're not present
        # use when table inventory across SDLC environments may not match
        self.optional_table = ''

        # Future: convert these to singular (vs plural) dict() for easier to read configurations
        self.ignore_columns = ''
        self.sensitive_columns = ''

        # override auto column conversion; specify a specific target column type
        # Use case: PostgreSQL char columns specified without size constraint.
        self.column = dict()

        # Future: primary_key was natural_key - rename back to natural key (dp_nk)
        self.primary_key = ''
        self.cdc = ''
        self.timestamp = ''
        self.first_timestamp = ''
        self.rowversion = ''
        self.first_rowversion = ''
        self.sequence = ''
        self.first_sequence = ''
        self.join = ''
        self.where = ''
        self.order = ''
        self.delete_when = ''
