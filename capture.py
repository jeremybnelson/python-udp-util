#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
capture.py

Capture data from a source database based on project_capture_*.ini file.

Database and resource (local/cloud) connections sourced from:
- bootstrap.ini (cloud configuration for current SDLC environment)
- init.ini (optional overrides of bootstrap)
- connect.ini (master collection of all endpoints)

Options
--onetime - run once/immediately; use when this script called via external scheduler
--nowait - execute immediately, then follow regular schedule
--notransfer - don't transfer captured data to blobstore; use for local testing
"""


# standard lib
import contextlib
import datetime
import logging
import shutil
import sys


# common lib
from common import copy_file_if_exists
from common import clear_folder
from common import create_folder
from common import describe
from common import file_size
from common import is_file
from common import is_glob_match
from common import iso_to_datetime
from common import just_file_name
from common import load_jsonpickle
from common import save_jsonpickle
from common import save_text
from common import script_name
from common import split


# udp lib
import cdc_select
import database


# udp classes
from blobstore import BlobStore
from daemon import Daemon
from event import Events


# module level logger
logger = logging.getLogger(__name__)


# job.ini - job_id counter and last_run_time for range checks
# allow last_run_time to be overridden for new tables being onboarded

# table definition parser - need a test mode as well
# .name
# .create
# .pull (capture, extract, ...) - SQL with <job>, <pk>, <timestamp> markers ???
# .pk (expression)
# .timestamp (expression)
# .columns (pulled from create)
# .types (pulled from create)
# .starttime (initial start time, used if table.history doesn't have table reference)
# .dataset_name (pulled from parent folder)


class TableHistory:

    def __init__(self, table_name):
        self.table_name = table_name
        self.last_filehash = None
        self.last_rowhash = None
        self.last_rowversion = None
        self.last_sequence = None
        self.last_timestamp = None

    def __str__(self):
        return describe(self, 'table_name, last_filehash, last_rowhash, last_rowversion, last_sequence, last_timestamp')


class JobHistory:

    def __init__(self, file_name):
        self.file_name = file_name
        self.job_id = 1
        self.tables = dict()

    def __str__(self):
        return describe(self, 'file_name, job_id, tables')

    def dump(self):
        for table_name in sorted(self.tables):
            logger.info(self.get_table_history(table_name))

    def get_table_history(self, table_name):
        table_name = table_name.lower()
        if table_name in self.tables:
            table_history = self.tables[table_name]
        else:
            table_history = TableHistory(table_name)
            self.tables[table_name] = table_history

        logger.info(table_history)
        return table_history

    def load(self):
        if not is_file(self.file_name):
            # file doesn't exist, initialize object with default values
            logger.info(f'Initializing {self.file_name}')
            self.job_id = 1
            self.tables = dict()
        else:
            logger.info(f'Loading {self.file_name}')
            obj = load_jsonpickle(self.file_name)

            # load key attributes
            self.job_id = obj.job_id
            self.tables = obj.tables

    # updating get_table_history()'s table_history object updates original in self.tables[]
    def save(self, is_maintenance=False):
        logger.info(f'Saving file {self.file_name}')

        # increment job_id if we're not in maintenance mode
        if not is_maintenance:
            self.job_id += 1

        save_jsonpickle(self.file_name, self)


class CaptureDaemon(Daemon):

    def __init__(self):
        # inherit and extend default __init__ behavior
        super().__init__()

        # job specific files
        self.capture_file_name = None
        self.zip_file_name = None

        # capture specific properties
        self.dataset_name = None
        self.tables = None
        self.database = None
        self.events = None
        self.job_id = None

        # overall job metrics
        self.job_row_count = 0
        self.job_data_size = 0

    def startup(self):
        # not required in current implementation
        pass

    @staticmethod
    def current_timestamp(db_engine):
        # determine current timestamp based on data source's database server time
        current_timestamp = db_engine.current_timestamp()

        # adjust current timestamp back one minute (step back) to account for open transactions
        current_timestamp = current_timestamp - datetime.timedelta(minutes=1)

        # always begin a capture pull on an integer second boundary
        current_timestamp = current_timestamp.replace(microsecond=0)
        logger.info(f'Current timestamp: {current_timestamp}')
        return current_timestamp

    def process_table(self, db, db_engine, schema_name, table_name, table_object, table_history, current_timestamp, current_sequence=0):
        """Process a specific table."""

        # skip default table and ignored tables
        if table_name == 'default':
            return

        # TODO: Allow ignore and drop table conditions to be passed to archive (log table state) and stage (to drop table and table references)
        elif table_object.ignore_table:
            logger.info(f'Skipping table: {table_name} (ignore_table=1)')
            return
        elif table_object.drop_table:
            logger.info(f'Skipping table: {table_name} (drop_table=1)')
            return

        # initialize table history's last time stamp to first timestamp if not set yet
        if not table_history.last_timestamp:
            # default first timestamp to 1900-01-01 if project has no first timestamp
            if not table_object.first_timestamp:
                table_object.first_timestamp = '1900-01-01'
            table_history.last_timestamp = iso_to_datetime(table_object.first_timestamp)

        # skip table if last timestamp > current timestamp, eg. tables pre-configured for the future
        if table_history.last_timestamp > current_timestamp:
            explanation = f'first/last timestamp {table_history.last_timestamp} > current timestamp {current_timestamp}'
            logger.info(f'Skipping table: {table_name} ({explanation})')
            return

        # if we're here then we have a legit last timestamp value to use for CDC
        last_timestamp = table_history.last_timestamp

        # initialize table's last_sequence to first_sequence if not set yet
        if not table_history.last_sequence:
            if not table_object.first_sequence:
                table_object.first_sequence = 0
            table_history.last_sequence = table_object.first_sequence

        self.events.start(table_name, 'table')
        # logger.info(f'Processing {table_name} ...')

        # create a fresh cursor for each table
        cursor = db.conn.cursor()

        # save table object for stage
        table_file_name = f'{self.work_folder}/{table_name}.table'
        save_jsonpickle(table_file_name, table_object)

        # discover table schema
        table_schema = db_engine.select_table_schema(schema_name, table_name)

        # handle non-existent tables
        if table_schema is None:
            if table_object.optional_table:
                logger.info(f'Optional table not found; skipped ({table_name})')
            else:
                logger.warning(f'Table not found; skipped ({table_name})')
            return

        # remove ignored columns from table schema
        if table_object.ignore_columns:
            # find columns to ignore (remove) based on ignore column names/glob-style patterns
            ignore_columns = []
            for column_name in table_schema.columns:
                for pattern in split(table_object.ignore_columns):
                    if is_glob_match(column_name, pattern):
                        ignore_columns.append(column_name)

            # delete ignored columns from our table schema
            for column_name in ignore_columns:
                logger.info(f'Ignore_column: {table_name}.{column_name}')
                table_schema.columns.pop(column_name)

        # save table schema for stage to use
        schema_table_name = f'{self.work_folder}/{table_name}.schema'
        save_jsonpickle(schema_table_name, table_schema)

        # save table pk for stage to use
        pk_columns = db_engine.select_table_pk(schema_name, table_name)
        if not pk_columns and table_object.primary_key:
            pk_columns = table_object.primary_key
        save_text(f'{self.work_folder}/{table_name}.pk', pk_columns)

        # normalize cdc setting
        table_object.cdc = table_object.cdc.lower()
        if table_object.cdc == 'none':
            table_object.cdc = ''

        # clear unknown cdc settings
        if table_object.cdc and table_object.cdc not in ('filehash', 'rowhash', 'rowversion', 'sequence', 'timestamp'):
            logger.warning(f'Warning: Unknown CDC setting; CDC setting cleared ({table_name}.cdc={table_object.cdc})')
            table_object.cdc = ''

        # clear cdc setting when no pk_columns are present
        # NOTE: filehash cdc does not require pk_columns.
        if table_object.cdc and table_object.cdc != 'filehash' and not pk_columns:
            logger.warning(f'Warning: CDC enabled but no PK; CDC setting cleared ({table_name}.cdc={table_object.cdc})')
            table_object.cdc = ''

        # if no cdc, then clear cdc related attributes
        if not table_object.cdc:
            table_object.filehash = ''
            table_object.rowhash = ''
            table_object.rowversion = ''
            table_object.sequence = ''
            table_object.timestamp = ''

        # update table object properties for cdc select build
        column_names = list(table_schema.columns.keys())
        table_object.schema_name = schema_name
        table_object.table_name = table_name
        table_object.column_names = column_names
        select_cdc = cdc_select.SelectCDC(db_engine, table_object)
        sql = select_cdc.select(self.job_id, current_timestamp, last_timestamp)

        # save generated SQL to work folder for documentation purposes
        sql_file_name = f'{self.work_folder}/{table_name}.sql'
        save_text(sql_file_name, sql)

        # run sql here vs via db_engine.capture_select
        # cursor = db_engine.capture_select(schema_name, table_name, column_names, last_timestamp, current_timestamp)
        cursor.execute(sql)

        # capture rows in fixed size batches to support unlimited size record counts
        # Note: Batching on capture side allows stage to insert multiple batches in parallel.

        if self.project.batch_size:
            batch_size = int(self.project.batch_size)
            # logger.info(f'Using project specific batch size: {self.project.batch_size}')
        else:
            batch_size = 250_000

        batch_number = 0
        row_count = 0
        data_size = 0
        while True:
            batch_number += 1
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break

            logger.info(f'Table({table_name}): batch={batch_number} using batch size {batch_size:,}')
            self.progress_message(f'extracting({table_name}.{batch_number:04}) ...')

            # flatten rows to list of column values
            json_rows = [list(row) for row in rows]
            output_file = f'{self.work_folder}/{table_name}#{batch_number:04}.json'
            save_jsonpickle(output_file, json_rows)

            # track metrics
            row_count += len(json_rows)
            data_size += file_size(output_file)

        # update table history with new last timestamp and sequence values
        table_history.last_timestamp = current_timestamp
        table_history.last_sequence = current_sequence

        # track total row count and file size across all of a table's batched json files
        self.events.stop(table_name, row_count, data_size)

        # save interim metrics for diagnostics
        self.events.save()

        self.job_row_count += row_count
        self.job_data_size += data_size

        # explicitly close cursor when finished
        # cursor.close()
        return

    def compress_work_folder(self):
        """Compress all files in work_folder to single file in publish_folder."""

        # setup
        self.events.start('compress', 'step')
        self.capture_file_name = f'{self.dataset_name}#{self.job_id:09}'
        self.zip_file_name = f'{self.publish_folder}/{self.capture_file_name}'

        # copy capture_state files to work folder to be included in capture zip package as well
        copy_file_if_exists(f'{self.state_folder}/last_job.log', self.work_folder)

        # compress (make_archive() appends a .zip file extension to zip_file_name)
        self.zip_file_name = shutil.make_archive(self.zip_file_name, format='zip', root_dir=self.work_folder)

        # finish
        self.events.stop('compress', 0, file_size(self.zip_file_name))

    def upload_to_blobstore(self):
        """Upload publish_folder's <dataset_name>-<job_id>.zip to landing blobstore."""

        # don't upload captured data if we're in --notransfer mode
        if self.option('notransfer'):
            logger.warning('Not uploading data to landing per --notransfer option')
            return

        # upload capture file to landing blobstore
        self.events.start('upload', 'step')
        resource = self.config(self.project.blobstore_landing)
        bs_landing = BlobStore()
        bs_landing.connect(resource)
        bs_landing.put(self.zip_file_name, just_file_name(self.zip_file_name))
        bs_landing.disconnect()

        # finish
        self.events.stop('upload', 0, file_size(self.zip_file_name))

    def save_recovery_state_file(self):

        # don't upload captured data if we're in --notransfer mode
        if self.option('notransfer'):
            return

        # Future: Save recovery file in capture.zip file and have archive extract and push back to dataset folder.
        # This way capture_state.zip is only updated AFTER its container file has been successfully archived.

        # create recovery state archive file as dataset_<nnnn>.zip
        # Note: Since this is always the latest state, it does not require a job id in file name.
        recovery_file_name = f'{self.publish_folder}/{self.dataset_name}'
        recovery_file_name = shutil.make_archive(recovery_file_name, format='zip', root_dir=self.state_folder)
        recovery_blob_name = f'capture/{self.dataset_name}.zip'

        # upload capture recovery state file to recovery blobstore
        resource = self.config(self.project.blobstore_recovery)
        bs_recovery = BlobStore()
        bs_recovery.connect(resource)
        bs_recovery.put(recovery_file_name, recovery_blob_name)
        bs_recovery.disconnect()

    def main(self):
        db = None
        try:
            # track dataset name for naming generated files and folders
            self.dataset_name = self.namespace.dataset

            # get job id and table history
            job_history_file_name = f'{self.state_folder}/capture.job'
            job_history = JobHistory(job_history_file_name)
            job_history.load()
            job_id = job_history.job_id
            self.job_id = job_id
            logger.info(f'\nCapture job {job_id} for {self.dataset_name} ...')
            self.progress_message(f'starting job {job_id} ...')

            # track job (and table) metrics
            dataset_id = self.namespace.dataset
            self.events = Events(f'{self.work_folder}/job.log', dataset_id=dataset_id, job_id=job_id)
            self.events.start('capture', 'job')

            # track overall job row count and file size
            self.job_row_count = 0
            self.job_data_size = 0

            # create/clear job folders
            create_folder(self.state_folder)
            clear_folder(self.work_folder)
            clear_folder(self.publish_folder)

            # connect to source database
            self.database = self.config(self.project.database_source)
            if self.database.platform == 'postgresql':
                db = database.PostgreSQL(self.database)
                db_engine = database.Database('postgresql', db.conn)
            elif self.database.platform == 'mssql':
                db = database.MSSQL(self.database)
                db_engine = database.Database('mssql', db.conn)
            else:
                raise NotImplementedError(f'Unknown database platform ({self.database.platform})')

            # determine current timestamp for this job's run

            # get current_timestamp() from source database with step back and fast forward logic
            current_timestamp = self.current_timestamp(db_engine)

            # process all tables
            self.events.start('extract', 'step')

            # build dict of table objects indexed by table name
            self.tables = dict()
            for section_name, section_object in self.config.sections.items():
                if section_name.startswith('table:'):
                    table_name = section_name.partition(':')[2]
                    self.tables[table_name] = section_object

            # extract data from each table
            for table_name, table_object in self.tables.items():
                table_history = job_history.get_table_history(table_name)

                # get current_sequence from source database
                if table_object.cdc == 'sequence':
                    current_sequence = db_engine.current_sequence(table_name)
                else:
                    current_sequence = 0

                self.process_table(db, db_engine, self.database.schema, table_name, table_object, table_history, current_timestamp, current_sequence)
            self.events.stop('extract', self.job_row_count, self.job_data_size)

            # save interim job metrics to work_folder before compressing this folder
            self.events.stop('capture', self.job_row_count, self.job_data_size)
            self.events.save()

            # compress work_folder files to publish_folder zip file
            self.compress_work_folder()

            # upload publish_folder zip file
            self.upload_to_blobstore()

            # save final metrics for complete job run
            self.events.stop('capture', self.job_row_count, self.job_data_size)
            self.events.save(f'{self.state_folder}/last_job.log')
            self.events.save()

            # update job_id and table histories
            if not self.option('notransfer'):
                # only save job history if we're transferring data to landing
                job_history.save()

            # compress capture_state and save to capture blobstore for recovery
            self.save_recovery_state_file()

            # update schedule's poll message
            last_job_info = f'last job {self.job_id} on {datetime.datetime.now():%Y-%m-%d %H:%M}'
            schedule_info = f'schedule: {self.schedule}'
            self.schedule.poll_message = f'{script_name()}({self.dataset_name}), {last_job_info}, {schedule_info}'

        # force unhandled exceptions to be exposed
        except Exception:
            logger.exception('Unexpected exception')
            raise

        finally:
            # explicitly close database connection when finished with job
            with contextlib.suppress(Exception):
                db.conn.close()


# main
if __name__ == '__main__':
    # Future: Re-wrap in app.py and daemon.py (class wrappers for NT Service).
    sys.argv.append('')
    daemon = CaptureDaemon()
    daemon.run()
