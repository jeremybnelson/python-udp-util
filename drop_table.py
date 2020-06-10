#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
drop_table.py

TODO: 2019-04-25 Need to skip table in the capture set that breaks; then pick up.

drop_table <dataset-id>.<table-name>
- load <dataset-id>/state/capture.job (json)
- del tables[ <table-name> ]
- save <dataset-id>/state/capture.job (json)
- load udp
- db.drop_table( <dataset-id>, <table-name> )  - should drop synonyms in future

Does not delete table rows from stat_log (preserves history)

Instructions
* To drop a table: remove [table:<table-name>] from project, then drop_table.py command
* To reload a table (starting from initial load): just drop_table.py command
"""


# standard lib
import logging
import sys


# common lib
from common import clear_folder
from common import create_folder
from common import log_session_info
from common import log_setup
from common import now
from common import script_name


# udp lib
import database


# TODO: JobHistory should be in its own module with compare, difference, drop methods
from capture import JobHistory
from capture import TableHistory


# udp classes
from config import ConfigSectionKey
from option import Option


# module level logger
logger = logging.getLogger(__name__)


class Utility:

    def __init__(self):
        """Generic initialization code."""

        # session folder (create if missing)
        self.session_folder = '../sessions'
        create_folder(self.session_folder)

        # log folder (create if missing)
        self.log_folder = f'{self.session_folder}/logs'
        create_folder(self.log_folder)

        # work folder (create and clear)
        self.work_folder = f'{self.session_folder}/{script_name()}/work'
        clear_folder(self.work_folder)

        # configuration engines
        self.config = None
        self.option = None

        # database
        self.database = None
        self.target_db_conn = None

        # parameter driven
        self.dataset_id = ''
        self.table_name = ''

        # since we start logging before we read config/options we log to known path vs dataset specific path
        log_setup(log_file_name=f'{self.log_folder}/{script_name()}.log')
        log_session_info()

    def run(self):
        """Generic workflow."""
        self.setup()
        self.main()
        self.cleanup()

    def setup(self):
        """Generic setup code."""

        # load standard config
        self.config = ConfigSectionKey('../conf', '../local')
        self.config.load('bootstrap.ini', 'bootstrap')
        self.config.load('init.ini')
        self.config.load('connect.ini')

        # load utility specific options using
        # env variable = UDP_<SCRIPT-NAME>; Option() retrieves command line options
        self.option = Option(f'udp_{script_name()}')

        # create/clear work folder
        self.work_folder = f'{self.session_folder}/{script_name()}/work'
        create_folder(self.work_folder)

        # display application banner
        # TODO: This should be a banner method()
        print(f'UDP {script_name()} utility')
        print(f'Alterra Unified Data Platform')
        copyright_year = f'{now():%Y}'
        copyright_message = f'Copyright (c) 2018-{copyright_year} Alterra Mountain Company, Inc.'
        print(copyright_message)

    def main(self):
        """Subclass with application specific logic."""
        pass

    def cleanup(self):
        """Subclass with application specific logic."""
        pass


class DropTableUtility(Utility):

    def main(self):
        if len(sys.argv) == 1:
            dataset_table_parameter = ''
        else:
            dataset_table_parameter = sys.argv[1].lower()

        if not dataset_table_parameter:
            print('\nError: Must specify <dataset>.<table> to drop.\n')
        else:
            # TODO: Rename dataset_name > dataset_id
            # parse out dataset and table name
            dataset_name, _, table_name = dataset_table_parameter.partition('.')

            # load dataset's capture.job
            job_history_file_name = f'{self.session_folder}/{dataset_name}/state/capture.job'
            job_history = JobHistory(job_history_file_name)
            print(f'job_history_file_name = {job_history_file_name}')
            job_history.load()
            job_history.dump()
            if table_name not in job_history.tables:
                print(f'Error: Table does not exist ({dataset_table_parameter})')
            else:
                # delete table entry
                del job_history.tables[table_name]
                job_history.save(is_maintenance=True)

            # TODO: Read a consistent udp_stage database key
            # TODO: App/utility validation should be a method that validates parameters, connections, dataset/table existences
            # TODO: Print and error should be methods()
            # TODO: Log start/stop, status (success, fail), duration
            # TODO: Database config should drive database.<engine> vs hard coding these statements
            # TODO: Test mode - verifies dataset.table exists, returns error code 0 if yes, 1 if no
            # TODO: --help mode with descriptive text in a Linux output format
            db_resource = self.config('database:amc_dsg_udp_stage_test')
            db = database.MSSQL(db_resource)
            self.target_db_conn = database.Database('mssql', db.conn)
            self.target_db_conn.use_database('udp_stage')

            # drop the target table
            if not self.target_db_conn.does_table_exist(dataset_name, table_name):
                print(f'Error: Table does not exist ({dataset_table_parameter})')
            else:
                print(f'Dropping {dataset_table_parameter} ...')
                self.target_db_conn.drop_table(dataset_name, table_name)
                if self.target_db_conn.does_table_exist(dataset_name, table_name):
                    print(f'Failure: Unable to drop table ({dataset_table_parameter})')
                else:
                    print(f'Successfully dropped {dataset_table_parameter}')

                    # optionally remove the table from stat_log history
                    if self.option('clear-history'):
                        print(f'Clearing {dataset_table_parameter} history from job log ...')
                        # TODO: delete from stat_log where dataset ..., table ..., step ...

            # cleanup
            db.conn.close()


# main
if __name__ == '__main__':
    app = DropTableUtility()
    app.run()
