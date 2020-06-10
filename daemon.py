#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
daemon.py

Light-weight daemon framework for all long running processes.
"""


# standard lib
import logging
import sys


# common lib
from common import create_folder
from common import is_file
from common import just_file_stem
from common import log_session_info
from common import log_setup
from common import now
from common import script_name


# udp class
from config import ConfigSectionKey
from option import Option
from schedule import Schedule


# module level logger
logger = logging.getLogger(__name__)


class Daemon:

    def __init__(self, project_file=None):
        # session folder (acts as root path for job specific folders)
        self.session_folder = '../sessions'

        # configuration engines
        self.config = None
        self.option = None

        # project metadata
        self.project = None
        self.namespace = None

        # project resources
        self.database = None
        self.schedule = None

        # project dataset specific working folders
        self.state_folder = None
        self.work_folder = None
        self.publish_folder = None

        # project database connections (db_conn)
        self.source_db_conn = None
        self.target_db_conn = None

        # project file and name
        self.project_file = ''
        self.project_name = ''

        # if optional project file supplied use it; otherwise try command line
        if project_file:
            self.project_file = project_file
        elif len(sys.argv) > 1:
            self.project_file = sys.argv[1]

        # make sure we have a valid project file
        app_name = script_name()
        if not self.project_file:
            print(f'{app_name}: error - must specify project file')
        elif not is_file(f'../conf/{self.project_file}'):
            print(f'{app_name}: error - project file not found ({project_file})')
        else:
            # project file controls configuration
            self.project_name = just_file_stem(self.project_file)

    # noinspection PyMethodMayBeStatic
    def cleanup(self):
        """Override: Optional cleanup code."""
        pass

    def main(self):
        """Override: Main code goes here."""
        pass

    def run(self):
        """
        Options
        --onetime[=1] run once, then exit; use if called by an external scheduler.
        --nowait[=1] run immediately without waiting for scheduler to determine execution.
        """

        # exit if __init__ didn't find a valid project file
        if not self.project_name:
            return

        # display application banner
        app_name = script_name()
        print(f'UDP {app_name.title()} {self.project_name}')
        copyright_year = f'{now():%Y}'
        copyright_message = f'Copyright (c) 2018-{copyright_year} Alterra Mountain Company, Inc.'
        print(copyright_message)

        # make sure root sessions folder exists
        create_folder(self.session_folder)

        # since we start logging before we read config/options we log to known path vs dataset specific path
        log_setup(log_file_name=f'{self.session_folder}/{self.project_name}.log')
        log_session_info()

        # common setup
        self.setup()

        # application specific startup logic
        self.start()

        # scheduling behavior based on --onetime, --nowait option
        if self.option('onetime') == '1':
            # one-time run; use when this script is being called by an external scheduler
            logger.info('Option(onetime=1): executing once')
            self.main()
        else:
            if self.option('nowait') == '1':
                # no-wait option; execute immediately without waiting for scheduler to initiate
                logger.info('Option(nowait=1): executing immediately, then following regular schedule')
                self.main()

            # standard wait for scheduled time slot and run logic
            while True:
                self.progress_message('waiting for next job ...')
                if self.schedule.wait():
                    self.main()
                    if self.option('scheduled_onetime') == '1':
                        logger.info('Option(scheduled_onetime=1): ran once at first scheduled timeslot')
                        break
                else:
                    break

        self.cleanup()

    def setup(self):
        """Generic setup code."""

        # load standard config
        config = ConfigSectionKey('../conf', '../local')
        self.config = config
        config.load('bootstrap.ini', 'bootstrap')
        config.load('init.ini')
        config.load('connect.ini')

        # load project specific config
        self.config.load(self.project_file)
        self.project = self.config('project')

        # load project specific options from optional project specific environ var
        environ_var = just_file_stem(self.project_file).lower()
        self.option = Option(environ_var, options=config('project').options)

        # load project namespace
        self.namespace = self.config('namespace')

        # load project specific schedule
        self.schedule = Schedule(config('schedule'))

        # job specific folders
        self.state_folder = f'{self.session_folder}/{self.namespace.dataset}/state'
        self.work_folder = f'{self.session_folder}/{self.namespace.dataset}/work'
        self.publish_folder = f'{self.session_folder}/{self.namespace.dataset}/publish'

        # diagnostics
        # self.option.dump()
        # self.config('project').dump(False)
        # self.config('schedule').dump(False)

    # noinspection PyMethodMayBeStatic
    def start(self):
        """Override: Optional application specific startup code called after generic setup()."""
        pass

    def progress_message(self, message):
        """Output progress message for current process."""
        timestamp = f'{now():%Y-%m-%d %H:%M:%S}'
        process_name = self.project.type
        dataset_name = self.namespace.dataset
        if process_name == dataset_name:
            dataset_name = ''
        else:
            dataset_name = f'({dataset_name})'
        message_prefix = f'{timestamp} {process_name}{dataset_name}'
        print(f'{message_prefix}: {message}')


# temp test scaffolding ...


# test code
def test():
    pass


# test code
if __name__ == '__main__':
    # daemon class has internal logging setup
    test()
