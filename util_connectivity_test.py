#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
audit.py

Connectivity Utility to test connection and permissions across UDP footprint
"""

# standard lib
import logging
import xlsxwriter
import datetime
import os
import pathlib

# common lib
from common import log_setup
from common import log_session_info
from common import full_path


# udp classes
from config import ConfigSectionKey
from database import MSSQL
from database import PostgreSQL
from util_audit import DatabaseAudit

# module level logger
logger = logging.getLogger(__name__)


def test_connectivity(config, db):
    dataset = config('namespace').dataset
    entity = config('namespace').entity
    location = config('namespace').location
    sdlc = config('namespace').sdlc
    subject = config('namespace').subject
    system = config('namespace').system

    for table in (t for t in config.sections if 'table:' in t):
        # table_object = config(table)
        table_name = table.partition(':')[2]
        try:
            db.select_row_count(db.schema, table_name)
            print(f'{dataset}.{table_name} Success')
        except:
            print(f'{dataset}.{table_name} Failed')

    # db.select_table_pk(db.schema,)
    print(f'{dataset}|{entity}_{system}_{subject}_{sdlc}')


def main():
    # Excel Logic
    time = datetime.datetime.now()
    file_name = f'''..\output\Connection_Results_{time:%Y-%m-%d}.xlsx'''

    # create workbook and worksheets
    workbook = xlsxwriter.Workbook(file_name)
    worksheet1 = workbook.add_worksheet('Table Overview')

    # Workbook Formats
    header_format = workbook.add_format({'bold': True, 'underline': True})
    failure_format = workbook.add_format({'font_color': 'red', 'bold': True})
    success_format = workbook.add_format({'font_color': 'green', 'bold': True})
    undefined_format = workbook.add_format({'font_color': 'orange', 'bold': True})

    # Write Headers
    worksheet1.write(0, 0, 'Database', header_format)
    worksheet1.write(0, 1, 'Connection Result', header_format)

    # Config logic
    config = ConfigSectionKey('../conf', '../local')
    config.load('connect.ini')

    # set y index
    row_index = 1

    for database in sorted((db for db in config.sections if 'database:' in db)):
        db_name = database.partition(':')[2]
        db_config = config(database)

        worksheet1.write(row_index, 0, db_name)
        if not db_config.database:
            worksheet1.write(row_index, 1, 'Connection Undefined', undefined_format)

        if db_config.platform == 'mssql':
            try:
                db_conn = MSSQL(db_config)
                print(f'{db_name}: Success')
                worksheet1.write(row_index, 1, 'Success', success_format)
            except Exception as e:
                print(f'{db_name}: Failed')
                worksheet1.write(row_index, 1, 'Failed', failure_format)
                worksheet1.write(row_index, 2, str(e))

        elif db_config.platform == 'postgresql':
            try:
                db_conn = PostgreSQL(db_config)
                print(f'{database}: Success')
                worksheet1.write(row_index, 1, 'Success', success_format)
            except Exception as e:
                print(f'{database}: Failed')
                worksheet1.write(row_index, 1, 'Failed', failure_format)
                worksheet1.write(row_index, 2, str(e))

        row_index += 1
    # start it up
    workbook.close()
    file_full_path = full_path(file_name)
    os.startfile(file_full_path)


if __name__ == '__main__':
    log_setup(log_level=logging.DEBUG)
    log_session_info()
    main()
