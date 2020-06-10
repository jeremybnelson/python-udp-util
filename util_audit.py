#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
util_audit.py

Audit Module to validate UDP data
"""

# standard lib
import logging
import xlsxwriter
import datetime
import os
import pathlib

# common lib
from common import expand
from common import log_setup
from common import log_session_info
from common import split
from common import delete_blank_lines

# udp classes
from config import ConfigSectionKey
from database import MSSQL
from database import PostgreSQL
from database import Database
from cdc_select import SelectCDC

# module level logger
logger = logging.getLogger(__name__)


class SelectCDCAudit(SelectCDC):
    """
    This extends the SelectCDC.py class for Audit specific usage
    """

    select_template = '''
		__select
		_  count(*) as row_count
		_  from "{schema_name}"."{table_name}" as "s"
		_  {join_clause}
		_  {where_clause}
		_  {order_clause}
		'''

    timestamp_where_template = '''
		__   (
		_      {timestamp_value} >= '{last_timestamp}'
		_    )
		'''

    def __init__(self, table):
        # indent template text
        self.select_template = indent(self.select_template)
        self.timestamp_where_template = indent(self.timestamp_where_template)

        # object scope properties
        self.table = table
        self.timestamp_value = ''
        self.timestamp_where_condition = ''

    def generate_row_count_timestamp(self):
        self.timestamp_logic(self.table.timestamp, self.table.first_timestamp)

        # these are used in the expand method. Ignore warnings
        schema_name = self.table.schema_name
        table_name = self.table.table_name
        join_clause = self.join_clause()
        where_clause = self.where_clause()
        order_clause = self.order_clause()
        # column_names = self.column_names()
        sql = expand(self.select_template)
        return delete_blank_lines(sql.strip() + ';')


class DatabaseAudit(Database):
    """
    This extends the database.py class for Audit specific usage
    This should be loaded with platform_audit.cfg files
    """

    def select_row_count(self, schema_name, table_name):
        """Returns row count"""
        command_name = 'select_row_count'
        if not self.does_table_exist(schema_name, table_name):
            # print(f'Table does not exist: {schema_name}.{table_name}')
            return None
        else:
            sql_template = self.sql(command_name)
            sql_command = expand(sql_template)
            self.log(command_name, sql_command)
            self.cursor.execute(sql_command)
            row_count = self.cursor.fetchone()
            return row_count

    def select_row_count_timestamp(self, table_object):
        """Returns row count"""
        command_name = 'select_row_count_with_timestamp'
        if not self.does_table_exist(table_object.schema_name, table_object.table_name):
            # print(f'Table does not exist: {schema_name}.{table_name}')
            return None
        else:
            # table_object.schema_name = schema_name
            # table_object.table_name = table_name
            select_cdc = SelectCDCAudit(table_object)
            row_count_timestamp_query = select_cdc.generate_row_count_timestamp()

            # sql_template = self.sql(command_name)
            # sql_command = expand(sql_template)
            # Call SelectCDCAudit select_row_count() to generate and return SQL statement
            self.log(command_name, row_count_timestamp_query)
            self.cursor.execute(row_count_timestamp_query)
            row_count = self.cursor.fetchone()
            return row_count

    def select_nullable_columns(self, schema_name, table_name):
        """Returns nullable columns"""
        command_name = 'select_nullable_columns'
        if not self.does_table_exist(schema_name, table_name):
            # print(f'Table does not exist: {schema_name}.{table_name}')
            return None
        else:
            null_column_list = []
            sql_template = self.sql(command_name)
            sql_command = expand(sql_template)
            self.log(command_name, sql_command)
            self.cursor.execute(sql_command)
            null_columns = self.cursor.fetchall()
            for columns in null_columns:
                null_column_list.append(columns.column.lower())
            return null_column_list

    def select_columns_with_datatype(self, schema_name, table_name):
        """select_columns_with_datatype"""
        command_name = 'select_columns_with_datatype'
        if not self.does_table_exist(schema_name, table_name):
            # print(f'Table does not exist: {schema_name}.{table_name}')
            return None
        else:
            sql_template = self.sql(command_name)
            sql_command = expand(sql_template)
            self.log(command_name, sql_command)
            self.cursor.execute(sql_command)
            columns_with_datatypes = self.cursor.fetchall()

            # Lower all column names in columns_with_datatypes
            # columns_with_datatypes = list(set(column.column.lower() for column in columns_with_datatypes))
            return columns_with_datatypes

    # noinspection PyUnusedLocal
    def select_null_count(self, schema_name, table_name, column_name):
        """Returns null count"""
        # column_name is used when the sql_template is expanded. ignore warning
        command_name = 'select_null_count'
        if not self.does_table_exist(schema_name, table_name):
            # print(f'Table does not exist: {schema_name}.{table_name}')
            return None
        else:
            sql_template = self.sql(command_name)
            sql_command = expand(sql_template)
            self.log(command_name, sql_command)
            self.cursor.execute(sql_command)
            null_count = self.cursor.fetchone()
            return null_count.null_count

    # noinspection PyUnusedLocal
    def select_min_max_len(self, schema_name, table_name, column_name):
        """Returns minimum and maximum length of column"""
        command_name = 'select_min_max_len'
        if not self.does_table_exist(schema_name, table_name):
            # print(f'Table does not exist: {schema_name}.{table_name}')
            return None
        else:
            sql_template = self.sql(command_name)
            sql_command = expand(sql_template)
            self.log(command_name, sql_command)
            self.cursor.execute(sql_command)
            column_detail = self.cursor.fetchone()
            return column_detail

    # noinspection PyUnusedLocal
    def select_min_max_len_cast(self, schema_name, table_name, column_name):
        """Returns minimum and maximum length of column"""
        command_name = 'select_min_max_len_cast'
        if not self.does_table_exist(schema_name, table_name):
            # print(f'Table does not exist: {schema_name}.{table_name}')
            return None
        else:
            sql_template = self.sql(command_name)
            sql_command = expand(sql_template)
            self.log(command_name, sql_command)
            self.cursor.execute(sql_command)
            column_detail = self.cursor.fetchone()
            return column_detail

    # noinspection PyUnusedLocal
    def select_min_max(self, schema_name, table_name, column_name):
        """Returns minimum and maximum values of column"""
        command_name = 'select_min_max'
        if not self.does_table_exist(schema_name, table_name):
            # print(f'Table does not exist: {schema_name}.{table_name}')
            return None
        else:
            sql_template = self.sql(command_name)
            sql_command = expand(sql_template)
            self.log(command_name, sql_command)
            self.cursor.execute(sql_command)
            column_detail = self.cursor.fetchone()
            return column_detail


def iterate_row_count(configs, dbs, workbook, worksheet, h_index=0, v_index=0):
    config_index = 0
    db_index = 0

    # Workbook Formats
    header_format = workbook.add_format({'bold': True, 'underline': True})
    sub_header_format = workbook.add_format({'bold': True})
    percent_format = workbook.add_format({'num_format': '0.00%'})

    # Loop through all configs and all dbs. config[1] is correlated with dbs[1] etc.
    while config_index < len(configs):
        worksheet.write(h_index, v_index, configs[config_index]('namespace').dataset, header_format)
        worksheet.write(h_index, v_index + 1, 'Row Count', header_format)
        worksheet.write(h_index, v_index + 2, 'UDP Row Count', header_format)
        worksheet.write(h_index, v_index + 3, 'Row Count Difference', header_format)
        worksheet.write(h_index, v_index + 4, 'Row Count % Difference', header_format)
        h_index += 1

        for table in (t for t in configs[config_index].sections if 'table:' in t):
            ignore_flag = configs[config_index](table).ignore_table
            if ignore_flag == '1':
                table_name = table.partition(':')[2]
                # write the table name
                worksheet.write(h_index, v_index, table_name, sub_header_format)
                # write the row count for respective table
                worksheet.write(h_index, v_index + 1, 'Table Ignored')
            else:
                # table_name = table.partition(':')[2]
                # timestamp_column = configs[config_index](table).timestamp
                # first_timestamp = configs[config_index](table).first_timestamp

                # create table config object
                # reference: for table_name, table_object in self.table_config.sections.items():
                table_object = configs[config_index](table)
                table_object.table_name = table.partition(':')[2]
                connect_config = configs[config_index]('project').database_source
                table_object.schema_name = configs[config_index](connect_config).schema

                if table_object.cdc == 'none':
                    src_row_count = dbs[db_index].select_row_count(table_object.schema_name, table_object.table_name)
                else:
                    src_row_count = dbs[db_index].select_row_count_timestamp(table_object)

                # src_row_count = dbs[db_index].select_row_count_timestamp(dbs[db_index].schema, table_name, timestamp_column, first_timestamp)

                # write the table name
                worksheet.write(h_index, v_index, table_object.table_name, sub_header_format)

                # write the row count for respective table
                worksheet.write(h_index, v_index + 1, src_row_count.row_count)

                # UDP Schema
                target_schema = configs[config_index]('namespace').dataset
                # target_schema = f'dataset_{target_schema_num}'
                # UDP Databased
                target_db = dbs[len(dbs) - 1]
                target_row_count = target_db.select_row_count(target_schema, table_object.table_name)

                if target_row_count is None:
                    worksheet.write(h_index, v_index + 2, 'Table Not Found')
                else:
                    worksheet.write(h_index, v_index + 2, target_row_count.row_count)
                    # write dif and percentage functions
                    worksheet.write_formula(h_index, v_index + 3, f'=B{h_index+1} - C{h_index+1}')
                    worksheet.write_formula(h_index, v_index + 4,
                                            f'=IF(AND(B{h_index+1}=0,'f'C{h_index+1}=0),"100",'f'C{h_index+1}/B{h_index+1} *100)&"%"',
                                            percent_format)

            # this must be last
            h_index += 1

        h_index += 1
        db_index += 1
        config_index += 1


def iterate_null_columns(configs, dbs, workbook, worksheet, h_index=0, v_index=0):
    config_index = 0
    db_index = 0

    # Workbook Formats
    header_format = workbook.add_format({'bold': True, 'underline': True})
    sub_header_format = workbook.add_format({'bold': True})
    percent_format = workbook.add_format({'num_format': '0.00%'})

    # Loop through all configs and all dbs. config[1] is correlated with dbs[1] etc.
    while config_index < len(configs):
        worksheet.write(h_index, v_index, configs[config_index]('namespace').dataset, header_format)
        worksheet.write(h_index, v_index + 1, 'Null Count', header_format)
        worksheet.write(h_index, v_index + 2, 'UDP Null Count', header_format)
        worksheet.write(h_index, v_index + 3, 'Row Count Difference', header_format)
        worksheet.write(h_index, v_index + 4, 'Row Count % Difference', header_format)
        h_index += 1

        # Set up UDP variables
        target_schema = configs[config_index]('namespace').dataset
        target_db = dbs[len(dbs) - 1]

        # Loop through all tables in configs
        # add logic to ignore tables where ignore_flag = 1
        for table in (t for t in configs[config_index].sections if 'table:' in t):
            table_name = table.partition(':')[2]
            src_null_columns = dbs[db_index].select_nullable_columns(dbs[db_index].schema, table_name)
            worksheet.write(h_index, v_index, table_name, sub_header_format)

            # remove ignored columns from src_null_columns list
            ignore_columns = split(configs[config_index](table).ignore_columns)

            # ToDo: Switch this to support Glob Pattern (ex: first_*)
            if ignore_columns:
                for column in ignore_columns:
                    if column.lower().strip() in src_null_columns:
                        src_null_columns.remove(column.lower().strip())

            # Gap between tables
            h_index += 1

            # Loop through all nullable columns
            for null_column in src_null_columns:
                # ignore_flag = configs[config_index](table).ignore_columns
                worksheet.write(h_index, v_index, null_column)

                src_null_count = dbs[db_index].select_null_count(dbs[db_index].schema, table_name, null_column)
                target_null_count = target_db.select_null_count(target_schema, table_name, null_column)
                # write source null count
                worksheet.write(h_index, v_index + 1, src_null_count)
                if target_null_count is None:
                    worksheet.write(h_index, v_index + 2, 'Column Not Found')
                else:
                    # write udp null count
                    worksheet.write(h_index, v_index + 2, target_null_count)
                    # write excel functions
                    worksheet.write_formula(h_index, v_index + 3, f'=B{h_index+1} - C{h_index+1}')
                    worksheet.write_formula(h_index, v_index + 4,
                                            f'=IF(AND(B{h_index+1}=0,'f'C{h_index+1}=0),"100",'f'C{h_index+1}/B{h_index+1} *100)&"%"',
                                            percent_format)
                h_index += 1

        h_index += 1
        db_index += 1
        config_index += 1


def iterate_column_min_max(configs, dbs, workbook, worksheet, h_index=0, v_index=0):
    config_index = 0
    db_index = 0

    # Workbook Formats
    header_format = workbook.add_format({'bold': True, 'underline': True})
    sub_header_format = workbook.add_format({'bold': True})
    percent_format = workbook.add_format({'num_format': '0.00%'})

    # Loop through all configs and all dbs. config[1] is correlated with dbs[1] etc.
    while config_index < len(configs):
        worksheet.write(h_index, v_index, configs[config_index]('namespace').dataset, header_format)
        worksheet.write(h_index, v_index + 1, 'Source Column Min Length', header_format)
        worksheet.write(h_index, v_index + 2, 'Target Column Min Length', header_format)
        worksheet.write(h_index, v_index + 3, 'Source Column Max Length', header_format)
        worksheet.write(h_index, v_index + 4, 'Target Column Max Length', header_format)
        h_index += 1

        # Set up UDP variables
        target_schema = configs[config_index]('namespace').dataset
        target_db = dbs[len(dbs) - 1]

        # Loop through all tables in configs
        # add logic to ignore tables where ignore_flag = 1
        for table in (t for t in configs[config_index].sections if 'table:' in t):
            table_name = table.partition(':')[2]
            src_columns = dbs[db_index].select_columns_with_datatype(dbs[db_index].schema, table_name)

            # Lower all fields in src_columns
            # src_columns = [column.column.lower() for column in src_columns]

            # Write table name
            worksheet.write(h_index, v_index, table_name, sub_header_format)

            # remove ignored columns from src_null_columns list
            # ToDo: Use the split() method from Common.py instead
            ignore_columns = split(configs[config_index](table).ignore_columns)

            # MMG: be "truthy"
            #if ignore_columns[0] == '':
            if not ignore_columns:
                final_src_columns = src_columns
            else:
                final_src_columns = list()
                ignore_columns = [column.lower() for column in ignore_columns]
                for column_desc in src_columns:
                    if column_desc.column.lower() not in ignore_columns:
                        final_src_columns.append(column_desc)

            # Increment each column
            h_index += 1
            # print(final_src_columns)
            # Loop through all columns
            for column in final_src_columns:
                if column.data_type in ('char', 'nchar', 'nvarchar', 'varchar'):
                    worksheet.write(h_index, v_index, column.column)

                    src_column_min_max = dbs[db_index].select_min_max_len(dbs[db_index].schema, table_name,
                                                                          column.column)
                    target_column_min_max = target_db.select_min_max_len(target_schema, table_name, column.column)

                    worksheet.write(h_index, v_index + 1, src_column_min_max.min)
                    worksheet.write(h_index, v_index + 2, target_column_min_max.min)
                    worksheet.write(h_index, v_index + 3, src_column_min_max.max)
                    worksheet.write(h_index, v_index + 4, target_column_min_max.max)

                    h_index += 1

                else:
                    worksheet.write(h_index, v_index, column.column)
                    if column.data_type == 'bit':
                        src_column_min_max = dbs[db_index].select_min_max_len_cast(dbs[db_index].schema, table_name,
                                                                                   column.column)
                        target_column_min_max = target_db.select_min_max_len_cast(target_schema, table_name,
                                                                                  column.column)
                    else:
                        src_column_min_max = dbs[db_index].select_min_max(dbs[db_index].schema, table_name,
                                                                          column.column)
                        target_column_min_max = target_db.select_min_max(target_schema, table_name, column.column)

                    if column.data_type in ('date', 'datetime', 'datetime2', 'datetime3', 'smalldatetime'):
                        if src_column_min_max.min is None:
                            # This means the table is empty.
                            pass
                        else:
                            worksheet.write_datetime(h_index, v_index + 1, src_column_min_max.min)
                            worksheet.write_datetime(h_index, v_index + 2, target_column_min_max.min)
                            worksheet.write_datetime(h_index, v_index + 3, src_column_min_max.max)
                            worksheet.write_datetime(h_index, v_index + 4, target_column_min_max.max)
                    else:
                        worksheet.write(h_index, v_index + 1, src_column_min_max.min)
                        worksheet.write(h_index, v_index + 2, target_column_min_max.min)
                        worksheet.write(h_index, v_index + 3, src_column_min_max.max)
                        worksheet.write(h_index, v_index + 4, target_column_min_max.max)

                    h_index += 1

        h_index += 1
        db_index += 1
        config_index += 1


def indent(text):
    """Protect logical indentation of indented multi-line text values."""
    output = []
    for line in text.strip().splitlines():
        line = line.strip()
        if line.startswith('_ '):
            line = line[1:]
        elif line.startswith('__'):
            line = line[2:].strip()
        output.append(line)
    return '\n'.join(output)


# Everything starts here
def main():
    # initialize the dict that will hold all config objects
    config_list = []
    db_list = []

    # Detect and load audit config files into a dict of config objects
    # ToDo: Additional logic needed to include project files in local dir
    for project_file in sorted(pathlib.Path('../conf/').glob('project_capture*')):
        config = ConfigSectionKey('../conf', '../local')
        config.load('connect.ini')
        config.load(project_file)
        # conn_config drills through the project file to the connect.ini and returns a DatabaseSection object
        conn_config = config(config('project').database_source)

        if conn_config.platform == 'mssql':
            db_conn = MSSQL(conn_config)
            db = DatabaseAudit(f'{conn_config.platform}_audit', db_conn.conn)
            db.schema = conn_config.schema

        elif conn_config.platform == 'postgresql':
            db_conn = PostgreSQL(conn_config)
            db = DatabaseAudit(f'{conn_config.platform}_audit', db_conn.conn)
            db.schema = conn_config.schema
        else:
            print("platform not found. Config file's incorrectly set up")
        # add db and config objects to respective lists based on project_capture_* files
        config_list.append(config)
        db_list.append(db)

    # add target_db to the db_list[]
    target_conn_config = config('database:amc_dsg_udp_01_stage_dev')
    target_db_conn = MSSQL(target_conn_config)
    target_db = DatabaseAudit('mssql_audit', target_db_conn.conn)
    target_db.use_database('udp_stage_dev')
    db_list.append(target_db)

    # Excel Logic
    environment = 'dev'
    time = datetime.datetime.now()
    file_name = f'''..\output\Audit_Results_{environment}_{time:%Y-%m-%d}.xlsx'''

    # create workbook and worksheets
    workbook1 = xlsxwriter.Workbook(file_name)
    worksheet1 = workbook1.add_worksheet('Table Overview')
    worksheet2 = workbook1.add_worksheet('Column Overview')
    worksheet3 = workbook1.add_worksheet('Column Detail')

    # Start the magic
    iterate_row_count(config_list, db_list, workbook1, worksheet1)
    # iterate_null_columns(config_list, db_list, workbook1, worksheet2)
    # iterate_column_min_max(config_list, db_list, workbook1, worksheet3)

    # start it up
    workbook1.close()
    os.startfile(file_name)


if __name__ == '__main__':
    log_setup(log_level=logging.DEBUG)
    log_session_info()
    main()
