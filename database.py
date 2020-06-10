#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
database.py

Database wrappers for:
- database platform specific connections
- generic database functionality

TODO: Refactor into database_connection and sql modules ???

Database platform specific SQL logic in <database>.sql files.

FUTURE:
- Make cursor and version code more generic.
- Wrap cursor.execute() to use a generic parameter syntax and optionally log generated SQL and execution times.
- Read property objects to load server, database, etc properties.
- Only expose a high level cursor, fetchone|many|all, executemany() ???
"""

# standard lib
import logging
import pickle


# common lib
from common import expand
from common import log_setup
from common import log_session_info
from common import quote


# udp classes
from config import ConfigSection
from config import ConfigSectionKey


# udp specific lib
import tableschema


# 3rd party lib
import psycopg2
import psycopg2.extensions
import psycopg2.extras
import pyodbc


# module level logger
logger = logging.getLogger(__name__)


class Object:
    pass


class Connection:

    def __init__(self, connection):
        self.platform = connection.platform
        self.queryparm = '?'
        self.driver = connection.driver
        self.host = connection.host
        self.database = connection.database
        self.username = connection.username
        self.password = connection.password
        self.port = connection.port
        self.on_connect = connection.on_connect

        # configuration/version info
        self.client_drivers = ''
        self.client_encoding = ''
        self.client_version = ''
        self.server_encoding = ''
        self.server_version = ''

        self.conn = None
        self.cursor = None
        self.connect()

        # log diagnostic info
        self.check_version()

        # client side properties
        logger.info(f'{self.platform}.client_version: {self.client_version}')
        if self.client_drivers:
            logger.info(f'{self.platform}.client_drivers: {self.client_drivers}')
        if self.client_encoding:
            logger.info(f'{self.platform}.client_encoding: {self.client_encoding}')

        # server side properties
        logger.info(f'{self.platform}.server_version: {self.server_version}')
        if self.server_encoding:
            logger.info(f'{self.platform}.server_encoding: {self.server_encoding}')

    def check_version(self):
        """Subclass for connection specific properties."""
        pass

    def connect(self):
        """Subclass for connection specific details."""
        pass


class MSSQL(Connection):

    def check_version(self):
        self.client_version = f'pyodbc {pyodbc.version}'
        self.client_drivers = f'{pyodbc.drivers()}'

        cursor = self.conn.cursor()
        cursor.execute('select @@version;')
        row = cursor.fetchone()
        if row:
            self.server_version = row[0].splitlines()[0]

    def connect(self):
        self.platform = 'mssql'
        self.queryparm = '?'
        pyodbc.lowercase = True

        conn_properties = list()

        # if you are running on Windows, you can just use the native driver: driver={SQL Server}
        # Note: Use driver={SQL Server} in non-Windows (Linux/MacOS) environments.
        # Note: Latest driver=ODBC Driver 17 for SQL Server;
        if not self.driver:
            # driver = 'ODBC Driver 13 for SQL Server'
            self.driver = '{SQL Server}'

        # enclose driver name in curly braces if curly braces not present in driver value
        if not self.driver.startswith('{'):
            self.driver = f'{{{self.driver}}}'

        conn_properties.append(f'driver={self.driver}')

        # TODO: Make the additional of optional settings a options=  property setting.

        # add MARS (multiple connection) support
        conn_properties.append('MARS_Connection=yes')

        # add ApplicationIntent to leverage read-only Availability Groups in SQL Server 2012+
        # read-only connections can be routed to read-only replicas (if they exist)
        # this setting is compatible with connections that do have Availability Groups
        # ref: https://stackoverflow.com/questions/15347541
        conn_properties.append('ApplicationIntent=ReadOnly')

        # if port override, must provide as part of host name vs "port=" parameter
        if self.port:
            conn_properties.append(f'server={self.host},{self.port}')
        else:
            conn_properties.append(f'server={self.host}')

        # optional database name to open ("use") on _connect
        if self.database:
            conn_properties.append(f'database={self.database}')

        # use trusted_connection -OR- user/password (these are mutually exclusive option)
        if not self.username:
            conn_properties.append('trusted_connection=yes')
        else:
            conn_properties.append(f'uid={self.username}')
            conn_properties.append(f'pwd={self.password}')

        # create the connection
        conn_string = '; '.join(conn_properties)

        # output conn string diagnostics separately so we suppress pwd= value in output
        conn_diagnostics = '; '.join([setting for setting in conn_properties if 'pwd=' not in setting])
        logger.info(f'MSSQL(pyodbc): {conn_diagnostics}')

        # connect
        self.conn = pyodbc.connect(conn_string)
        self.conn.autocommit = False
        self.cursor = self.conn.cursor()

        # enable fast_executemany option
        # Ref: https://stackoverflow.com/questions/29638136/speed-up-bulk-insert-to-mssql-server
        self.cursor.fast_executemany = True

        # support on_connect options like Siriusware "open symmetric key PII decryption by certificate Sirius01;"
        if self.on_connect:
            self.cursor.execute(self.on_connect)


class PostgreSQL(Connection):

    def check_version(self):
        self.client_drivers = None
        self.client_encoding = self.conn.get_parameter_status('client_encoding')
        self.client_version = f'psycopg2 {psycopg2.__version__}'

        # DateStyle, TimeZone, integer_datetimes, standard_conforming_strings
        self.server_encoding = self.conn.get_parameter_status('server_encoding')

        cursor = self.conn.cursor()
        cursor.execute('select version();')
        row = cursor.fetchone()
        if row:
            self.server_version = row[0]

    def connect(self):
        self.platform = 'postgresql'
        self.queryparm = '%s'

        # default PostgreSQL port
        if not self.port:
            self.port = 5432

        # ssl=true
        conn_properties = list()
        conn_properties.append(f'host={self.host}')
        conn_properties.append(f'dbname={self.database}')
        conn_properties.append(f'user={self.username}')
        conn_properties.append(f'password={self.password}')
        conn_properties.append(f'port={self.port}')

        # create the connection
        conn_string = ' '.join(conn_properties)

        # output conn string diagnostics separately so we suppress password= value in output
        conn_diagnostics = '; '.join([setting for setting in conn_properties if 'password=' not in setting])
        logger.info(f'PostgreSQL(psycopg2): {conn_diagnostics}')

        # connect
        self.conn = psycopg2.connect(conn_string)
        self.conn.autocommit = False
        self.conn.cursor_factory = psycopg2.extras.NamedTupleCursor
        self.cursor = self.conn.cursor()


"""
Python DB API-compliance: auto-commit is off by default. You need to call conn.commit to commit any pending transaction.
Connections (and cursors) are context managers, you can simply use the with statement to automatically commit/rollback a 
transaction on leaving the context.

When a connection exits the with block, if no exception has been raised by the block, the transaction is committed. 
If an exception was raised, then the transaction is rolled back.
When a cursor exits the with block it is closed, releasing any resources associated with it.

# start a transaction and create a cursor
with conn, conn.cursor() as cursor: cursor.execute(sql)
"""


class Database:

    def __init__(self, platform, conn):
        self.platform = platform
        self.conn = conn
        self.cursor = self.conn.cursor()
        self.sql = ConfigSection('../conf', '../local')
        self.sql.load(f'{platform}.cfg')
        # self.sql.dump()

        # TODO: This should come in another way
        if platform == 'postgresql':
            self.queryparm = '%s'
        else:
            self.queryparm = '?'

    @staticmethod
    def log(command_name, sql):
        single_line_sql = sql.replace('\n', r'\n')
        logger.debug(f'sql({command_name}): {single_line_sql}')

    # def sql(self, command):
    # 	return self.sql_config.sections[command]

    def is_null(self, sql_command):
        # Note: referenced in embedded f-string

        # noinspection PyUnusedLocal
        # Note: command_name used in embedded f-string.
        command_name = 'is_null'
        self.log(command_name, sql_command)

        self.cursor.execute(sql_command)
        row = self.cursor.fetchone()
        if row:
            # print(f'not_null(row[0]) = {row[0]}')
            return row[0] is None
        else:
            # print(f'not_null() - no row')
            return True

    def execute(self, command_name, value=None):
        # noinspection PyUnusedLocal
        queryparm = self.queryparm
        sql_template = self.sql(command_name)
        sql_command = expand(sql_template)
        if value is None:
            cursor = self.cursor.execute(sql_command)
        else:
            cursor = self.cursor.execute(sql_command, value)
        self.log(command_name, sql_command)
        return cursor

    # noinspection PyUnusedLocal
    def timestamp_literal(self, timestamp_value):
        timestamp_str = f'{current_timestamp:%Y-%m-%d %H:%M:%S}'
        command_name = 'timestamp_literal'
        sql_template = self.sql(command_name)

        # we evaluation expression in Python vs via database engine
        return expand(sql_template)

    # noinspection PyUnusedLocal
    def current_timestamp(self, timezone=None):
        command_name = 'current_timestamp'
        sql_template = self.sql(command_name)
        sql_command = expand(sql_template)
        self.cursor.execute(sql_command)
        return self.cursor.fetchone()[0]

    def current_rowversion(self, table_name):
        # Based on MS RowVersion CDC.
        raise NotImplementedError(f'MS RowVersion CDC not supported yet ({table_name})')

    # noinspection PyUnusedLocal
    def current_sequence(self, table_name):
        # Note: Based on Siriusware proprietary CDC vs MS RowVersion CDC.
        command_name = 'current_sequence'
        sql_template = self.sql(command_name)
        sql_command = expand(sql_template)
        self.log(command_name, sql_command)
        return self.cursor.fetchone()[0]

    # noinspection PyUnusedLocal
    def does_database_exist(self, database_name):
        command_name = 'does_database_exist'
        sql_template = self.sql(command_name)
        sql_command = expand(sql_template)
        self.log(command_name, sql_command)
        return not self.is_null(sql_command)

    def create_database(self, database_name):
        command_name = 'create_database'
        if not self.does_database_exist(database_name):
            autocommit = self.conn.autocommit
            self.conn.autocommit = True
            sql_template = self.sql(command_name)
            sql_command = expand(sql_template)
            self.log(command_name, sql_command)
            self.cursor.execute(sql_command)
            self.conn.autocommit = autocommit

    # noinspection PyUnusedLocal
    # Note: database_name used in embedded f-strings.
    def use_database(self, database_name):
        command_name = 'use_database'
        sql_template = self.sql('use_database')
        sql_command = expand(sql_template)
        self.log(command_name, sql_command)
        self.cursor.execute(sql_command)

    # noinspection PyUnusedLocal
    # Note: schema_name used in embedded f-string
    def does_schema_exist(self, schema_name):
        command_name = 'does_schema_exist'
        sql_template = self.sql(command_name)
        sql_command = expand(sql_template)
        self.log(command_name, sql_command)
        return not self.is_null(sql_command)

    def create_schema(self, schema_name):
        command_name = 'create_schema'
        if not self.does_schema_exist(schema_name):
            autocommit = self.conn.autocommit
            self.conn.autocommit = True
            sql_template = self.sql(command_name)
            sql_command = expand(sql_template)
            self.log(command_name, sql_command)
            self.cursor.execute(sql_command)
            self.conn.autocommit = autocommit

    # noinspection PyUnusedLocal
    # Note: schema_name, table_name used in embedded f-strings.
    # Note: Treats views as tables.
    def does_table_exist(self, schema_name, table_name):
        command_name = 'does_table_exist'
        sql_template = self.sql(command_name)
        sql_command = expand(sql_template)
        self.log(command_name, sql_command)
        return not self.is_null(sql_command)

    def select_table_schema(self, schema_name, table_name):
        command_name = 'select_table_schema'
        if not self.does_table_exist(schema_name, table_name):
            # print(f'Table does not exist: {schema_name}.{table_name}')
            return None
        else:
            sql_template = self.sql(command_name)
            sql_command = expand(sql_template)
            self.log(command_name, sql_command)
            self.cursor.execute(sql_command)

            # 2018-05-29 - make sure pickled table schema is not tied to database client
            rows = self.cursor.fetchall()
            column_names = [column[0] for column in self.cursor.description]
            columns = []
            for row in rows:
                column = Object()
                columns.append(column)
                for column_name in column_names:
                    value = getattr(row, column_name)
                    setattr(column, column_name, value)

            # return Table(table_name, self.cursor.fetchall())
            return tableschema.TableSchema(table_name, columns)

    def select_table_pk(self, schema_name, table_name):
        """Returns a comma delimited string of sorted pk column names or '' if no pk is defined."""
        command_name = 'select_table_pk'
        if not self.does_table_exist(schema_name, table_name):
            # print(f'Table does not exist: {schema_name}.{table_name}')
            return None
        else:
            sql_template = self.sql(command_name)
            sql_command = expand(sql_template)
            self.log(command_name, sql_command)
            self.cursor.execute(sql_command)
            rows = self.cursor.fetchall()
            if not rows:
                pk_columns = ''
            else:
                pk_columns = sorted([row[0] for row in rows])
                pk_columns = ', '.join(pk_columns)
            return pk_columns

    def create_table_from_table_schema(self, schema_name, table_name, table, extended_definitions=None):
        command_name = 'create_table_from_table_schema'
        if not self.does_table_exist(schema_name, table_name):
            autocommit = self.conn.autocommit
            self.conn.autocommit = True

            # noinspection PyUnusedLocal
            # Note: column_definitions used in embedded f-strings.
            column_definitions = table.column_definitions(extended_definitions)
            sql_template = self.sql(command_name)
            sql_command = expand(sql_template)

            # print(f'create_table_from_table_schema:\n{sql_command}\n')

            self.log(command_name, sql_command)
            self.cursor.execute(sql_command)
            self.conn.autocommit = autocommit

    # TODO: Replace schema_name, table_name with [command_name].
    def create_named_table(self, schema_name, table_name):
        command_name = f'create_named_table_{schema_name}_{table_name}'
        if not self.does_table_exist(schema_name, table_name):
            autocommit = self.conn.autocommit
            self.conn.autocommit = True
            sql_template = self.sql(command_name)
            sql_command = expand(sql_template)
            self.log(command_name, sql_command)
            self.cursor.execute(sql_command)
            self.conn.autocommit = autocommit

    def drop_table(self, schema_name, table_name):
        command_name = 'drop_table'
        if self.does_table_exist(schema_name, table_name):
            autocommit = self.conn.autocommit
            self.conn.autocommit = True
            sql_template = self.sql(command_name)
            sql_command = expand(sql_template)
            self.log(command_name, sql_command)
            self.cursor.execute(sql_command)
            self.conn.autocommit = autocommit

    # applies to session vs global temp tables
    def drop_temp_table(self, table_name):
        command_name = 'drop_temp_table'

        # strip optional leading #'s from table name since our SQL template includes
        # FIX: This means we strip ##global_temp as well as #local_temp

        # noinspection PyUnusedLocal
        table_name = table_name.strip('#')

        autocommit = self.conn.autocommit
        self.conn.autocommit = True
        sql_template = self.sql(command_name)
        sql_command = expand(sql_template)
        self.log(command_name, sql_command)
        self.cursor.execute(sql_command)
        self.conn.autocommit = autocommit

    # noinspection PyUnusedLocal
    # Note: schema_name, table_name used in embedded f-strings.
    def insert_into_table(self, schema_name, table_name, **column_names_values):
        command_name = f'insert_into_table'
        column_names = ', '.join(quote(column_names_values.keys()))
        column_placeholders = ', '.join([self.queryparm] * len(column_names_values))
        column_values = column_names_values.values()
        autocommit = self.conn.autocommit
        self.conn.autocommit = True
        sql_template = self.sql(command_name)
        sql_command = expand(sql_template)
        self.log(command_name, sql_command)
        self.cursor.execute(sql_command, *column_values)
        self.conn.autocommit = autocommit

    # noinspection PyUnusedLocal
    # Note: schema_name, table_name used in embedded f-strings.
    def bulk_insert_into_table(self, schema_name, table_name, table_schema, rows, extended_definitions=None):
        command_name = f'insert_into_table'

        # insert extended column definitions into schema
        if extended_definitions:
            table_schema.column_definitions(extended_definitions)

        column_names = ', '.join(quote(table_schema.columns.keys()))
        # print(f'column_names: {column_names}')

        column_placeholders = ', '.join([self.queryparm] * len(table_schema.columns))
        autocommit = self.conn.autocommit
        self.conn.autocommit = False
        sql_template = self.sql(command_name)
        sql_command = expand(sql_template)
        self.log(command_name, sql_command)
        self.cursor.fast_executemany = True
        row_count = self.cursor.executemany(sql_command, rows)
        self.cursor.commit()
        self.conn.autocommit = autocommit
        return row_count

    # noinspection PyUnusedLocal
    # Note: schema_name, table_name used in embedded f-strings.
    def capture_select(self, schema_name, table_name, column_names, last_timestamp=None, current_timestamp=None):
        command_name = f'capture_select'
        column_names = ', '.join(quote(column_names))

        autocommit = self.conn.autocommit
        if self.platform == 'mssql':
            self.conn.autocommit = True

        sql_template = self.sql(command_name)
        # print(f'\ncapture_select.sql_template:\n{sql_template}\n')
        sql_command = expand(sql_template)
        self.log(command_name, sql_command)
        self.cursor.execute(sql_command)
        if self.platform == 'mssql':
            self.conn.autocommit = autocommit
        return self.cursor

    # noinspection PyUnusedLocal
    # Note: schema_name, table_name used in embedded f-strings.
    def delete_where(self, schema_name, table_name, value):
        command_name = f'delete_where'
        sql_template = self.sql(command_name)
        sql_command = expand(sql_template)
        self.log(command_name, sql_command)
        self.cursor.execute(sql_command)

    # FUTURE:
    # insert into
    # update
    # merge

    '''
    [get_pk]
    [get_pk]
    select {pk_column_name} as pk
    __from {schema_name}.{table_name}
    __where {nk_column_name} = {nk_column_value};

    [insert_pk]
    insert into {schema_name}.{table_name}
    __{column_names}
    __values
    __{column_values};
    '''

    # Future: cache pk requests in a local session dict; pk's will never change once issued
    # noinspection PyUnusedLocal
    # Note: schema_name, table_name used in embedded f-strings.
    def get_pk(self, schema_name, table_name, pk_column_name, nk_column_name, **key_values):
        command_name = f'get_pk'

        pk_conditions = list()
        for key, value in key_values.items():
            pk_conditions.append(f'{key}={value}')
        pk_conditions = ' and '.join(pk_conditions)

        autocommit = self.conn.autocommit
        self.conn.autocommit = True
        sql_template = self.sql(command_name)
        sql_command = expand(sql_template)
        self.log(command_name, sql_command)
        self.cursor.execute(sql_command)
        self.conn.autocommit = autocommit


# test code
def main():
    config = ConfigSectionKey('conf', 'local')
    config.load('bootstrap.ini', 'bootstrap')
    config.load('init.ini')
    config.load('connect.ini')

    # SQL Server
    mssql_connection = config('database:udp_aws_stage_01_datalake')
    db = MSSQL(mssql_connection)

    # cursor = conn.cursor()
    # cursor.execute('select top 10 * from udp_stage.amc_heroku_amp_01_sales.addresses;')
    # rows = cursor.fetchall()
    # for row in rows:
    # 	print(row)
    # print()

    database = Database('mssql', db.conn)
    print(f'SQL Server current timestamp = {database.current_timestamp()}')

    database.create_database('test123')
    database.use_database('test123')
    database.create_schema('test_schema')

    # # test creating udp_admin tables via create_named_table
    # database.use_database('udp_stage')
    # database.create_schema('udp_admin')
    # table = database.select_table_schema('dbo', 'dimproduct')
    # database.create_table_from_table_schema('udp_admin', 'dimproduct2', table)
    # database.create_named_table('udp_admin', 'nst_lookup')
    # database.create_named_table('udp_admin', 'job_log')
    # database.create_named_table('udp_admin', 'table_log')

    # FUTURE:
    # extend create tables with custom udp* columns defined as a block
    # capture: udp_jobid, udp_timestamp, udp_rowversion
    # stage:. udp_pk (identity for non-temp), udp_nk (based on primary key), udp_nstpk, udp_jobpk,
    # TODO: Extra columns for #target (no identity) are slightly different than target (pk default identity).

    """
    # test staging management of arriving archived capture files
    database.use_database('udp_stage')
    database.create_schema('udp_catalog')

    database.drop_table('udp_catalog', 'stage_arrival_queue')
    database.drop_table('udp_catalog', 'stage_pending_queue')

    database.create_named_table('udp_catalog', 'stage_arrival_queue')
    database.create_named_table('udp_catalog', 'stage_pending_queue')

    new_file = dict(archive_file_name='amc_heroku_amp_01_sales-1.zip', job_id=1)
    database.insert_into_table('udp_catalog', 'stage_arrival_queue', **new_file)
    new_file = dict(archive_file_name='amc_heroku_amp_01_sales-2.zip', job_id=2)
    database.insert_into_table('udp_catalog', 'stage_arrival_queue', **new_file)
    new_file = dict(archive_file_name='amc_heroku_amp_01_sales-3.zip', job_id=3)
    database.insert_into_table('udp_catalog', 'stage_arrival_queue', **new_file)

    # new_file = dict(archive_file_name='amc_heroku_amp_01_sales-1.zip')
    # database.insert_into_table('udp_catalog', 'stage_pending_queue', **new_file)

    # any new arrivals that we can process? job_id=1 or next job in sequence?
    cursor = database.execute('select_from_stage_arrival_queue')
    row = cursor.fetchone()
    if row:
        # get object_key we should fetch for staging
        print(f'Found next file to stage: {row}')
        archive_file_name = row.archive_file_name
        job_id = int(archive_file_name.split('.')[0].rsplit('-', 1)[-1])
        namespace = archive_file_name.rsplit('-', 1)[0]
        object_key = f'{namespace}/{archive_file_name}'

        print(f'fetch from archives: {object_key}\n')

        # remove the file from both the arrival and pending queues
        database.execute('delete_from_stage_arrival_queue', archive_file_name)
        database.execute('delete_from_stage_pending_queue', archive_file_name)

        # post the next file in sequence for namespace to pending queue
        next_archive_file_name = f'{namespace}-{job_id+1}.zip'
        next_file = dict(archive_file_name=next_archive_file_name)
        database.insert_into_table('udp_catalog', 'stage_pending_queue', **next_file)
    """

    """
    # Ideas:

    # [create_named_table_udp_catalog_stage_queue]
    # [insert_into_named_table_udp_catalog_stage_queue]
    # [select_from_named_table_udp_catalog_stage_queue]
    # [update_named_table_udp_catalog_stage_queue]

    # namespace, job_id, is_last_staged, is_pending, queued_timestamp
    # archive processes capture file: insert namespace, jobid, 0, 1, now() into stage_queue
    # stage poll - select new jobs in sequence
    # update

    # table.postgresql_to_mssql()
    # table.mssql_to_mssql()
    """

    # PostgreSQL
    postgresql_connection = config('database_amc_heroku_amp_01_sales_prod')
    db = PostgreSQL(postgresql_connection)

    # cursor = conn.cursor()
    # cursor.execute('select * from guests limit 10;')
    # for row in cursor.fetchall():
    # 	print(row.email, row)
    database = Database('postgresql', db.conn)
    # print(f'PostgreSQL current timestamp = {database.current_timestamp()}')

    table_name = 'carts'
    table_1 = database.select_table_schema('public', table_name)
    output_stream = open(f'{table_name}.schema', 'wb')
    pickle.dump(table_1, output_stream)
    output_stream.close()

    input_stream = open(f'{table_name}.schema', 'rb')
    table_2 = pickle.load(input_stream)
    input_stream.close()
    for column in table_2.columns:
        print(table_2.columns[column])
        pass


# test code
if __name__ == '__main__':
    log_setup()
    log_session_info()
    main()
