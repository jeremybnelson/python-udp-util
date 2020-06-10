#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
stage.py

Monitor arrival and pending queues to determine when a new data update has arrived.

Depending on CDC type of table:
- drop (vs truncate) and reload table
- upsert (SQL Server merge) changes to target table
"""


# standard lib
import glob
import logging
import pathlib
import zipfile


# common lib
from common import clear_folder
from common import is_file
from common import just_file_name
from common import just_file_stem
from common import load_jsonpickle
from common import load_text
from common import now


# udp lib
import cdc_merge
import database
import tableschema
import udp


# udp classes
from blobstore import BlobStore
from daemon import Daemon


# 3rd party lib
# import arrow


# module level logger
logger = logging.getLogger(__name__)


def convert_data_types(rows, table_schema):
    for column_index, column in enumerate(table_schema.columns.values()):
        # print(f'{column.column_name} ({column.data_type}) (index {column_index})')

        # convert all date, datetime, time strings to datetime values
        # if column.data_type in ('date', 'datetime', 'datetime2', 'smalldatetime', 'time'):
        # 	# print(f'Converting {column.column_name} ({column.data_type}) (index {column_index}) to datetime')
        # 	for row in rows:
        # 		if row[column_index] is not None:
        # 			# shorten high precision values to avoid ODBC Datetime field overflow errors
        # 			if len(row[column_index]) > 23:
        # 				row[column_index] = row[column_index][0:25]
        # 			row[column_index] = arrow.get(row[column_index]).datetime

        # make sure nvarchar are really strings
        if column.data_type == "nvarchar":
            # print(f'Converting {column.column_name} ({column.data_type}) (index {column_index}) to str')
            for row in rows:
                if row[column_index] is not None:
                    row[column_index] = str(row[column_index])


"""
ARRAY: cast(<column> as text)
BIGINT
BOOLEAN: cast(<column> as integer)
CHARACTER VARYING
DATE
INTEGER
JSONB: cast(<column> as text)
TEXT
TIMESTAMP WITHOUT TIME ZONE
USER DEFINED: cast(<column> as text)
UUID: cast(<column> as text)
"""


def convert_to_mssql(table_schema, extended_definitions=None):
    # add extended definitions if present
    if extended_definitions:
        for definition in extended_definitions:
            column = tableschema.Column()
            column.column_name = definition.split()[0]
            column.data_type = definition.split()[1]
            table_schema.columns[column.column_name] = column

    for column_name, column in table_schema.columns.items():
        data_type = column.data_type.lower()

        if data_type == "array":
            column.data_type = "nvarchar"
            column.character_maximum_length = 512

        elif data_type == "bigint":
            column.data_type = "bigint"

        elif data_type == "boolean":
            column.data_type = "tinyint"

        elif data_type == "character varying":
            column.data_type = "nvarchar"
            column.character_maximum_length = 768

        # PostgreSQL/SQL Server
        elif data_type == "date":
            column.data_type = "date"

        # override deprecated datetime
        elif data_type == "datetime":
            column.data_type = "datetime2"

        elif data_type == "integer":
            column.data_type = "int"

        elif data_type == "jsonb":
            column.data_type = "nvarchar"
            column.character_maximum_length = -1

        elif data_type in ("money", "smallmoney"):
            column.data_type = "decimal"
            column.numeric_precision = 18
            column.numeric_scale = 4

        # PostgreSQL/SQL Server
        elif data_type == "text":
            column.data_type = "nvarchar"
            column.character_maximum_length = -1

        elif data_type == "timestamp without time zone":
            column.data_type = "datetime2"
            column.datetime_precision = 7

        elif data_type in ("user defined", "user-defined"):
            column.data_type = "nvarchar"
            column.character_maximum_length = 128

        elif data_type == "uuid":
            column.data_type = "nvarchar"
            column.character_maximum_length = 36

        else:
            # pass it through as native sql server type
            # int (all sizes), decimal/numeric, float/real
            pass


class StageDaemon(Daemon):

    """Daemon class integrates core config, option, and schedule functionality."""

    def start(self):
        super().start()

        # make sure core database environment in place
        udp.setup(self.config)

    def stage_file(self, archive_capture_file_name):
        logger.info(f"Getting {archive_capture_file_name} from archive blob store")

        # make sure work folder exists and is empty
        clear_folder(self.work_folder)

        # connect to the archive blobstore
        resource = self.config(self.project.blobstore_archive)
        bs_archive = BlobStore()
        bs_archive.connect(resource)

        # extract dataset name and job id from archive capture file name
        dataset_name, _, job_id = just_file_stem(archive_capture_file_name).partition(
            "#"
        )

        # copy archive_capture_file_name to our local working folder
        capture_file_name = just_file_name(archive_capture_file_name)
        local_work_file_name = f"{self.work_folder}/{capture_file_name}"
        archive_capture_file_blob_name = f"{archive_capture_file_name}"
        bs_archive.get(local_work_file_name, archive_capture_file_blob_name)
        bs_archive.disconnect()

        # unzip the capture file we retrieved from archive
        with zipfile.ZipFile(local_work_file_name) as zf:
            zf.extractall(self.work_folder)

        # create the file's dataset_name schema if missing
        self.target_db_conn.create_schema(dataset_name)

        # process all table files in our work folder
        for file_name in sorted(glob.glob(f"{self.work_folder}/*.table")):
            table_name = just_file_stem(file_name)
            logger.info(f"Processing {table_name} ...")

            # always load table objects
            table_object = load_jsonpickle(f"{self.work_folder}/{table_name}.table")

            # skip table if no schema file exists
            schema_file_name = f"{self.work_folder}/{table_name}.schema"
            if not is_file(schema_file_name):
                logger.warning(f"Table skipped ({table_name}); schema file not found")
                continue

            # always load table schema
            table_schema = load_jsonpickle(schema_file_name)

            # always load table pk
            table_pk = load_text(f"{self.work_folder}/{table_name}.pk").strip()

            # extend table object with table table and column names from table_schema object
            table_object.table_name = table_name
            table_object.column_names = [
                column_name for column_name in table_schema.columns
            ]

            # if drop_table, drop table and exit
            if table_object.drop_table:
                logger.info(f"Table drop request; table_drop=1")
                self.target_db_conn.drop_table(dataset_name, table_name)
                return

            # convert table schema to our target database and add extended column definitions
            extended_definitions = "udp_jobid int, udp_timestamp datetime2".split(",")
            convert_to_mssql(table_schema, extended_definitions)

            # Future: support custom staging table type overrides
            # [table].table_type = < blank > | standard, columnar, memory, columnar - memory

            # handle cdc vs non-cdc table workflows differently
            logger.debug(
                f"{table_name}.cdc={table_object.cdc}, timestamp={table_object.timestamp}"
            )
            if (
                not table_object.cdc
                or table_object.cdc.lower() == "none"
                or not table_pk
            ):
                # if table cdc=none, drop the target table
                logger.info(f"Table cdc=[{table_object.cdc}]; rebuilding table")
                self.target_db_conn.drop_table(dataset_name, table_name)

                # then re-create target table with latest schema
                # FUTURE: Add udp_pk, udp_nk, udp_nstk and other extended columns
                logger.info(f"Re-creating non-CDC table: {dataset_name}.{table_name}")
                self.target_db_conn.create_table_from_table_schema(
                    dataset_name, table_name, table_schema, extended_definitions
                )

                # no cdc in effect for this table - insert directly to target table
                work_folder_obj = pathlib.Path(self.work_folder)
                batch_number = 0
                for json_file in sorted(work_folder_obj.glob(f"{table_name}#*.json")):
                    # load rows from json file
                    # input_stream = open(json_file)
                    # rows = json.load(input_stream)
                    # input_stream.close()
                    rows = load_jsonpickle(json_file)

                    # insert/upsert/merge *.json into target tables
                    if not rows:
                        logger.info(f"Table {table_name} has 0 rows; no updates")
                    else:
                        batch_number += 1
                        logger.info(
                            f"Job {job_id}, batch {batch_number}, table {table_name}"
                        )
                        self.progress_message(
                            f"loading {just_file_stem(capture_file_name)}({table_name}.{batch_number:04}) ..."
                        )

                        # convert date/datetime columns to date/datetime values
                        convert_data_types(rows, table_schema)
                        self.target_db_conn.bulk_insert_into_table(
                            dataset_name, table_name, table_schema, rows
                        )

            else:
                # table has cdc updates

                # create target table if it doesn't exist
                if not self.target_db_conn.does_table_exist(dataset_name, table_name):
                    # FUTURE: Add udp_pk, udp_nk, udp_nstk and other extended columns
                    logger.info(f"Creating table: {dataset_name}.{table_name}")
                    self.target_db_conn.create_table_from_table_schema(
                        dataset_name, table_name, table_schema, extended_definitions
                    )

                # create temp table to receive captured changes
                # FUTURE: Create a database wrapper function for creating 'portable' temp table names vs hard-coding '#'.
                temp_table_name = f"_{table_name}"
                self.target_db_conn.drop_table(dataset_name, temp_table_name)
                self.target_db_conn.create_table_from_table_schema(
                    dataset_name, temp_table_name, table_schema, extended_definitions
                )

                # insert captured updates into temp table
                work_folder_obj = pathlib.Path(self.work_folder)
                batch_number = 0
                for json_file in sorted(work_folder_obj.glob(f"{table_name}#*.json")):
                    # load rows from json file
                    # input_stream = open(json_file)
                    # rows = json.load(input_stream)
                    # input_stream.close()
                    rows = load_jsonpickle(json_file)

                    # insert/upsert/merge *.json into target tables
                    if not rows:
                        logger.info(f"Table {table_name} has 0 rows; no updates")
                        break
                    else:
                        batch_number += 1
                        logger.info(
                            f"Job {job_id}, batch {batch_number}, table {table_name}"
                        )
                        self.progress_message(
                            f"loading {just_file_stem(capture_file_name)}({table_name}.{batch_number:04}) ..."
                        )

                        # convert date/datetime columns to date/datetime values
                        convert_data_types(rows, table_schema)
                        self.target_db_conn.bulk_insert_into_table(
                            dataset_name, temp_table_name, table_schema, rows
                        )
                else:
                    # merge (upsert) temp table to target table
                    merge_cdc = cdc_merge.MergeCDC(table_object, extended_definitions)
                    sql_command = merge_cdc.merge(dataset_name, table_pk)

                    # TODO: Capture SQL commands in a sql specific log.
                    logger.debug(sql_command)
                    self.target_db_conn.cursor.execute(sql_command)

                # drop temp table after merge
                self.target_db_conn.drop_table(dataset_name, temp_table_name)

    def process_next_file_to_stage(self):

        # any new arrivals that we can process? job_id=1 or next job in sequence?
        cursor = self.target_db_conn.execute("select_from_stage_arrival_queue")
        row = cursor.fetchone()
        if not row:
            return False
        else:
            # get blob name we should fetch for staging
            logger.info(f"Found next file to stage: {row}")
            archive_file_name = row.archive_file_name

            job_id = int(archive_file_name.split(".")[0].rsplit("#", 1)[-1])
            dataset_name = archive_file_name.partition("#")[0]
            archive_file_blob_name = f"{dataset_name}/{archive_file_name}"

            # stage the file we found
            self.progress_message(f"processing {archive_file_name} ...")
            self.stage_file(archive_file_blob_name)

            # after archive capture file processed then remove it from arrival/pending queues
            self.target_db_conn.execute(
                "delete_from_stage_arrival_queue", archive_file_name
            )
            self.target_db_conn.execute(
                "delete_from_stage_pending_queue", archive_file_name
            )

            # post the next file in sequence for dataset_name to pending queue
            next_archive_file_name = f"{dataset_name}#{job_id+1:09}.zip"
            row = dict(archive_file_name=next_archive_file_name)
            self.target_db_conn.insert_into_table(
                "udp_sys", "stage_pending_queue", **row
            )

            # return True to indicate we should continue processing queued up archived files
            return True

    def main(self):
        logger.info(
            f"{now():%Y-%m-%d %H:%M:%S}: Polling for queued files to process ..."
        )

        # connect to target database
        database_target = self.project.database_target
        db_resource = self.config(database_target)
        db = database.MSSQL(db_resource)
        # db.debug_flag = True
        self.target_db_conn = database.Database("mssql", db.conn)
        self.target_db_conn.use_database("udp_stage")

        # process queued files, then exit and let daemon scheduler loop handle polling
        have_archive_file_to_process = True
        while have_archive_file_to_process:
            # keep processing archived files until there are no more to process
            have_archive_file_to_process = self.process_next_file_to_stage()


# main
if __name__ == "__main__":
    project_file = "project_stage.ini"
    daemon = StageDaemon(project_file)
    daemon.run()
