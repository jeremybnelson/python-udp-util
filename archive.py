#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
archive.py

Polls landing blobstore for updates.
- identifies new files
- moves them to archive blobstore (a copy and delete transaction)
- extracts capture job event metrics for posting to system tables
- updates stage monitored arrival queue with successfully archived updates

Note: Redesign has replaced storage queue with blobstore polling technique.
"""


# standard lib
import logging


# common lib
from common import clear_folder
from common import from_jsonpickle
from common import just_file_stem
from common import now
from common import read_archived_file


# udp classes
from blobstore import BlobStore
from daemon import Daemon


# udp lib
import database
import udp


# module level logger
logger = logging.getLogger(__name__)


class ArchiveDaemon(Daemon):

    """Daemon class integrates core config, option, and schedule functionality."""

    def start(self):
        super().start()

        # make sure core database environment in place
        udp.setup(self.config)

    def archive_capture_file(self, capture_file_name):
        """
		Move capture file from landing to archive blobstore.
		Update stat log with capture metrics.
		Update stage arrival queue with capture file we just archived.

		Note: Capture file move done in a copy and delete sequence.
		"""

        # make sure work folder exists and is empty
        clear_folder(self.work_folder)

        # connect to the landing blobstore
        resource = self.config(self.project.blobstore_landing)
        bs_landing = BlobStore()
        bs_landing.connect(resource)

        # copy the capture file from our landing blobstore to our local work folder
        local_capture_file_name = f"{self.work_folder}/{capture_file_name}"
        # logger.info(f'capture_file_name = {capture_file_name}')
        # logger.info(f'local_capture_file_name = {local_capture_file_name}')
        bs_landing.get(local_capture_file_name, capture_file_name)

        # extract metrics from capture file and post to stat log
        self.update_stat_log(local_capture_file_name)

        # connect to the archive blobstore
        resource = self.config(self.project.blobstore_archive)
        bs_archive = BlobStore()
        bs_archive.connect(resource)

        # copy our local copy of the capture file to the archive blobstore
        dataset_name = capture_file_name.split("#")[0]
        archive_capture_file_name = f"{dataset_name}/{capture_file_name}"
        bs_archive.put(local_capture_file_name, archive_capture_file_name)
        bs_archive.disconnect()

        # delete the capture file from landing blobstore
        # Note: This completes the move operation.
        bs_landing.delete(capture_file_name)
        bs_landing.disconnect()

        # update stage arrival queue with name of capture file we just archived
        self.update_stage_arrival_queue(capture_file_name)

    def get_landing_file(self):
        """Looks for next file to process in landing blobstore."""

        # connect to the landing blobstore
        resource = self.config(self.project.blobstore_landing)
        bs_landing = BlobStore()
        bs_landing.connect(resource)
        capture_file_names = bs_landing.list("dataset*")
        bs_landing.disconnect()

        logger.info(f"capture_file_names = {capture_file_names}")

        # look for next capture file in landing
        if not capture_file_names:
            capture_file_name = ""
        else:
            self.progress_message(
                f"{len(capture_file_names)} file(s) available for archiving ..."
            )
            capture_file_name = capture_file_names[0]
        return capture_file_name

    def update_stage_arrival_queue(self, capture_file_name):
        """Register capture file in stage_arrival_queue table."""
        job_id = int(just_file_stem(capture_file_name).split("#")[1])
        row = dict(archive_file_name=capture_file_name, job_id=job_id)
        # print(f'table({self.target_db_conn}): stage_arrival_queue.insert({row})')
        self.target_db_conn.insert_into_table("udp_sys", "stage_arrival_queue", **row)

    def update_stat_log(self, capture_file_name):

        # extract job.log/last_job.log from capture zip and merge these into stat_log table
        job_log_data = read_archived_file(capture_file_name, "job.log", default=None)
        if job_log_data:
            job_log_json = from_jsonpickle(job_log_data)
            for row in job_log_json:
                # skip capture stats which only have intermediate end_time and run_time values
                # next capture file will include an accurate version of this stat in last_job.job file
                if row["event_stage"] != "capture":
                    # print(f'table({self.target_db_conn}): stat_log.insert({row})')
                    self.target_db_conn.insert_into_table("udp_sys", "stat_log", **row)

        # if 'last_job.log' in archive.namelist():
        job_log_data = read_archived_file(
            capture_file_name, "last_job.log", default=None
        )
        if job_log_data:
            last_job_log_json = from_jsonpickle(job_log_data)
            for row in last_job_log_json:
                if row["event_stage"] in ("capture", "compress", "upload"):
                    # print(f'table({self.target_db_conn}) stat_log.insert({row})')
                    self.target_db_conn.insert_into_table("udp_sys", "stat_log", **row)

    # main
    def main(self):
        # force unexpected exceptions to be exposed (at least during development)
        try:
            # connect to target database
            database_target = self.project.database_target
            db_resource = self.config(database_target)
            db = database.MSSQL(db_resource)
            self.target_db_conn = database.Database("mssql", db.conn)
            self.target_db_conn.use_database("udp_stage")

            # process all files in queue before returning to polling loop
            while True:
                # look for next file to process
                capture_file_name = self.get_landing_file()
                if not capture_file_name:
                    # nothing to process
                    break
                else:
                    # archive the capture file we found
                    self.progress_message(f"processing {capture_file_name} ...")
                    logger.info(f"Archive processing {capture_file_name} ...")
                    self.archive_capture_file(capture_file_name)

        # force unhandled exceptions to be exposed
        except Exception:
            logger.exception("Unexpected exception")
            raise


# main code
if __name__ == "__main__":
    project_file = "project_archive.ini"
    daemon = ArchiveDaemon(project_file)
    daemon.run()
