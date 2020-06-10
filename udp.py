#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
udp.py

Code to stand-up (or extend) a UDP target environment.
- Database
- Schemas
- System tables

"""


# standard lib
import logging


# common lib
from common import log_setup
from common import log_session_info


# udp classes
from config import ConfigSectionKey


# udp lib
import database


# module level logger
logger = logging.getLogger(__name__)


# global names
udp_stage_database = "udp_stage"
udp_sys_schema = "udp_sys"


def setup(config):
    # Future:
    # - Drive this setup from a project_setup.ini file
    # - Add activity_log (vs job_log/stat_log) references
    # - Debug should also enable/disable SQL output and timing/record count stats

    db_resource = config("database:amc_dsg_udp_stage")
    db = database.MSSQL(db_resource)
    db_conn = database.Database("mssql", db.conn)

    # create data stage database if not present; then use
    db_conn.create_database(udp_stage_database)
    db_conn.use_database(udp_stage_database)

    # create udp sys schema if not present
    db_conn.create_schema(udp_sys_schema)

    # create udp sys tables if not present
    db_conn.create_named_table(udp_sys_schema, "nst_lookup")
    db_conn.create_named_table(udp_sys_schema, "job_log")
    db_conn.create_named_table(udp_sys_schema, "stat_log")
    db_conn.create_named_table(udp_sys_schema, "table_log")
    db_conn.create_named_table(udp_sys_schema, "stage_arrival_queue")
    db_conn.create_named_table(udp_sys_schema, "stage_pending_queue")


# temp test scaffolding ...


# test code
def test():
    # activate logging
    log_setup()
    log_session_info()

    # load standard config
    config = ConfigSectionKey("../conf", "../local")
    config.load("bootstrap.ini", "bootstrap")
    config.load("init.ini")
    config.load("connect.ini")
    setup(config)


# test code
if __name__ == "__main__":
    test()
