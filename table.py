#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
Table.py

Track status of tables
- table structure (auto-discovered)
- table configuration (project file [table:*] section)

See also
- capture.py
- stage.py
- tableschema.py
- database.py

Todo
- column classifications per SQL Server column classifications; pii, financial, secret, sensitive, strategic
- change properties to property| = list and property|attribute = dict
- fix multi-line config.py property values for join, where

# properties whose changes trigger a table reload
self.table_type = '' # <blank> (default), columnar, memory, or columnar-memory; stage uses when creating table
self.table_name = ''
self.table_prefix = ''
self.table_suffix = ''
self.drop_table = ''
self.ignore_table = ''

# description of table
self.primary_key = ''
self.cdc = ''
self.timestamp = ''
self.first_timestamp = ''
self.rowversion = ''
self.first_rowversion = ''
self.join = ''
self.where = ''
self.ignore_columns = ''

# drop these properties ???
self.order = ''
self.delete_when = ''


###

# future properties that we can ignore for now

# metadata
self.table_comment = ' '
self.tags = ''
self.sensitive_columns = ''

# column conversions
self.column = dict() # override column conversion by specifying target column type/size


"""


# stdlib
import logging


# common
from common import log_setup
from common import log_session_info


# udp classes
from config import ConfigSectionKey


# module level logger
logger = logging.getLogger(__name__)


# temp test harness ...


# test code
def main():
    config = ConfigSectionKey("../conf", "../local")
    config.load("project_capture_amc_rtp_sales.ini")
    for table_name in config.keys("table:*"):
        table = config(table_name)
        table.dump(dump_blank_values=False)


# test code
if __name__ == "__main__":
    log_setup(log_level=logging.WARNING)
    log_session_info()
    main()
