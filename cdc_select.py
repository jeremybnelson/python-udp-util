#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
# cdc_select.py

Generate CDC select statement.

table_object.schema_name = schema_name
table_object.table_name = table_name
table_object.column_names = '*'
select_cdc = SelectCDC(table_object)
sql = select_cdc.select(job_id, current_timestamp, last_timestamp)
"""

# standard lib
import logging

# common lib
from common import delete_blank_lines
from common import expand
from common import log_session_info
from common import log_setup
from common import spaces
from common import split

# module level logger
logger = logging.getLogger(__name__)


def q(items):
    """Decorates item/items with double-quotes to protect table/column names that may be reserved words."""
    if isinstance(items, (list, tuple, set)):
        # don't double double-quote items that are already double-quoted
        return [item if item.startswith('"') else f'"{item}"' for item in items]
    elif items.startswith('"'):
        # don't double double-quote items that are already double-quoted
        return items
    else:
        return f'"{items}"'


def add_alias(column_name, table_alias):
    """Adds table_alias (if missing) and double-quotes table alias and column name."""
    column_name = column_name.replace('"', "")
    if "." in column_name:
        table_alias, separator, column_name = column_name.partition(".")
    column_name = f"{q(table_alias)}.{q(column_name)}"
    return column_name


def add_aliases(column_names, table_alias="s"):
    """Performs add_alias() on a list of column names."""
    return [add_alias(column_name, table_alias) for column_name in column_names]


###


"""Test code"""


# test - part of TableSchema
class Column:
    def __init__(self, column):
        self.column_name = column.column_name
        self.data_type = column.data_type
        self.is_nullable = column.is_nullable
        self.character_maximum_length = column.character_maximum_length
        self.numeric_precision = column.numeric_precision
        self.numeric_scale = column.numeric_scale
        self.datetime_precision = column.datetime_precision
        self.character_set_name = column.character_set_name
        self.collation_name = column.collation_name


# test
class Table:
    def __init__(self, schema_name, table_name, column_names):
        self.schema_name = schema_name
        self.table_name = table_name
        self.column_names = split(column_names)

        self.table_prefix = ""
        self.table_suffix = ""
        self.natural_key = ""
        self.cdc = ""
        self.timestamp = ""
        self.first_timestamp = ""
        self.rowversion = ""
        self.first_rowversion = ""
        self.select = ""
        self.where = ""
        self.ignore = ""
        self.order = ""


###


def indent(text):
    """Protect logical indentation of indented multi-line text values."""
    output = []
    for line in text.strip().splitlines():
        line = line.strip()
        if line.startswith("_ "):
            line = line[1:]
        elif line.startswith("__"):
            line = line[2:].strip()
        output.append(line)
    return "\n".join(output)


def clean_sql(text):
    # convert text to lowercase so we can match keywords consistently
    text = text.lower()

    # strange SQL formatting corrections go here ...

    # correct missing spaces before square-brackets
    text = text.replace("join[", "join [")

    # remove square-bracket quoting; we'll re-quote later with ANSI double-quotes
    text = text.replace("[", "").replace("]", "")

    # make sure '=, (, )' are space delimited so they don't "stick" to adjacent tokens
    text = text.replace("=", " = ")
    text = text.replace("(", " ( ")
    text = text.replace(")", " ) ")

    # remove -- comments from each line
    output = []
    for line in text.strip().splitlines():
        line = line.partition("--")[0]
        output.append(line)
    text = "\n".join(output)

    # normalize text to single-space delimited string
    text = " ".join(text.split())

    # after special chars are space delimited and whitespace normalized ...

    # remove WITH (NOLOCK) clauses
    text = text.replace("with ( nolock )", "")

    return text


"""
quote join/on table items ???
join processing: select > join

before \n and \t to space conversion run clean_sql()
clean_sql(text) - remove -- comments, replace [/] square-braces with ANSI double-quotes

join on clauses can have parentheses join table t on (t.col=s.col) 
we need to split out the parentheses, =, <, >, !, quote the elements, and recombine - or just use as-is ???
after converting \n and \t to spaces, remove extra spaces 
full|left|right [outer] join, inner join, cross join, join ... vs just "join " support
remove "WITH (NOLOCK)" clauses
replace "RTPIkon..TransactionLine" (database..table) with schema.table
quote schema.table and alias.column in join/on clauses; do after fixing '..' expressions

no support for embedded selects: join (select distinct rtp1_source_code, lobcode from ##ProductCode_Ascent) t
embedded selects (and unions/intersections) must be implemented as views or temp tables
SQL Server does not support: join ... using(...), natural join 
"""

join_keyword_phrases = """
full inner join
full outer join
left inner join
left outer join
right inner join
right outer join
cross join
full join
left join
right join
inner join
outer join
join
""".splitlines()


# [FULL|LEFT|RIGHT] [INNER|OUTER] ]CROSS] <JOIN> [<database>>..]<table> <alias> [with (NOLOCK)] ON <condition> [-- *]
def format_join(text, schema_name):
    text = clean_sql(text)

    join_keywords = split(
        "full, left, right, inner, outer, cross, join, on, and, or, not"
    )
    output = []
    last_token = ""
    for token in text.split():
        if token in join_keywords or not token[0].isalpha():
            output.append(token)
        else:
            if ".." in token:
                token = q(token.partition("..")[2])
            elif token.startswith("dbo."):
                token = q(token[4:])
            elif "." in token:
                alias_name, separator, table_name = token.partition(".")
                token = f"{q(alias_name)}.{q(table_name)}"
            else:
                token = q(token)

            # add schema name if last token ends with 'join' and token missing schema name
            if last_token.endswith("join") and "." not in token:
                token = f"{q(schema_name)}.{token}"

            output.append(token)

        last_token = token

    text = " ".join(output)

    # convert join keyword phrases to tokens
    for join_keyword_phrase in join_keyword_phrases:
        join_keyword_token = join_keyword_phrase.replace(" ", "::")
        text = text.replace(join_keyword_phrase, join_keyword_token)

    # format joins into 2-line clauses
    output = []
    for token in text.split():
        if token.endswith("join"):
            token = f"\n{spaces(2)}{token}"
        elif token == "on":
            token = f"\n{spaces(4)}{token}"
        output.append(token + " ")

    # expand join keyword tokens back to join keyword phrases
    text = "".join(output)
    text = text.replace("::", " ")

    return text


class SelectCDC:
    # noinspection SqlNoDataSourceInspection
    select_template = """
      select
        {column_names},
        {job_id} as "udp_job",
        {timestamp_value} as "udp_timestamp"
        from "{schema_name}"."{table_name}" as "s"
        {join_clause}
        {where_clause}
        {order_clause}
    """

    timestamp_where_template = """
        (
            {timestamp_value} >= '{last_timestamp}' and
            {timestamp_value} < '{current_timestamp}'
        )
    """

    sequence_where_template = """
        (
            {sequence_value} >= '{last_sequence}' and
            {sequence_value} < '{current_sequence}'
        )
    """

    def __init__(self, db_engine, table):
        # indent template text
        self.select_template = indent(self.select_template)
        self.timestamp_where_template = indent(self.timestamp_where_template)

        # object scope properties
        self.db_engine = db_engine
        self.table = table
        self.timestamp_value = ""
        self.timestamp_where_condition = ""

    def column_names(self):
        if self.table.column_names == "*":
            return "*"
        else:
            column_names = add_aliases(self.table.column_names, "s")
            return ", ".join(column_names)

    # noinspection PyUnusedLocal
    # Note: last_timestamp referenced in expanded where f-string.
    def timestamp_logic(self, current_timestamp, last_timestamp=None):
        if not self.table.timestamp:
            # # TEMP SOLUTION: 2019-03-19
            # if self.table.schema_name == 'dataset_1001':
            # 	# PostgreSQL:
            # 	timestamp_str = f'{current_timestamp:%Y-%m-%d %H:%M:%S}'
            # 	self.timestamp_value = f"to_timestamp('{timestamp_str}', 'YYYY-MM-DD hh24:mi:ss')::timestamp without time zone"
            # 	self.timestamp_where_condition = ''
            # else:
            # 	# SQL Server: {ts'{timestamp_str}'}
            # 	timestamp_str = f'{current_timestamp:%Y-%m-%d %H:%M:%S}'
            # 	self.timestamp_value = f"to_timestamp('{timestamp_str}', 'YYYY-MM-DD hh24:mi:ss')::timestamp without time zone"
            # 	self.timestamp_where_condition = ''

            # May/Jun enhancement
            self.timestamp_value = self.db_engine.timestamp_literal(current_timestamp)
            self.timestamp_where_condition = ""

        else:
            timestamp_columns = add_aliases(split(self.table.timestamp))
            if len(timestamp_columns) == 1:
                timestamp_value = q(timestamp_columns[0])
            else:
                # build timestamp column values as ("<column-1>"), ("<column-2>"), ("<column-n>")
                timestamp_values = ", ".join(
                    [f"({q(column_name)})" for column_name in timestamp_columns]
                )
                timestamp_value = (
                    f'(select max("v") from (values {timestamp_values}) as value("v"))'
                )

            self.timestamp_value = timestamp_value
            self.timestamp_where_condition = expand(self.timestamp_where_template)

    def join_clause(self):
        schema_name = self.table.schema_name
        join_clause = self.table.join.strip("\\")
        if join_clause:
            join_clause = "\n" + format_join(join_clause, schema_name)
        return join_clause

    def where_clause(self):
        if not self.table.where and not self.timestamp_where_condition:
            where_clause = ""
        elif self.table.where and not self.timestamp_where_condition:
            where_clause = f"where\n{spaces(4)}({self.table.where})"
        elif not self.table.where and self.timestamp_where_condition:
            where_clause = f"where\n{spaces(4)}{self.timestamp_where_condition}"
        else:
            where_clause = f"where\n{spaces(4)}({self.table.where}) and\n{spaces(4)}{self.timestamp_where_condition}"
        return where_clause

    def order_clause(self):
        # order by option
        order_clause = ""
        if self.table.order:
            order_columns = add_aliases(split(self.table.order))
            order_clause = f'order by {", ".join(order_columns)}'
        return order_clause

    # noinspection PyUnusedLocal
    def select(self, job_id, current_timestamp, last_timestamp):
        self.timestamp_logic(current_timestamp, last_timestamp)

        schema_name = self.table.schema_name
        table_name = self.table.table_name
        column_names = self.column_names()
        timestamp_value = self.timestamp_value
        join_clause = self.join_clause()
        where_clause = self.where_clause()
        order_clause = self.order_clause()
        sql = expand(self.select_template)
        return delete_blank_lines(sql.strip() + ";")


# temporary test harness ...


# test code
def main():
    pass


# test code
if __name__ == "__main__":
    log_setup()
    log_session_info()
    main()
