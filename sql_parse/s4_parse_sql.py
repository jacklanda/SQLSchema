# -*- coding: utf-8 -*-
# @author: Yang Liu
# @email: v-yangliu4@microsoft.com


r"""
###########################################################
###                                                     ###
###    Parse .sql file from s3, extract constraints.    ###
###    Supported extraction features at present:        ###
###    - constriants like pk / fk / uk / ui             ###
###      wherever on create table or alter table,       ###
###                                                     ###
###########################################################

                         SQL Table Parser
                            /   |   \
                           /    |    \
                        create ...  alter
                         / \         / \
                        /   \       /   \
                      ...   ...   ...   ...

Procedure design:
    - Judge firstly whether is a `create` operation or an `alter` operation on table, it can be described as a binary selection.
    - Read && save table name, and split each sql statement according to the comma `,` followed it.
    - For each statement, judge whether is a multi-column operation or a single column operation whatever it's creation or alteration.
    - Handle the different statement according to its column type.
    - Append the handling result to our pre-defined data structure.
    - When handled all the SQL files, dump locally the data structure as a pickle file to finish SQL parsing procedure.
"""

###############################################################
# [DONE] parsing coverage on CREATE TABLE,                    #
#        ALTER TABLE and CREATE INDEX statements: 88.53%      #
# [DONE] handle PK                                            #
# [DONE] handle FK                                            #
# [DONE] handle multi-col keys                                #
# [DONE] parse for single-file level                          #
# [DONE] parse for repo-database level                        #
# [DONE] handle clauses without semicolon delimiter           #
# [DONE] handle create (unique) index                         #
# [DONE] handle queries with JOINs                            #
# [DONE] impl parallel parsing                                #
###############################################################


import os
import re
import time
import signal
import pickle
import logging
import traceback
from enum import Enum
from enum import unique
from pprint import pprint
from copy import deepcopy
from collections import deque

import sqlparse

from sample import print_table_obj
from parse_query import QueryParser
# from parse_join_query import (
# parse_one_statement_select_join,
# parse_one_statement_select_where,
# )
from utils import (
    rm_kw,
    fmt_str,
    clean_stmt,
    calc_col_cov,
    split_string,
    query_stmt_split,
    # convert_camel_to_underscore,
    Counter,
    RegexDict,
)


INPUT_FOLDER = os.path.join(os.getcwd(), "data/s3_sql_files_crawled_all_vms")
OUTPUT_FOLDER = os.path.join(os.getcwd(), "data/s4_sql_files_parsed")
STATEMENT_SIZE_LIMIT = 50000

TOKEN_NOTNULL = "[NOTNULL]"
# Note: The `UNIQUE` constraint ensures that all values in a column are different,
#       both the `UNIQUE` and `PRIMARY KEY` constraints provide a guarantee for uniqueness for a column or set of columns.
#       A `PRIMARY KEY` constraint automatically has a `UNIQUE` constraint.
#       However, you can have many `UNIQUE` constraints per table, but only one `PRIMARY KEY` constraint per table.
TOKEN_UNIQUE = "[UNIQUE]"
TOKEN_TABLE = "[TABLE]"
TOKEN_COL = "[COL]"
# prefix of col data types recognized, before we recognize the line to be col definition:
# https://www.w3schools.com/sql/sql_datatypes.asp
# https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver15
COL_DATA_TYPES = ["varchar", "serial", "long", "uuid", "bytea", "json", "string", "char", "binary", "blob", "clob", "text", "enum", "set", "number", "numeric", "bit", "int", "bool", "float", "double", "decimal", "date", "time", "year", "image", "real", "identity", "identifier", "raw", "graphic", "money", "geography", "cursor", "rowversion", "hierarchyid", "uniqueidentifier", "sql_variant", "xml", "inet", "cidr", "macaddr", "point", "line", "lseg", "box", "path", "polygon", "circle", "regproc", "tsvector", "sysname", "tid"]

REGEX_DICT = RegexDict()

COUNTER_CT, COUNTER_CT_SUCC, COUNTER_CT_EXCEPT = Counter(), Counter(), Counter()
COUNTER, COUNTER_EXCEPT = Counter(), Counter()
COUNTER_QUERY, COUNTER_QUERY_EXCEPT = Counter(), Counter()

TOTAL_TABLE_NUM, REPEAT_NUM, NOT_REPEAT_NUM = 0, 0, 0


@unique
class ParseStage(Enum):
    """Enum class for definitions on parsing stages.

    Stage
    -----
    - create: handle CREATE TABLE statements.
    - alter: handle ALTER TABLE statements.
    - fk: handle FKs on referred missing cases in create and alter.
    - query: handle queries with JOINs statements.
    """
    create = 0
    alter = 1
    insert = 2
    fk = 3
    query = 4


class Query:
    """Query class for clauses SELECT with JOINs object construction,
    Construct a Query object to maintain all the Join objects in query-join statement.

    Params
    ------
    - join_list: list[Join]

    Attribs
    -------
    - join_list: list[Join]

    Returns
    -------
    - A Query object
    """

    def __init__(self, join_list):
        self.__join_list = join_list

    @property
    def join_list(self):
        return self.__join_list


class Key:
    """Key class for Primary Key, Candidate Key, Unique Index and Unique Column object construction.
    A table can have only one primary key, which may consist of single or multiple fields.
    In this class, we use instance variable `is_pk` to represent this object is to save pk or others.
    There are totally six kinds of optional key types for every Key object:
    (1) PrimaryKey, (2) CandidateKey, (3) UniqueKey
    (4) UniqueIndex, (5) UniqueColumn, (6) Index

    Params
    ------
    - key_type: str
    - key_col_list: list[Column]

    Attribs
    -------
    - key_type: str
    - key_col_list: list[Column]

    Returns
    -------
    - a Key object
    """

    def __init__(self, key_type, key_col_list):
        self.__key_type = key_type
        self.__key_col_list = key_col_list

    @property
    def key_type(self):
        return self.__key_type

    @property
    def key_col_list(self):
        return self.__key_col_list


class ForeignKey:
    """ForeignKey class for Foreign Key object construction.
    A table can have several different fks, which may consist of single or multiple fields.
    The several fk objects could be maintained in a list which should be a member of Table class.

    Params
    ------
    - fk_col_list: list[Column]
    - ref_tab_obj: Table
    - ref_col_list: list[Column]

    Attribs
    -------
    - fk_cols: list[Column]
    - ref_tab: Table
    - ref_cols: list[Column]

    Returns
    -------
    - a ForeignKey object
    """

    def __init__(self, fk_col_list, ref_tab_obj, ref_col_list):
        self.__fk_col_list = fk_col_list
        if not isinstance(ref_tab_obj, Table):
            raise ValueError("param `ref_tab_obj` must be a Table object!")
        self.__ref_tab_obj = ref_tab_obj
        self.__ref_col_list = ref_col_list

    @property
    def fk_cols(self):
        return self.__fk_col_list

    @property
    def ref_tab(self):
        return self.__ref_tab_obj

    @property
    def ref_cols(self):
        return self.__ref_col_list


class Column:
    """Construct a column object for a SQL column.

    Params
    ------
    - col_name: str
    - is_notnull: bool, default=False

    Returns
    -------
    - a Column object
    """

    def __init__(
        self,
        col_name,
        is_notnull=False,
    ):
        self.col_name = col_name
        self.is_notnull = is_notnull

    def is_col_inferred_notnull(self):
        return self.is_notnull

    def print_for_lm_components(self):
        """multi-line, new format, for classification csv
        """
        str_list = [self.cleansed_col_name()]
        if self.is_col_inferred_notnull():
            str_list.append(TOKEN_NOTNULL)
        else:
            str_list.append('')

        return str_list

    def cleansed_col_name(self):
        return self.col_name.strip('\'`"[]')


class Table:
    """Construct table object for a SQL table.

    Params
    ------
    - tab_name: str
    - hashid: str
    - key_list: Optional[None, list[Key]], default=None
    - fk_list: Optional[None, list[ForeignKey]], default=None

    Returns
    -------
    - a Table object
    """

    def __init__(self, tab_name, hashid, key_list=None, fk_list=None):
        self._tab_name = tab_name
        self._hashid = hashid
        self._key_list = key_list
        self._fk_list = fk_list
        self._name2col = dict()
        self._col_name_seq = list()  # log the order in which cols are added into the table (leftness etc. matter)

    @property
    def tab_name(self):
        return self._tab_name

    @property
    def hashid(self):
        return self._hashid

    @property
    def key_list(self):
        if self._key_list is None:
            self._key_list = list()
        return self._key_list

    @property
    def fk_list(self):
        if self._fk_list is None:
            self._fk_list = list()
        return self._fk_list

    @property
    def name2col(self):
        return self._name2col

    @property
    def col_name_seq(self):
        return self._col_name_seq

    def insert_col(self, col):
        """Insert a new column into the table object."""
        if col.col_name in self.name2col:
            # raise Exception(f"col already exists in table: {col.col_name}")
            return
        self.name2col[col.col_name] = col

    def print_for_lm_multi_line(self):
        """Generate text for language modeling."""
        # iterate cols in the order in which they are added
        col_lm_str_list = list()
        for col_name in self.col_name_seq:
            col_obj = self.name2col[col_name]
            components = col_obj.print_for_lm_components()
            components.insert(0, self.hashid)  # add file-name
            components.insert(1, self.cleansed_table_name())  # add file-name

            # skip line if components[0] (table-name) has bad punct (likely bad sql parse)
            if ' ' in components[1] or ',' in components[1]:
                print("skipping line for bad parse (punct in tab_name): " + components[1])
                continue
            # skip line if components[0] (table-name) has bad punct (likely bad sql parse)
            if ' ' in components[2] or ',' in components[2]:
                print("skipping line for bad parse (punct in col_name): " + components[2])
                continue
            col_lm_str_list.append(','.join(components))

        return col_lm_str_list

    def cleansed_table_name(self):
        """Clean table name."""
        clean_tab_name = self.tab_name
        if '.' in clean_tab_name:
            clean_tab_name = clean_tab_name.split('.')[1]
        return clean_tab_name.strip('\'`"[]')


class File:
    """Construct a file object to keep the information from a SQL file.

    Describe
    --------
    1) firstly need to separate statements into blocks by `;`,
    2) and then find one includes the substr either CREATE TABLE or ALTER TABLE,
    3) for CREATE TABLE, use regex to separate cols by `,`, and extract constraints,
    4) ditto for ALTER TABLE.

    Params
    ------
    - hashid: str
    - repo_name2tab: dict[str:Table]

    Returns
    -------
    - a file object
    """

    def __init__(self, hashid, repo_name2tab, multi_name2tab):
        self.hashid = hashid
        self.repo_name2tab = repo_name2tab
        self.multi_name2tab = multi_name2tab
        self.memo = set()  # set(tuple[str])
        self.query_list = list()

    @staticmethod
    def construct_key_obj(key_type, key_col_list):
        """Construct a key object.

        Params
        ------
        - key_type: str
        - key_cols_list: list

        Returns
        -------
        - a Key object
        """
        # key_col_list = fmt_str(key_cols_str).split(',')
        # key_col_list = [c.strip() for c in key_col_list]
        return Key(key_type, key_col_list)

    @staticmethod
    def construct_fk_obj(fk_col_list, ref_tab_obj, ref_col_list):
        """Construct a foreign key object.

        Params
        ------
        - fk_cols_str: str
        - ref_tab_obj: str
        - ref_cols_str: str

        Returns
        -------
        - a ForeignKey object
        """
        # fk_col_list = fmt_str(fk_cols_str).split(',')
        # fk_col_list = [c.strip() for c in fk_col_list]
        # ref_col_list = fmt_str(ref_cols_str).split(',')
        # ref_col_list = [c.strip() for c in ref_col_list]
        return ForeignKey(fk_col_list, ref_tab_obj, ref_col_list)

    def extract_tab_col_name(self, entity_name):
        return (entity_name.rsplit('.', 1)[0], entity_name.rsplit('.', 1)[1]) \
            if '.' in entity_name \
            else (None, entity_name)

    def get_max_col_nums_table(self, new_table_obj):
        table_obj = new_table_obj
        # self.multi_name2tab[table_obj.tab_name].add(table_obj)
        for tab_obj in self.multi_name2tab[table_obj.tab_name]:
            if len(table_obj.name2col) == len(tab_obj.name2col):
                if len(table_obj.key_list) + len(table_obj.fk_list) < len(tab_obj.key_list) + len(tab_obj.fk_list):
                    table_obj = tab_obj
            elif len(table_obj.name2col) < len(tab_obj.name2col):
                table_obj = tab_obj
        return table_obj

    def is_ui_ref_valid(self, tab, cols):
        """Check the validity of the references
        for unique index wherever on create or alter table.
        """
        return self._is_ref_valid(tab, cols)

    def is_pk_ref_valid(self, tab, cols):
        """Check the validity of the references
        for primary key wherever on create or alter table.
        """
        return self._is_ref_valid(tab, cols)

    def is_fk_ref_valid(self, tab, cols):
        """Check the validity of the references
        for foreign key wherever on create or alter table.

        Two aspects to check:
        - check the def cols in table
        - check the ref cols in ref table
        """
        return self._is_ref_valid(tab, cols)

    def is_uk_ref_valid(self, tab, cols):
        """Check the validity of the references
        for unique key wherever on create or alter table.
        """
        return self._is_ref_valid(tab, cols)

    def is_key_ref_valid(self, tab, cols):
        """Check the validity of the references
        for key wherever on create or alter table.
        """
        return self._is_ref_valid(tab, cols)

    def _is_ref_valid(self, tab, cols):
        """Internal base function for inspecting whether the input references is valid,
        and the input params `tab` and `cols` are necessary.

        Params
        ------
        - tab: Optional[str, Table]
        - cols: str

        Returns
        -------
        - bool
        """
        def add_branket(s):
            return '[' + s + ']'

        def rm_branket(s):
            return s.replace('[', '').replace(']', '') \
                if len(s) > 1 and s[0] == '[' and s[-1] == ']' else s
        # check ref tab is valid or not
        if isinstance(tab, Table):
            tab_obj = tab
        elif isinstance(tab, str):
            tab = fmt_str(tab)
            if tab in self.repo_name2tab:
                tab_obj = self.repo_name2tab[tab]
            else:
                # print(f"Unknown ref table `{tab}`", end=" | ")
                return False
        else:
            raise TypeError("References check error! Param `tab`'s type must be either Table or str")
        # check ref col is valid or not
        if cols is not None:
            cols = fmt_str(cols).split(',')
            cols = [rm_kw(c) for c in cols]
            for col in cols:
                if col not in tab_obj.name2col:
                    # print(f"Unknown ref col `{col}` in ref table `{tab_obj.tab_name}`")
                    return False
        return True

    def parse_one_statement_create_table(self, stmt):
        """Parse a SQL statement on create table,
        Put the unresolved foreign key to file_memo.
        TODO: unhandled T-SQL for now, ignore for now.
              syntax from https://docs.microsoft.com/en-us/sql/t-sql/statements/create-table-transact-sql?view=sql-server-ver15

        Params
        ------
        - stmt: str

        Returns
        -------
        - a Table object / None
        """
        try:
            # parse table name, create table obj
            # tab_name = fmt_str(stmt.split("create table")[1].split('(')[0]).replace("if not exists", "").replace("IF NOT EXISTS", "").strip()
            if "create table" in stmt.lower():
                tab_name = fmt_str(split_string(stmt, "create table").split('(')[0]).replace("if not exists", "").replace("IF NOT EXISTS", "").strip()
                try:
                    stmt = fmt_str(split_string(stmt, "create table").split('(', 1)[1]) if '(' in stmt else fmt_str(split_string(stmt, "create table").split(tab_name)[1])
                except:
                    return
            elif "create temporary table" in stmt.lower():
                tab_name = fmt_str(split_string(stmt, "create temporary table").split('(')[0]).replace("if not exists", "").replace("IF NOT EXISTS", "").strip()
                try:
                    stmt = fmt_str(split_string(stmt, "create temporary table").split('(', 1)[1]) if '(' in stmt else fmt_str(split_string(stmt, "create temporary table").split(tab_name)[1])
                except:
                    return

            tab_obj = Table(tab_name, self.hashid)

            # new feature:
            # try:
            # stmt = stmt.split('(', 1)[1].strip()
            # except:
            # stmt = stmt.split(tab_name, 1)[1].strip()
            pattern = re.compile("\(.*?\)", re.IGNORECASE)
            multicol_list = pattern.findall(stmt)
            # stmt = re.sub("\(.*?\)", "[MULTI-COL]", stmt, re.IGNORECASE)
            stmt = pattern.sub("[MULTI-COL]", stmt)

            # get all clauses on create table
            # clauses = split_string(stmt, "create table").split('(', 1)[1].strip()
            # remove the last found index of )
            # TODO: remove clause after `)`
            stmt = "".join(stmt[i] for i in range(len(stmt)) if i != stmt.rfind(')'))
            # split by comma, use regex to ignore commas in matching parentheses
            # this regex pattern could ensure multi columns kept.
            # clauses = [c.strip() for c in re.split(REGEX_DICT("split_clause_by_comma"), stmt, re.IGNORECASE) if not c.isspace()]
            clauses = [c.strip() for c in stmt.split(',') if not c.isspace()]

            with Timeout(seconds=1):
                # potential memory leak here, could be handled better.
                if len(multicol_list) != 0:
                    temp_list = list()
                    i = 0
                    while i in range(len(multicol_list)):
                        for c in clauses:
                            while "[MULTI-COL]" in c and i < len(multicol_list):
                                multicol = multicol_list[i]
                                c = c.replace("[MULTI-COL]", multicol, 1)
                                i += 1
                            temp_list.append(c)
                    clauses = temp_list

            for clause in clauses:
                clause_lower = clause.lower()
                # skip the clause which starts with COMMENT ON
                if clause_lower.startswith("comment on"):
                    continue
                # handle clause starts with constraint
                elif clause_lower.startswith("constraint"):
                    # handle: CONSTRAINT [constraint_name] PRIMARY KEY ([pk_cols])
                    if "primary key" in clause_lower:
                        pattern = REGEX_DICT("constraint_pk_create_table")
                        try:
                            result = re.findall(pattern, clause, re.IGNORECASE)
                        except:
                            continue
                        if len(result) > 0:
                            pk_cols = rm_kw(result[0])
                        else:
                            # raise Exception("CONSTRAINT PRIMARY KEY def error: match number must be 1!")
                            # print("CONSTRAINT PRIMARY KEY def error: match number must be 1!")
                            COUNTER_EXCEPT.add()
                            continue
                        if self.is_pk_ref_valid(tab_obj, pk_cols):
                            try:
                                pk_cols = get_column_object(tab_obj, pk_cols)
                                pk_obj = File.construct_key_obj("PrimaryKey", pk_cols)
                                tab_obj.key_list.append(pk_obj)
                            except:
                                continue
                        else:
                            # raise Exception("CONSTRAINT PRIMARY KEY def error: references on create table not found!")
                            # print("CONSTRAINT PRIMARY KEY def error: references on create table not found!")
                            COUNTER_EXCEPT.add()
                            continue
                    # handle: CONSTRAINT [constraint_name]
                    #         FOREIGN KEY ([fk_cols]) REFERENCES [ref_table] ([ref_cols])
                    elif "foreign key" in clause_lower:
                        pattern = REGEX_DICT("constraint_fk_create_table")
                        try:
                            result = re.findall(pattern, clause, re.IGNORECASE)[0]
                        except:
                            continue
                        if len(result) == 3:
                            fk_cols = fmt_str(result[0])
                            fk_ref_tab = fmt_str(result[1])
                            fk_ref_cols = fmt_str(result[2])
                        else:
                            # raise Exception("CONSTRAINT FOREIGN KEY def error: match number must be 3!")
                            # print("CONSTRAINT FOREIGN KEY def error: match number must be 3!")
                            COUNTER_EXCEPT.add()
                            continue
                        if self.is_fk_ref_valid(tab_obj, fk_cols):
                            if fk_ref_tab == tab_name and self.is_fk_ref_valid(tab_obj, fk_ref_cols):
                                try:
                                    fk_cols = get_column_object(tab_obj, fk_cols)
                                    fk_ref_cols = get_column_object(tab_obj, fk_ref_cols)
                                    fk_obj = File.construct_fk_obj(fk_cols, tab_obj, fk_ref_cols)
                                    tab_obj.fk_list.append(fk_obj)
                                except:
                                    continue
                            elif fk_ref_tab != tab_name and self.is_fk_ref_valid(fk_ref_tab, fk_ref_cols):
                                try:
                                    ref_tab_obj = self.repo_name2tab[fk_ref_tab]
                                    fk_cols = get_column_object(tab_obj, fk_cols)
                                    fk_ref_cols = get_column_object(ref_tab_obj, fk_ref_cols)
                                    fk_obj = File.construct_fk_obj(fk_cols, ref_tab_obj, fk_ref_cols)
                                    tab_obj.fk_list.append(fk_obj)
                                except:
                                    continue
                            else:
                                self.memo.add((tab_name, fk_cols, fk_ref_tab, fk_ref_cols))
                                COUNTER_EXCEPT.add()
                                # print("CONSTRAINT FOREIGN KEY def error: references on create table not found!")
                        else:
                            # raise Exception("CONSTRAINT FOREIGN KEY def error: references on create table not found!")
                            # print("CONSTRAINT FOREIGN KEY def error: references on create table not found!")
                            COUNTER_EXCEPT.add()
                            continue
                    # handle: CONSTRAINT [constraint_name] UNIQUE ([uniq_cols])
                    # n.b. UNIQUE and UNIQUE KEY are equivalent
                    elif "unique" in clause_lower:
                        pattern = REGEX_DICT("constraint_unique_create_table")
                        try:
                            result = re.findall(pattern, clause, re.IGNORECASE)
                        except:
                            continue
                        if len(result) == 1:
                            uk_cols = fmt_str(rm_kw(result[0]))
                        else:
                            # raise Exception("CONSTRAINT UNIQUE def error: match number must be 1!")
                            # print("CONSTRAINT UNIQUE def error: match number must be 1!")
                            COUNTER_EXCEPT.add()
                            continue
                        if self.is_uk_ref_valid(tab_obj, uk_cols):
                            try:
                                uk_cols = get_column_object(tab_obj, uk_cols)
                                uk_obj = File.construct_key_obj("UniqueKey", uk_cols)
                                tab_obj.key_list.append(uk_obj)
                            except:
                                continue
                        else:
                            # raise Exception("CONSTRAINT UNIQUE def error: references on create table not found!")
                            # print("CONSTRAINT UNIQUE def error: references on create table not found!")
                            COUNTER_EXCEPT.add()
                            continue
                    else:
                        # raise Exception("CONSTRAINT handle error: unknown constraint type!")
                        # print("CONSTRAINT handle error: unknown constraint type!")
                        COUNTER_EXCEPT.add()
                        continue
                # handle primary key
                elif "primary key" in clause_lower:
                    # n.b. It seems that no references-case on the statement starts with "primary key".
                    #      The statement starts with "primary key" means only using pre-defined cols to define pk,
                    #      the statement doesn't start with "primary key" means both create a new col and define a pk.
                    if clause_lower.startswith("primary key"):
                        try:
                            # pk_cols = fmt_str(clause.split("key")[1].split('(')[1].split(')')[0])
                            pk_cols = fmt_str(split_string(clause, "key").split('(')[1].split(')')[0])
                        except:
                            continue

                        if self.is_pk_ref_valid(tab_obj, pk_cols):
                            try:
                                pk_cols = get_column_object(tab_obj, pk_cols)
                                pk_obj = File.construct_key_obj("PrimaryKey", pk_cols)
                                tab_obj.key_list.append(pk_obj)
                            except:
                                continue
                        else:
                            # raise Exception("PRIMARY KEY def error: references on create table not found!")
                            # print("PRIMARY KEY def error: references on create table not found!")
                            COUNTER_EXCEPT.add()
                            continue
                    else:
                        try:
                            # pk_col_defs = clause.split("primary key")[0].split()
                            pk_col_defs = split_string(clause, "primary key", get_first=True).split()
                            pk_col = fmt_str(pk_col_defs[0].strip())
                            # pk_col_type = fmt_str(pk_col_defs[1].strip())  # unused for now
                        except:
                            continue

                        # handle the omission clause for both indicate pk and fk,
                        # e.g. [key_col] INTEGER PRIMARY KEY REFERENCES [ref_tab] ([ref_cols])
                        if " references " in clause_lower:
                            try:
                                # fk_ref_def = clause.split("references")[1].strip().split(')', maxsplit=1)[0].split('(')
                                fk_ref_def = split_string(clause, "references").strip().split(')', maxsplit=1)[0].split('(')
                                # fk_ref_tab = fmt_str(fk_ref_def[0].split("on")[0]) if len(fk_ref_def) == 1 else fmt_str(fk_ref_def[0])
                                fk_ref_tab = fmt_str(split_string(fk_ref_def[0], "on", get_first=True)) if len(fk_ref_def) == 1 else fmt_str(fk_ref_def[0])
                                fk_ref_cols = fmt_str(fk_ref_def[1]) if len(fk_ref_def) == 2 else None
                                # print("FOREIGN KEY def error: references on create table not found!")
                                self.memo.add((tab_name, pk_col, fk_ref_tab, fk_ref_cols))
                                COUNTER_EXCEPT.add()
                            except:
                                continue

                        col_obj = Column(pk_col)
                        tab_obj.insert_col(col_obj)
                        tab_obj.col_name_seq.append(pk_col)

                        if self.is_pk_ref_valid(tab_obj, pk_col):
                            try:
                                pk_col = get_column_object(tab_obj, pk_col)
                                pk_obj = File.construct_key_obj("PrimaryKey", pk_col)
                                tab_obj.key_list.append(pk_obj)
                            except:
                                continue
                        else:
                            # raise Exception("PRIMARY KEY def error: references on create table not found!")
                            # print("PRIMARY KEY def error: references on create table not found!")
                            COUNTER_EXCEPT.add()
                            continue
                # handle foreign key
                elif clause_lower.startswith("foreign key"):
                    # n.b. Slightly Similar to primary key, foreign key
                    #      has two different semantics according its keyword position.
                    #      However, one of the variant CONSTRAINT ... has been handled in front.
                    pattern = REGEX_DICT("startwith_fk_create_table")
                    try:
                        result = re.findall(pattern, clause, re.IGNORECASE)[0]
                    except:
                        continue
                    # fk must have references, so its matching length is 3.
                    # FOREIGN KEY([fk_name]) REFERENCES [ref_tab_name]([ref_col_name])
                    if len(result) == 3:
                        fk_cols = fmt_str(result[0])
                        fk_ref_tab = fmt_str(result[1])
                        fk_ref_cols = fmt_str(result[2])
                    else:
                        # raise Exception("FOREIGN KEY def error: match number must be 3!")
                        # print("FOREIGN KEY def error: match number must be 3!")
                        COUNTER_EXCEPT.add()
                        continue
                    if self.is_fk_ref_valid(tab_obj, fk_cols) and \
                       self.is_fk_ref_valid(fk_ref_tab, fk_ref_cols):
                        try:
                            ref_tab_obj = self.repo_name2tab[fk_ref_tab]
                            fk_cols = get_column_object(tab_obj, fk_cols)
                            fk_ref_cols = get_column_object(ref_tab_obj, fk_ref_cols)
                            fk_obj = File.construct_fk_obj(fk_cols, ref_tab_obj, fk_ref_cols)
                            tab_obj.fk_list.append(fk_obj)
                        except:
                            continue
                    else:
                        self.memo.add((tab_name, fk_cols, fk_ref_tab, fk_ref_cols))
                        COUNTER_EXCEPT.add()
                # handle unique key
                elif clause_lower.startswith("unique key"):
                    pattern = REGEX_DICT("startwith_uk_create_table")
                    try:
                        result = re.findall(pattern, clause, re.IGNORECASE)
                    except:
                        continue
                    if len(result) == 1:
                        uk_cols = result[0]
                    else:
                        # raise Exception("UNIQUE KEY defined error: match number must be 1!")
                        # print("UNIQUE KEY defined error: match number must be 1!")
                        COUNTER_EXCEPT.add()
                        continue
                    if self.is_uk_ref_valid(tab_obj, uk_cols):
                        try:
                            uk_cols = get_column_object(tab_obj, uk_cols)
                            uk_obj = File.construct_key_obj("UniqueKey", uk_cols)
                            tab_obj.key_list.append(uk_obj)
                        except:
                            continue
                    else:
                        # raise Exception("UNIQUE KEY ref error: references on create table not found!")
                        # print("UNIQUE KEY ref error: references on create table not found!")
                        COUNTER_EXCEPT.add()
                        continue
                # handle candidate key
                elif len(re.findall("^key\s", clause, re.IGNORECASE)) == 1:
                    # KEY [key_name] ([key_col_0], ...)  # key_name is unused for now.
                    pattern = REGEX_DICT("candidate_key_create_table")
                    try:
                        result = re.findall(pattern, clause, re.IGNORECASE)
                    except:
                        continue
                    if len(result) == 1:
                        key_cols = re.sub(pattern, "", result[0], re.IGNORECASE)  # rm internal parenthesis
                    else:
                        # raise Exception("KEY defined error: match number must be 1!")
                        # print("KEY defined error: match number must be 1!")
                        COUNTER_EXCEPT.add()
                        continue
                    # TODO: check whether have references
                    if " references " in clause_lower:
                        pass
                    if self.is_key_ref_valid(tab_obj, key_cols):
                        try:
                            key_cols = get_column_object(tab_obj, key_cols)
                            key_obj = File.construct_key_obj("CandidateKey", key_cols)
                            tab_obj.key_list.append(key_obj)
                        except:
                            continue
                    else:
                        # raise Exception("KEY ref error: references on create table not found!")
                        # print("KEY ref error: references on create table not found!")
                        COUNTER_EXCEPT.add()
                        continue
                # handle unique index
                elif clause_lower.startswith("unique index"):
                    pattern = REGEX_DICT("startwith_ui_create_table")
                    try:
                        result = re.findall(pattern, clause, re.IGNORECASE)[0]
                    except:
                        continue
                    if len(result) == 2:
                        # uniq_idx_name = result[0]
                        ui_cols = result[1]
                    else:
                        # raise Exception("UNIQUE INDEX defined error: match number must be 2!")
                        # print("UNIQUE INDEX defined error: match number must be 2!")
                        COUNTER_EXCEPT.add()
                        continue
                    if self.is_ui_ref_valid(tab_obj, ui_cols):
                        try:
                            ui_cols = get_column_object(tab_obj, ui_cols)
                            ui_obj = File.construct_key_obj("UniqueIndex", ui_cols)
                            tab_obj.key_list.append(ui_obj)
                        except:
                            continue
                    else:
                        # raise Exception("UNIQUE INDEX ref error: references on create table not found!")
                        # print("UNIQUE INDEX ref error: references on create table not found!")
                        COUNTER_EXCEPT.add()
                        continue
                # handle: UNIQUE ([uni_cols])
                elif clause_lower.startswith("unique "):
                    pattern = REGEX_DICT("startwith_unique_create_table")
                    try:
                        result = re.findall(pattern, clause, re.IGNORECASE)
                    except:
                        continue
                    if len(result) == 1:
                        uk_cols = re.sub("(\(.*\))", "", result[0], re.IGNORECASE)
                    else:
                        # raise Exception("UNIQUE def error: match number must be 1!")
                        # print("UNIQUE def error: match number must be 1!")
                        COUNTER_EXCEPT.add()
                        continue
                    if self.is_ui_ref_valid(tab_obj, uk_cols):
                        try:
                            uk_cols = get_column_object(tab_obj, uk_cols)
                            uk_obj = File.construct_key_obj("UniqueKey", uk_cols)
                            tab_obj.key_list.append(uk_obj)
                        except:
                            continue
                    else:
                        # raise Exception("UNIQUE def error: references on create table not found!")
                        # print("UNIQUE def error: references on create table not found!")
                        COUNTER_EXCEPT.add()
                        continue
                # handle index
                elif clause_lower.startswith("index"):
                    pattern = REGEX_DICT("startwith_index_create_table")
                    try:
                        result = re.findall(pattern, clause, re.IGNORECASE)
                    except:
                        continue
                    if len(result) == 1:
                        index_cols = rm_kw(result[0])
                    else:
                        # raise Exception("INDEX defined error: match number must be 1!")
                        # print("INDEX defined error: match number must be 1!")
                        COUNTER_EXCEPT.add()
                        continue
                    if self.is_ui_ref_valid(tab_obj, index_cols):
                        try:
                            index_cols = get_column_object(tab_obj, index_cols)
                            index_obj = File.construct_key_obj("Index", index_cols)
                            tab_obj.key_list.append(index_obj)
                        except:
                            continue
                    else:
                        # raise Exception("INDEX ref error: references on create table not found!")
                        # print("INDEX ref error: references on create table not found!")
                        COUNTER_EXCEPT.add()
                        continue
                # handle ordinary col definition
                # TODO: handle the col with references
                #       (e.g. UserID integer REFERENCES users (UserID) ON DELETE CASCADE,
                #             array_id bigint references "array" (id) on delete cascade)
                elif "references" in clause_lower:
                    try:
                        result = re.findall("(.*?)\s(.*?)\s.*references", clause, re.IGNORECASE)[0]
                        col_name, col_type = fmt_str(result[0]), fmt_str(result[1])
                    except:
                        continue
                    else:
                        if col_name == "":
                            continue
                        if not any(known_type in col_type.lower() for known_type in COL_DATA_TYPES):
                            continue

                        col_obj = Column(col_name)

                        # handle UNIQUE constraint in ordinary column
                        if "unique" in clause_lower:
                            uniq_col_obj = File.construct_key_obj("UniqueColumn", [col_obj])
                            tab_obj.key_list.append(uniq_col_obj)

                        if "not null" in clause_lower:
                            col_obj.is_notnull = True
                        # add col_obj into table_obj
                        tab_obj.insert_col(col_obj)
                        tab_obj.col_name_seq.append(col_name)
                else:
                    if len(clause.split()) == 1:
                        # print(f"too few cols in clause to parse! | {clause}")
                        continue
                    # n.b.
                    # here are two branches to extract `c_name` and `c_type`:
                    # 1) as to the situation can only appears with punc wrapped which allows space in col name,
                    #    detect if clause includes punc like `, ', ", if True, extract by regex.
                    # 2) if False, extract by simple split on space,
                    #    it could asure that col name which not includes space would not be split unexpectedly.
                    # col_defs = clause.split(" default ")[0].split(" comment ")[0].strip()
                    col_defs = split_string(split_string(clause, " default ", get_first=True), " comment ", get_first=True).strip()
                    if '`' in col_defs or '\'' in col_defs or '"' in col_defs:
                        try:
                            result = re.findall("([`|'|\"].*?[`|'|\"])", col_defs, re.IGNORECASE)[0]
                        except:
                            # raise Exception("Regex match failed!" + traceback.format_exc())
                            # print("Regex match failed!" + traceback.format_exc())
                            continue
                        else:
                            c_name = fmt_str(result)
                            c_type_splt = clause.split(result)
                            # c_type = clause.split(result)[1].split()[0]
                            if len(c_type_splt) == 1:
                                c_type = "int"
                            elif len(c_type_splt) > 1:
                                c_type = c_type_splt[1].strip()
                            else:
                                c_type = "int"
                    else:
                        splt = col_defs.split()
                        if not splt:
                            continue
                        c_name = fmt_str(splt[0])
                        try:
                            c_type = fmt_str(splt[1])
                        except:
                            c_type = "int"

                    if c_name == "":
                        continue
                    if not any(known_type in c_type.lower() for known_type in COL_DATA_TYPES):
                        # print('unrecognized type: ' + c_type)
                        continue

                    col_obj = Column(c_name)

                    # handle UNIQUE constraint in ordinary column
                    if "unique" in clause_lower:
                        uniq_col_obj = File.construct_key_obj("UniqueColumn", [col_obj])
                        tab_obj.key_list.append(uniq_col_obj)

                    if "not null" in clause_lower:
                        col_obj.is_notnull = True
                    # add col_obj into table_obj
                    tab_obj.insert_col(col_obj)
                    tab_obj.col_name_seq.append(c_name)
            return tab_obj if len(tab_obj.name2col) != 0 else None
        except Exception as e:
            # print()
            print("create table parse error↓")
            print(stmt)
            logging.exception(e)
            COUNTER_EXCEPT.add()
            return None

    def parse_one_statement_create_as_select(self, stmt):
        pattern_repl = re.compile("\(.*?\)", re.IGNORECASE)
        stmt = re.sub(pattern_repl, "", stmt)
        if "create temporary table" in stmt.lower():
            pattern_repl = re.compile("create temporary table", re.IGNORECASE)
            stmt = re.sub(pattern_repl, "create table", stmt)
        elif "create view" in stmt.lower():
            pattern_repl = re.compile("create view", re.IGNORECASE)
            stmt = re.sub(pattern_repl, "create table", stmt)
        table_name = fmt_str(split_string(split_string(split_string(split_string(stmt, "create table", 1, get_first=False),
                                                       "as", 1, get_first=True),
                                                       "if not exists", 1, get_first=False),
                                          "references", 1, get_first=True))
        table_name = table_name.split()[0].strip()
        # if table_name in self.repo_name2tab:
        lower2name2tab = {k.lower(): (k, v) for k, v in self.repo_name2tab.items()}
        tab_obj = lower2name2tab[table_name.lower()][1] if table_name.lower() in lower2name2tab else Table(table_name, self.hashid)
        columns = fmt_str(split_string(split_string(stmt, "select", 1, get_first=False), "from", 1, get_first=True)).replace("distinct", "").replace("DISTINCT", "").replace("Distinct", "").split(',')
        columns = [c.strip() for c in columns]
        column_list = list()
        for c in columns:
            c = c.rsplit()[-1].strip() if ' ' in c else c
            if ".*" in c and c.rsplit(".*", 1)[0].lower() in lower2name2tab:
                another_tab_obj = lower2name2tab[c.rsplit(".*", 1)[0].lower()][1]
                for col_obj in another_tab_obj.name2col:
                    if col_obj.col_name not in tab_obj.name2col:
                        new_col_obj = Column(col_obj.col_name)
                        tab_obj.insert_col(new_col_obj)
                        tab_obj.col_name_seq.append(col_obj.col_name)
                        column_list.append(col_obj.col_name)
                continue
            elif " as " in c.lower():
                # col = c.split(" as ", 1)[-1].strip()
                col = re.split(" as | AS | As | aS ", c, 1)[-1].strip()
                column_list.append(col)
            elif ".*" not in c and '.' in c:
                col = c.rsplit('.', 1)[-1].strip()
                column_list.append(col)
            elif '*' in c:
                try:
                    from_table = fmt_str(split_string(stmt, " from ", 1, get_first=False).strip().split()[0].strip())
                    another_tab_obj = lower2name2tab[from_table.lower()][1]
                    for col_obj in another_tab_obj.name2col:
                        if col_obj.col_name not in tab_obj.name2col:
                            new_col_obj = Column(col_obj.col_name)
                            tab_obj.insert_col(new_col_obj)
                            tab_obj.col_name_seq.append(col_obj.col_name)
                            column_list.append(col_obj.col_name)
                    continue
                except:
                    continue
            else:
                col = c.strip()
                column_list.append(col)
            if col not in tab_obj.name2col:
                col_obj = Column(col)
                tab_obj.insert_col(col_obj)
                tab_obj.col_name_seq.append(col)
        print("input create table as stmt:", stmt)
        print(f"create table as select succ: table: {table_name}, columns: {column_list}")
        # pprint(tab_obj.name2col)
        return tab_obj

    def parse_one_statement_alter_table(self, stmt):
        """Parse a SQL statement on alter table.

        Params
        ------
        - stmt: str

        Returns
        -------
        - None
        """
        try:
            stmt = stmt.replace(" only ", " ")
            stmt = stmt.replace(" ONLY ", " ")
            # parse table name
            # tab_name = fmt_str(stmt.split('alter table')[1].split()[0])
            tab_name_raw = split_string(stmt, "alter table").strip().split()[0]
            tab_name = fmt_str(tab_name_raw)
            # tab_name = fmt_str(re.match(REGEX_DICT("get_alter_table_name"), stmt, re.IGNORECASE).group(2))
            lower2name2tab = {k.lower(): (k, v) for k, v in self.repo_name2tab.items()}
            if tab_name.lower() not in lower2name2tab:
                print(f"Did not find this table on alter table: {tab_name}")
                # if " " in tab_name:
                # return
                tab_obj = Table(tab_name, self.hashid)
                self.repo_name2tab[tab_name] = tab_obj
            else:
                tab_obj = lower2name2tab[tab_name.lower()][1]

            # Parse key cols on alter table
            # EXAMPLE:
            # ```SQL
            # ALTER TABLE `songs`
            # ADD CONSTRAINT `album_name_constraint` FOREIGN KEY (`song_id`, `song_artist`, `song_year`) REFERENCES `albums` (`album_id`, `album_artist`, `album_year`) ON DELETE NO ACTION ON UPDATE NO ACTION,
            # ADD CONSTRAINT `price_constraint` FOREIGN KEY (`song_price`) REFERENCES `daily_song_price` (`single_song_price`) ON DELETE NO ACTION ON UPDATE NO ACTION;
            # ```
            # will be masked after executed:
            # ```SQL
            # ALTER TABLE `songs`
            # ADD CONSTRAINT `album_name_constraint` FOREIGN KEY [MULTI-COL] REFERENCES `albums` [MULTI-COL] ON DELETE NO ACTION ON UPDATE NO ACTION,
            # ADD CONSTRAINT `price_constraint` FOREIGN KEY (`song_price`) REFERENCES `daily_song_price` (`single_song_price`) ON DELETE NO ACTION ON UPDATE NO ACTION;
            # ```

            # Mask multi columns for protection.
            # Firstly, preserve all the multi columns by their match order and append each of them to a list,
            # then replace all the multi columns as [MULTI-COL],
            # after split statements, restore the multi columns to their content by order in list.
            pattern = re.compile("\(.*?\)", re.IGNORECASE)
            multicol_list = pattern.findall(stmt)
            # stmt = re.sub("\(.*?\)", "[MULTI-COL]", stmt, re.IGNORECASE)
            stmt = pattern.sub("[MULTI-COL]", stmt)
            # clauses = stmt.split("alter table")[1].replace(tab_name, "").strip().split(',')
            # clauses = split_string(stmt, "alter table").replace(tab_name, "").strip().split(',')
            clauses = fmt_str(split_string(stmt, "alter table").replace(tab_name_raw, "")).split(',')
            with Timeout(seconds=1):
                # potential memory leak here, could be handled better.
                if len(multicol_list) != 0:
                    temp_list = list()
                    i = 0
                    while i in range(len(multicol_list)):
                        for c in clauses:
                            while "[MULTI-COL]" in c and i < len(multicol_list):
                                multicol = multicol_list[i]
                                c = c.replace("[MULTI-COL]", multicol, 1)
                                i += 1
                            temp_list.append(c)
                    clauses = temp_list

            # Parse each sub clause according its constraint type
            for clause in [c.strip() for c in clauses]:
                clause_lower = clause.lower()
                # handle pk on alter table for two variants.
                if "primary key" in clause_lower:
                    if "add constraint" in clause_lower:
                        pattern = REGEX_DICT("add_constraint_pk_alter_table")
                        # clause = clause.split("add constraint")[1].strip()
                        clause = split_string(clause, "add constraint").strip()
                        try:
                            result = re.findall(pattern, clause, re.IGNORECASE)[0]
                        except:
                            continue
                        if isinstance(result, str):
                            pk_cols = fmt_str(result)
                        else:
                            raise Exception("ADD CONSTRAINT PRIMARY KEY error: match number must be 1!")
                    elif "add primary key" in clause_lower:
                        pattern = REGEX_DICT("add_pk_alter_table")
                        # clause = clause.split("add primary key")[1].strip()
                        clause = split_string(clause, "add primary key").strip()
                        result = re.findall(pattern, clause, re.IGNORECASE)[0]
                        if isinstance(result, str):
                            pk_cols = fmt_str(result)
                        else:
                            raise Exception("ADD PRIMARY KEY error: match number not equal to 1!")
                    else:
                        raise Exception(f"Unknown pk variant: {clause}")
                    if self.is_pk_ref_valid(tab_obj, pk_cols):
                        pk_cols = get_column_object(tab_obj, pk_cols)
                        pk_obj = File.construct_key_obj("PrimaryKey", pk_cols)
                        tab_obj.key_list.append(pk_obj)
                    else:
                        raise Exception("ADD PRIMARY KEY error: column(s) on alter table not found!")
                elif "foreign key" in clause_lower:
                    # handle fk on alter table for two variants.
                    # 1) ADD CONSTRAINT [fk_alias] FOREIGN KEY([fk_col(s)]) REFERENCES [ref_table_name] ([ref_col_name])
                    # 2) ADD FOREIGN KEY ([fk_col(s)]) REFERENCES [ref_table_name] ([ref_col_name])
                    if "add constraint" in clause_lower:
                        pattern = REGEX_DICT("add_constraint_fk_alter_table")
                        # multi alter statement for add constraint fk
                        # clause = clause.split("add constraint")[1].strip()
                        clause = split_string(clause, "add constraint").strip()
                        try:
                            result = re.findall(pattern, clause, re.IGNORECASE)[0]
                        except:
                            continue
                        # fk must have reference, so its len is 3 at least.
                        # 1. ADD CONSTRAINT [alias] FOREIGN KEY([fk_name]) REFERENCES [ref_table_name]([ref_col_name])
                        if len(result) == 3:
                            fk_cols, fk_ref_tab, fk_ref_cols = result
                            fk_cols = fmt_str(fk_cols)
                            fk_ref_tab = fmt_str(fk_ref_tab)
                            fk_ref_cols = fmt_str(fk_ref_cols)
                        else:
                            raise Exception("ADD CONSTRAINT FOREIGN KEY error: match number not equal to 3!")
                    elif "add foreign key" in clause_lower:
                        pattern = REGEX_DICT("add_fk_alter_table")
                        # clause = clause.split("add foreign key")[1].strip()
                        clause = split_string(clause, "add foreign key").strip()
                        try:
                            result = re.findall(pattern, clause, re.IGNORECASE)[0]
                        except:
                            continue
                        if len(result) == 3:
                            fk_cols, fk_ref_tab, fk_ref_cols = result
                            fk_cols = fmt_str(fk_cols)
                            fk_ref_tab = fmt_str(fk_ref_tab)
                            fk_ref_cols = fmt_str(fk_ref_cols)
                        else:
                            raise Exception("ADD FOREIGN KEY error: match number not equal to 3!")
                    else:
                        raise Exception(f"Unknown fk variant: {clause}")
                    # check fk cols and its ref are valid or not
                    if self.is_fk_ref_valid(tab_obj, fk_cols) and \
                       self.is_fk_ref_valid(fk_ref_tab, fk_ref_cols):
                        # print(f"| <foreign_key_cols:\"{fmt_str(fk_cols)}\"> | <ref_table_name:\"{fmt_str(fk_ref_tab)}\"> | <ref_cols:\"{fmt_str(fk_ref_cols)}\"> |")
                        ref_tab_obj = self.repo_name2tab[fk_ref_tab]
                        fk_cols = get_column_object(tab_obj, fk_cols)
                        fk_ref_cols = get_column_object(ref_tab_obj, fk_ref_cols)
                        fk_obj = File.construct_fk_obj(fk_cols, ref_tab_obj, fk_ref_cols)
                        tab_obj.fk_list.append(fk_obj)
                    else:
                        self.memo.add((tab_name, fk_cols, fk_ref_tab, fk_ref_cols))
                        COUNTER_EXCEPT.add()
                        # print("ADD FOREIGN KEY error: references on alter table not found!")
                        # raise Exception("ADD FOREIGN KEY error: references on alter table not found!")
                elif "unique" in clause_lower:
                    # 1) handle ADD UNIQUE KEY
                    if "add unique key" in clause_lower:
                        # clause = clause.split("add unique key")[1].strip()
                        clause = split_string(clause, "add unique key").strip()
                        pattern = REGEX_DICT("add_unique_key_alter_table")
                        try:
                            result = re.findall(pattern, clause, re.IGNORECASE)[0]
                        except:
                            continue
                        if len(result) == 2:
                            # uniq_key_name = fmt_str(result[0])  # unused for now
                            uk_cols = fmt_str(result[1])
                        else:
                            raise Exception("ADD UNIQUE KEY error: match number not equal to 2!")
                        if self.is_uk_ref_valid(tab_obj, uk_cols):
                            uk_cols = get_column_object(tab_obj, uk_cols)
                            uk_obj = File.construct_key_obj("UniqueKey", uk_cols)
                            tab_obj.key_list.append(uk_obj)
                        else:
                            raise Exception("ADD UNIQUE KEY error: references on alter table not found!")
                    # 2) handle ADD UNIQUE INDEX
                    elif "add unique index" in clause_lower:
                        pattern = REGEX_DICT("add_unique_index_alter_table")
                        # clause = clause.split("add unique index")[1].strip()
                        clause = split_string(clause, "add unique index").strip()
                        try:
                            result = re.findall(pattern, clause, re.IGNORECASE)
                        except:
                            continue
                        if len(result) == 1:
                            ui_cols = fmt_str(result[0])
                        else:
                            raise Exception("ADD UNIQUE INDEX error: match number not equal to 1!")
                        if self.is_ui_ref_valid(tab_obj, ui_cols):
                            ui_cols = get_column_object(tab_obj, ui_cols)
                            ui_obj = File.construct_key_obj("UniqueIndex", ui_cols)
                            tab_obj.key_list.append(ui_obj)
                        else:
                            raise Exception("ADD UNIQUE INDEX error: references on alter table not found!")
                    # 3) handle ADD CONSTRAINT UNIQUE KEY
                    elif "add constraint" in clause_lower:
                        pattern = REGEX_DICT("add_constraint_unique_alter_table")
                        try:
                            result = re.findall(pattern, clause, re.IGNORECASE)
                        except:
                            continue
                        if len(result) == 1:
                            uk_cols = fmt_str(result[0])
                        else:
                            raise Exception("ADD CONSTRAINT UNIQUE error: match number not equal to 1!")
                        if self.is_uk_ref_valid(tab_obj, uk_cols):
                            uk_cols = get_column_object(tab_obj, uk_cols)
                            uk_obj = File.construct_key_obj("UniqueKey", uk_cols)
                            tab_obj.key_list.append(uk_obj)
                        else:
                            raise Exception("ADD CONSTRIANT UNIQUE error: references on alter table not found!")
                    # 4) handle CREATE UNIQUE [constraint_name] INDEX
                    elif len(re.findall("create\s+unique\s*(clustered|nonclustered)?\s+index", clause_lower, re.IGNORECASE)) == 1:
                        pattern = REGEX_DICT("create_unique_index_alter_table")
                        try:
                            result = re.findall(pattern, clause, re.IGNORECASE)[0]
                        except:
                            continue
                        if len(result) == 2 or len(result) == 3:
                            # ref_tab = fmt_str(result[0])
                            ref_cols = fmt_str(result[1])
                            # asc_or_desc = fmt_str(result[2])  # unused for now
                        else:
                            raise Exception("CREATE UNIQUE INDEX error: match number not equal to 2 or 3!")
                        if self.is_ui_ref_valid(tab_obj, ref_cols):
                            ref_cols = get_column_object(tab_obj, ref_cols)
                            ui_obj = File.construct_key_obj("UniqueIndex", ref_cols)
                            tab_obj.key_list.append(ui_obj)
                        else:
                            raise Exception("CREATE UNIQUE INDEX error: references on alter table not found!")
                    else:
                        raise Exception(f"UNIQUE error: unknown add unique variant! => {clause}")
                # handle add candidate key on alter table
                elif "add key" in clause_lower:
                    pattern = REGEX_DICT("add_key_alter_table")
                    try:
                        result = re.findall(pattern, clause, re.IGNORECASE)
                    except:
                        continue
                    if len(result) == 1:
                        key_cols = fmt_str(result[0])
                    else:
                        raise Exception("ADD KEY error: match number not equal to 1!")
                    if self.is_key_ref_valid(tab_obj, key_cols):
                        key_cols = get_column_object(tab_obj, key_cols)
                        key_obj = File.construct_key_obj("CandidateKey", key_cols)
                        tab_obj.key_list.append(key_obj)
                    else:
                        raise Exception("ADD KEY error: references on alter table not found!")
                elif clause_lower.startswith(("add ", "add column")):
                    if "add column" in clause_lower:
                        tokens = clause.replace("ADD COLUMN", "").replace("add column", "").strip().split()
                    else:
                        tokens = clause.replace("ADD ", "").replace("add ", "").strip().split()
                    try:
                        col_name, col_type = tokens[0], tokens[1]
                    except:
                        continue
                    if not any(known_type in col_type.lower() for known_type in COL_DATA_TYPES):
                        continue
                    if col_name in tab_obj.name2col:
                        continue
                    if ' ' in col_name:
                        continue
                    tab_obj.name2col[col_name] = Column(col_name)
                else:
                    # print(f"Unhandled operation on alter table: {clause}")
                    pass
        except Exception as e:
            # print("alter table parse error↓")
            # logging.exception(e)
            COUNTER_EXCEPT.add()
            return None

    def parse_one_statement_create_index(self, stmt):
        """Parse a SQL statement on create (unique) index.

        Params
        ------
        - stmt: str

        Returns
        -------
        - None
        """
        stmt = fmt_str(stmt)
        stmt_lower = stmt.lower()
        def remove_keyword(s): return s.replace(" DESC", "").replace(" desc", "").replace(" NULLS", "").replace(" nulls", "").replace(" LAST", "").replace(" last", "")
        try:
            pattern = REGEX_DICT("create_index_or_unique_index")
            result = re.findall(pattern, stmt, re.IGNORECASE)[0]
            if len(result) == 3:
                idx_tab_name = fmt_str(result[0])
                # idx_type = fmt_str(result[1])  # unused for now
                idx_cols = fmt_str(remove_keyword(result[2]))
            else:
                raise Exception("CREATE INDEX error: match number must be 3!")
            if self.is_ui_ref_valid(idx_tab_name, idx_cols):
                tab_obj = self.repo_name2tab[idx_tab_name]
                idx_cols = get_column_object(tab_obj, idx_cols)
                idx_obj = File.construct_key_obj("UniqueIndex", idx_cols)\
                    if "create unique index" in stmt_lower else File.construct_key_obj("Index", idx_cols)
                tab_obj.key_list.append(idx_obj)
            else:
                raise Exception("CREATE INDEX error: references on CREATE INDEX not found!")
        except Exception as e:
            # print("create index parse error↓")
            # logging.exception(e)
            COUNTER_EXCEPT.add()
            return None

    def _parse_one_statement_insert(self, stmt):
        # print("input insert statement:", stmt)
        stmt_lower = stmt.lower()
        stmt = fmt_str(split_string(stmt, "values", get_first=True))
        if '(' not in stmt or ')' not in stmt:
            return
        name2tab = self.repo_name2tab
        if "insert into" in stmt_lower:
            pattern = "insert\s+into\s+(.*?)\s*\((.*?)\)"
        elif "insert" in stmt_lower:
            pattern = "insert\s+(.*?)\s*\((.*?)\)"
        else:
            return
        result = re.findall(pattern, stmt, re.IGNORECASE)
        if len(result) == 2:
            table_name = fmt_str(result[0])
            insert_cols = [c.strip() for c in fmt_str(result[1]).split(',')]
        elif len(result) == 1 and len(result[0]) == 2:
            table_name = fmt_str(result[0][0])
            insert_cols = [c.strip() for c in fmt_str(result[0][1]).split(',')]
        else:
            return
        table_name_cmp = table_name.rsplit('.', 1)[-1].replace('[', '').replace(']', '') \
            if '.' in table_name else table_name.replace('[', '').replace(']', '')
        lower2name2tab = {k.lower(): (k, v) for k, v in name2tab.items()}
        lower2name2tab |= {k.lower().rsplit('.', 1)[-1].replace('[', '').replace(']', ''): (k, v)
                           for k, v in name2tab.items()
                           if '.' in k and k.lower().rsplit('.', 1)[-1].replace('[', '').replace(']', '') not in lower2name2tab}
        if table_name_cmp.lower() in lower2name2tab:
            table_obj = lower2name2tab[table_name_cmp.lower()][1]
        elif ' ' in table_name:
            return
        else:
            table_obj = Table(table_name, self.hashid)
            name2tab[table_name] = table_obj
        lower2name2col = {k.lower(): (k, v) for k, v in table_obj.name2col.items()} | \
            {k.lower().replace('[', '').replace(']', ''): (k, v) for k, v in table_obj.name2col.items()} |\
            {'[' + k.lower() + ']': (k, v) for k, v in table_obj.name2col.items() if '[' not in k and ']' not in k}

        for col in insert_cols:
            if ' ' in col:
                continue
            if col.lower() not in lower2name2col:
                table_obj.name2col[col] = Column(col)

    def parse_one_statement(self, stmt):
        """Parse single SQL statement splitted by semicolon `;`

        Params
        ------
        - stmt: str

        Returns
        -------
        - None
        """
        try:
            with Timeout(seconds=1):
                stmt = sqlparse.format(stmt, strip_comments=True)
        except Exception as e:
            # print(e)
            return
        else:
            stmt = ' '.join(stmt.split())
            stmt_lower = stmt.lower()

        # preprocess statement
        stmt = clean_stmt(stmt)
        # skip empty string
        if stmt == "":
            return
        elif ("create table" in stmt_lower or "create view" in stmt_lower) and " as select " in stmt_lower:
            COUNTER.add()
            try:
                # print("input create table as stmt:", stmt)
                tab_obj = self.parse_one_statement_create_as_select(stmt)
            except Exception as e:
                COUNTER_CT_EXCEPT.add()
                print("create as select error")
                logging.exception(e)
                return
            else:
                if tab_obj.tab_name not in self.multi_name2tab:
                    self.multi_name2tab[tab_obj.tab_name] = set()
                self.multi_name2tab[tab_obj.tab_name].add(tab_obj)
                self.repo_name2tab[tab_obj.tab_name] = self.get_max_col_nums_table(tab_obj) \
                    if tab_obj.tab_name in self.repo_name2tab else tab_obj
                # print(f"table name: {tab_obj.tab_name} => {self.repo_name2tab[tab_obj.tab_name]}")
        elif "create table" in stmt_lower or "create temporary table" in stmt_lower:
            try:
                COUNTER_CT.add()
                # print("input create stmt:", stmt)
                tab_obj = self.parse_one_statement_create_table(stmt)
            except:
                COUNTER_CT_EXCEPT.add()
                return
            else:
                if tab_obj is None:
                    COUNTER_CT_EXCEPT.add()
                    return
                if tab_obj.tab_name not in self.multi_name2tab:
                    self.multi_name2tab[tab_obj.tab_name] = set()
                self.multi_name2tab[tab_obj.tab_name].add(tab_obj)
                self.repo_name2tab[tab_obj.tab_name] = self.get_max_col_nums_table(tab_obj) \
                    if tab_obj.tab_name in self.repo_name2tab else tab_obj
                # print(f"table name: {tab_obj.tab_name} => {self.repo_name2tab[tab_obj.tab_name]}")
                COUNTER_CT_SUCC.add()
        elif "alter table" in stmt_lower:
            COUNTER.add()
            self.parse_one_statement_alter_table(stmt)
        elif "create index" in stmt_lower or "create unique index" in stmt_lower:
            COUNTER.add()
            self.parse_one_statement_create_index(stmt)
        elif "insert into" in stmt_lower or "insert" in stmt_lower:
            try:
                stmt = stmt[stmt_lower.index("insert"):]
                self._parse_one_statement_insert(stmt)
            except Exception as e:
                print("parse insert error↓")
                logging.exception(e)
        else:
            # check if the input statement is supported.
            # raise Exception(f"Unhandled table operation: {stmt}")
            # print(f"Unhandled table operation: {stmt}")
            pass

    def parse(self, stmts, stage):
        """Parse input SQL statements.

        Params
        ------
        - stmts: str
        - stage: enum

        Returns
        -------
        - None
        """
        def add_semicolon(s):
            # TODO: choose another approach to split
            pat_ct = re.compile("create table", re.IGNORECASE)
            pat_at = re.compile("alter table", re.IGNORECASE)
            pat_ci = re.compile("create index", re.IGNORECASE)
            pat_cui = re.compile("create unique index", re.IGNORECASE)
            # pat_sel = re.compile("select", re.IGNORECASE)
            pat_ii = re.compile("insert into", re.IGNORECASE)
            pat_cv = re.compile("create view", re.IGNORECASE)
            s = pat_ct.sub(";\ncreate table", s)
            s = pat_at.sub(";\nalter table", s)
            s = pat_ci.sub(";\ncreate index", s)
            s = pat_cui.sub(";\ncreate unique index", s)
            # s = pat_sel.sub(";\nselect", s)
            s = pat_ii.sub(";\ninsert into", s)
            s = pat_cv.sub(";\ncreate view", s)
            return s
            # return s\
            # .replace("create table", ";\ncreate table")\
            # .replace("alter table", ";\nalter table")\
            # .replace("create index", ";\ncreate index")\
            # .replace("create unique index", ";\ncreate unique index")\
            # .replace("select", ";\nselect")\
            # .replace("insert into", ";\ninsert into")\
            # .replace("create view", ";\ncreate view")

        # stmts = stmts.lower().split(';')
        if stage != ParseStage.query:
            # stmts = stmts.lower().split("\n\n")
            stmts = add_semicolon(stmts).split(';')
            """
            if ';' in stmts:
                stmts = stmts.lower().split(';')
            else:
                # split statements by adding semicolon manually
                stmts = add_semicolon(stmts.lower()).split(';')
            """

        if stage == ParseStage.create:
            stmts = [s for s in stmts if "create table" in s.lower() or "create temporary table" in s.lower()]
        elif stage == ParseStage.alter:
            stmts = [s for s in stmts if "alter table" in s.lower() or "create index" in s.lower() or "create unique index" in s.lower()]
        elif stage == ParseStage.insert:
            stmts = [s for s in stmts if "insert into" in s.lower() or "insert" in s.lower()]
        elif stage == ParseStage.query:
            pass
        for s in stmts:
            if len(s) < STATEMENT_SIZE_LIMIT:
                self.parse_one_statement(s)
            else:
                print("skipping a long statement")


def get_column_object(table_obj, cols_name_str):
    """column names to a list of column objects."""
    col_obj_list = list()
    col_name_list = fmt_str(cols_name_str).split(',')
    col_name_list = [rm_kw(c) for c in col_name_list]
    for col_name in col_name_list:
        col_obj = table_obj.name2col[col_name]
        col_obj_list.append(col_obj)
    return col_obj_list


def parse_repo_files(repo_obj):
    """Parse all SQL files in the same repository.
    - first stage: parse all CREATE TABLE statements in files,
                   and record all unresolved referred tuple in repo's memo.
    - second stage: parse repo again to handle ALTER TABLE and CREATE (UNIQUE) INDEX statements.
    - third stage: parse FKs in memo.
    - fourth stage: parse repo for the third time to handle queries with JOINs statements.

    Benefit
    -------
    - Solve the reversed-orders cases(alter first and create later)
    - As much as possible solve the FKs' references missing cases
    - Aggregate and parse SQL files in the same source repository, easy to manage

    Params
    ------
    - repo_obj: Repository

    Returns
    -------
    - Repository
    """
    fpath_list = repo_obj.repo_fpath_list
    file_obj_queue = deque()
    repo_memo = dict()
    repo_query_list = list()
    all_check_failed_cases = list()
    multi_name2tab = dict()
    unfound_tables = list()
    for stage in ParseStage:
        print('=' * 30, stage, '=' * 30)
        for fp in fpath_list:
            with Timeout(3):
                if stage == ParseStage.create:
                    print('-' * 90)
                    print(f"{stage}:\t{fp}")
                if stage == ParseStage.create:
                    # handle CREATE TABLE clauses
                    # fp = "/datadrive/yang/exp/data/s3_sql_files_crawled_all_vms/4986571943599317614.sql"
                    with open(fp, encoding="utf-8", errors="ignore") as f:
                        hashid = fp.split('/')[-1]
                        lines = f.readlines()
                        file_obj = File(hashid, repo_obj.name2tab, multi_name2tab)
                        try:
                            stmts = ''.join(lines)
                            # stmts = convert_camel_to_underscore(stmts)
                            file_obj.parse(stmts, stage)
                        except Exception as e:
                            print("first stage failed | ", e)
                        finally:
                            # whatever parse success or failed, push file_obj into queue
                            file_obj_queue.append(file_obj)
                            # repo_memo[file_obj.hashid] = deepcopy(file_obj.memo)
                elif stage == ParseStage.alter:
                    # handle ALTER TABLE clauses
                    with open(fp, encoding="utf-8", errors="ignore") as f:
                        lines = f.readlines()
                        with Pipeline(file_obj_queue) as file_obj:
                            try:
                                stmts = ''.join(lines)
                                # stmts = convert_camel_to_underscore(stmts)
                                file_obj.parse(stmts, stage)
                            except Exception as e:
                                print("second stage failed | ", e)
                            finally:
                                # whatever parse success or failed, append file_obj to queue
                                if len(file_obj.memo) == 0:
                                    continue
                                repo_memo[file_obj.hashid] = deepcopy(file_obj.memo)
                elif stage == ParseStage.insert:
                    # handle INSERT (INTO) clauses
                    # fp = "/datadrive/yang/exp/data/s3_sql_files_crawled_all_vms/8348806408482661630.sql"
                    with open(fp, encoding="utf-8", errors="ignore") as f:
                        lines = f.readlines()
                        with Pipeline(file_obj_queue) as file_obj:
                            try:
                                stmts = ''.join(lines)
                                # stmts = convert_camel_to_underscore(stmts)
                                file_obj.parse(stmts, stage)
                            except Exception as e:
                                print("third stage failed | ", e)
                elif stage == ParseStage.fk:
                    # handle FKs in each `file_obj.memo`
                    # n.b. according missing referred table name,
                    #      search if there is matched table object in repo.name2tab and its cols in tab_obj.name2col.
                    with Pipeline(file_obj_queue) as file_obj:
                        if len(file_obj.memo) != 0:
                            for item in file_obj.memo:
                                tab_name, fk_col_name, ref_tab_name, ref_col_name = item
                                if tab_name in repo_obj.name2tab \
                                        and ref_tab_name in repo_obj.name2tab\
                                        and file_obj.is_fk_ref_valid(tab_name, fk_col_name) \
                                        and file_obj.is_fk_ref_valid(ref_tab_name, ref_col_name):
                                    tab_obj = repo_obj.name2tab[tab_name]
                                    ref_tab_obj = repo_obj.name2tab[ref_tab_name]
                                    try:
                                        fk_col_name = get_column_object(tab_obj, fk_col_name)
                                        ref_col_name = get_column_object(ref_tab_obj, ref_col_name)
                                    except:
                                        continue
                                    fk_obj = File.construct_fk_obj(fk_col_name, ref_tab_obj, ref_col_name)
                                    tab_obj.fk_list.append(fk_obj)
                                    # remove handled items in repo_memo
                                    COUNTER_EXCEPT.minus()
                                    try:
                                        repo_memo[file_obj.hashid].remove(item)
                                    except:
                                        pass
                                    print(f"Found FK {tab_obj.hashid}:{tab_obj.tab_name} in {ref_tab_obj.hashid}:{ref_tab_obj.tab_name} in memo")
                                else:
                                    print(f"Not found FK {tab_name}:{fk_col_name}:{ref_tab_name}:{ref_col_name} in memo")
                elif stage == ParseStage.query:
                    # handle join-query statement
                    # fp = "/datadrive/yang/exp/data/s3_sql_files_crawled_all_vms/902666163346747082.sql"
                    stmts = query_stmt_split(fp)
                    with Pipeline(file_obj_queue) as file_obj:
                        try:
                            for s in stmts:
                                if len(s) > 30000:
                                    print("skipping a long statement")
                                    continue
                                parser = QueryParser(file_obj.repo_name2tab, is_debug=False)
                                try:
                                    query_obj, check_failed_cases = parser.parse(s)
                                    # query_obj, unfound_list = parser.parse(s)
                                    if query_obj:
                                        repo_query_list.append(query_obj)
                                        COUNTER_QUERY.add()
                                    # unfound_tables += unfound_list
                                    if check_failed_cases:
                                        all_check_failed_cases.append((fp, check_failed_cases))
                                except:
                                    COUNTER_QUERY_EXCEPT.add()
                                    continue
                        except Exception as e:
                            print("fifth stage failed | ", e)
        repo_obj.parsed_file_list = list(file_obj_queue)
        repo_obj.check_failed_cases = all_check_failed_cases
        # repo_obj.memo = repo_memo
        repo_obj.join_query_list = repo_query_list
        if stage == ParseStage.query:
            print(repo_query_list)
            print(f"query_succ: {COUNTER_QUERY.num - COUNTER_QUERY_EXCEPT.num}, query_except: {COUNTER_QUERY_EXCEPT.num}")
        # print(f"succ: {COUNTER.num - COUNTER_EXCEPT.num}, except: {COUNTER_EXCEPT.num}")

    print("repo parse done")
    # self.repo_obj.repo_url
    # repo_obj.unfound_tables = unfound_tables
    global TOTAL_TABLE_NUM
    global REPEAT_NUM
    global NOT_REPEAT_NUM
    TOTAL_TABLE_NUM += len(repo_obj.name2tab)
    print(f"total table nums: {TOTAL_TABLE_NUM}")
    print(f"repeat table nums: {REPEAT_NUM}, not repeat table nums: {NOT_REPEAT_NUM}")
    # print_name2tab(repo_obj.name2tab, multi_name2tab)
    print(f"create table total: {COUNTER_CT.num}, create table succ: {COUNTER_CT_SUCC.num}, create table except: {COUNTER_CT_EXCEPT.num}")
    # print("repo table nums:", len(repo_obj.name2tab))
    return repo_obj if repo_obj.name2tab else None


def print_name2tab(repo_name2tab, multi_name2tab):
    global REPEAT_NUM
    global NOT_REPEAT_NUM
    for _, table_obj in repo_name2tab.items():
        total_lost_col_nums = 0
        print('*' * 120)
        print_table_obj(table_obj)
        print()
        if len(multi_name2tab[table_obj.tab_name]) > 1:
            print("multi_name2tab↓")
            for table in multi_name2tab[table_obj.tab_name]:
                REPEAT_NUM += 1
                if table is not table_obj:
                    print("repeat table↓")
                    print_table_obj(table)
                    lost_cov_nums = calc_col_cov(table_lhs=table_obj, table_rhs=table)
                    total_lost_col_nums += lost_cov_nums
                    # print(f"total col nums: {len(table_obj.name2col)}, lost col nums: {lost_col_nums}")
            print(f"repeat table total col nums: {len(table_obj.name2col)}, \
                repeat table total lost col nums: {total_lost_col_nums}")
        elif len(multi_name2tab[table_obj.tab_name]) == 1:
            NOT_REPEAT_NUM += 1
            print("not repeat table↑")


def dump_parsed_files(parsed_file_list):
    """Dump parsed sql list to a local pickle file.

    Params
    ------
    - parsed_file_list: list

    Returns
    -------
    - None
    """
    pickle_output = os.path.join(OUTPUT_FOLDER, "s4_parsed_sql_file_list.pkl")
    pickle.dump(parsed_file_list, open(pickle_output, "wb"))

    # dump all the parsed sql statements to a csv file
    lm_output = os.path.join(OUTPUT_FOLDER, "s4_parsed_sql_into_lm.csv")
    with open(lm_output, 'w') as f:
        for f_obj in parsed_file_list:
            for tab_name in f_obj.name2tab:
                tab_obj = f_obj.name2tab[tab_name]
                lines = tab_obj.print_for_lm_multi_line()
                for l in lines:
                    f.write(l + '\n')


class Pipeline:
    """Pipeline class for automatically manage queue's in & out."""

    def __init__(self, q_obj):
        self.q_obj = q_obj
        self.f_obj_tmp = self.q_obj.popleft()

    def __enter__(self):
        return self.f_obj_tmp

    def __exit__(self, type, value, traceback):
        self.q_obj.append(self.f_obj_tmp)


class Timeout:
    """Timeout class for timing and avoiding long-time string processing."""

    def __init__(self, seconds=1, error_message="Timeout"):
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        raise TimeoutError(self.error_message)

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        signal.alarm(0)


def test_timeout():
    try:
        with Timeout(seconds=3):
            time.sleep(4)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    pass
