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
# [Done] parsing coverage on CREATE TABLE,                    #
#        ALTER TABLE and CREATE INDEX statements: 88.53%      #
# [Done] handle PK                                            #
# [Done] handle FK                                            #
# [Done] handle multi-col keys                                #
# [Done] parse for repo-database level                        #
# [Done] handle clauses without semicolon delimiter           #
# [Done] handle create (unique) index                         #
# TODO: handle queries with JOINs                             #
# TODO: check on alter table                                  #
#       https://www.w3schools.com/sql/sql_check.asp           #
###############################################################


import os
import re
import time
import glob
import signal
import pickle
import traceback
from enum import Enum
from enum import unique
from copy import deepcopy
from collections import deque

import sqlparse

from utils import (
    rm_kw,
    fmt_str,
    clean_stmt,
    Counter,
    RegexDict,
)
from unit_test import (
    test_badcase,
    get_pk_def_case_on_create,
    get_fk_def_case_on_create,
    get_constraint_begin_on_create,
    get_index_def_case_on_create,
    get_add_constaint_unique_on_create,
    get_uniq_case_on_create,
    get_uniq_key_case_on_create,
    get_key_case_on_create,
    get_create_uniq_case_on_create,
    get_add_pk_case_on_alter,
    get_add_fk_case_on_alter,
    get_add_constraint_pk_case_on_alter,
    get_add_constraint_fk_case_on_alter,
    get_add_uniq_key_case_on_alter,
    get_add_uniq_idx_case_on_alter,
    get_add_key_case_on_alter,
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
COL_DATA_TYPES = ["varchar", "serial", "long", "uuid", "bytea", "json", "string", "char", "binary", "blob", "clob", "text", "enum", "set", "number", "numeric", "bit", "int", "bool", "float", "double", "decimal", "date", "time", "year", "image", "real", "identifier", "raw", "graphic", "money", "geography", "cursor", "rowversion", "hierarchyid", "uniqueidentifier", "sql_variant", "xml", "inet", "cidr", "macaddr", "point", "line", "lseg", "box", "path", "polygon", "circle", "regproc", "tsvector", "sysname"]

REGEX_DICT = RegexDict()

COUNTER, COUNTER_EXCEPT = Counter(), Counter()


@unique
class ParseStage(Enum):
    """Enum class for definitions on parsing stages.
    - create: handle CREATE TABLE statements.
    - alter: handle ALTER TABLE statements.
    - fk: handle FKs on referred missing cases in create and alter.
    - query: handle queries with JOINs statements.
    """
    create = 0
    alter = 1
    fk = 2
    query = 3


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
    - key_col_list: list[str]

    Attribs
    -------
    - key_type: str
    - key_col_list: list[str]

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
    - fk_col_list: list[str]
    - ref_tab_obj: Table
    - ref_col_list: list[str]

    Attribs
    -------
    - fk_cols: list[str]
    - ref_tab: Table
    - ref_cols: list[str]

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
            raise Exception(f"col already exists in table: {col.col_name}")
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

    def __init__(self, hashid, repo_name2tab):
        self.hashid = hashid
        self.repo_name2tab = repo_name2tab
        # self.name2tab = dict()
        self.memo = set()  # set(tuple[str])

    @staticmethod
    def construct_key_obj(key_type, key_cols_str):
        """Construct a key object.

        Params
        ------
        - key_type: str
        - key_cols_str: str

        Returns
        -------
        - a Key object
        """
        key_col_list = fmt_str(key_cols_str).split(',')
        key_col_list = [c.strip() for c in key_col_list]
        return Key(key_type, key_col_list)

    @staticmethod
    def construct_fk_obj(fk_cols_str, ref_tab_obj, ref_cols_str):
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
        fk_col_list = fmt_str(fk_cols_str).split(',')
        fk_col_list = [c.strip() for c in fk_col_list]
        ref_col_list = fmt_str(ref_cols_str).split(',')
        ref_col_list = [c.strip() for c in ref_col_list]
        return ForeignKey(fk_col_list, ref_tab_obj, ref_col_list)

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
        # check ref tab is valid or not
        if isinstance(tab, Table):
            tab_obj = tab
        elif isinstance(tab, str):
            tab = fmt_str(tab)
            if tab in self.repo_name2tab:
                tab_obj = self.repo_name2tab[tab]
            else:
                print(f"Unknown ref table `{tab}`")
                return False
        else:
            raise TypeError("References check error! Param `tab`'s type must be either Table or str")
        # check ref col is valid or not
        if cols is not None:
            cols = fmt_str(cols).split(',')
            cols = [rm_kw(c) for c in cols]
            for col in cols:
                if col not in tab_obj.name2col:
                    print(f"Unknown ref col `{col}` in ref table `{tab_obj.tab_name}`")
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
            # tab_name = fmt_str(stmt.split("create table")[1].split('(')[0]).replace("if not exists", "").strip()  # deprecated
            tab_name = fmt_str(re.match(REGEX_DICT("get_create_table_name"), stmt, re.IGNORECASE).group(1))
            tab_obj = Table(tab_name, self.hashid)
            # get all clauses on create table
            clauses = stmt.split("create table")[1].split('(', 1)[1].strip()
            # remove the last found index of )
            clauses = "".join(clauses[i] for i in range(len(clauses)) if i != clauses.rfind(')'))
            # remove type size with parentheses
            clauses = re.sub("\(\d+\)", "", clauses)
            # split by comma, use regex to ignore commas in matching parentheses
            # this regex pattern could ensure multi columns kept.
            clauses = [c.strip() for c in re.split(REGEX_DICT("split_clause_by_comma"), clauses) if not c.isspace()]
            for clause in clauses:
                # skip the clause which starts with comment
                if clause.startswith("comment"):
                    continue
                # handle clause starts with constraint
                elif clause.startswith("constraint"):
                    # handle: CONSTRAINT [constraint_name] PRIMARY KEY ([pk_cols])
                    if "primary key" in clause:
                        pattern = REGEX_DICT("constraint_pk_create_table")
                        result = re.findall(pattern, clause, re.IGNORECASE)
                        if len(result) > 0:
                            pk_cols = rm_kw(result[0])
                        else:
                            raise Exception("CONSTRAINT PRIMARY KEY def error: match number must be 1!")
                        if self.is_pk_ref_valid(tab_obj, pk_cols):
                            # print(f"| <primary_key_cols:\"{fmt_str(pk_cols)}\"> |")
                            pk_obj = File.construct_key_obj("PrimaryKey", pk_cols)
                            tab_obj.key_list.append(pk_obj)
                        else:
                            raise Exception("CONSTRAINT PRIMARY KEY def error: references on create table not found!")
                    # handle: CONSTRAINT [constraint_name]
                    #         FOREIGN KEY ([fk_cols]) REFERENCES [ref_table] ([ref_cols])
                    elif "foreign key" in clause:
                        pattern = REGEX_DICT("constraint_fk_create_table")
                        result = re.findall(pattern, clause, re.IGNORECASE)[0]
                        if len(result) == 3:
                            fk_cols = fmt_str(result[0])
                            fk_ref_tab = fmt_str(result[1])
                            fk_ref_cols = fmt_str(result[2])
                        else:
                            raise Exception("CONSTRAINT FOREIGN KEY def error: match number must be 3!")
                        if self.is_fk_ref_valid(tab_obj, fk_cols):
                            fk_ref_tab = fmt_str(fk_ref_tab)
                            if fk_ref_tab == tab_name and self.is_fk_ref_valid(tab_obj, fk_ref_cols):
                                # print(f"| <foreign_key_cols:\"{fmt_str(fk_cols)}\"> | <fk_ref_tab:\"{fmt_str(fk_ref_tab)}\"> | <fk_ref_cols:\"{fmt_str(fk_ref_cols)}\"> |")
                                fk_obj = File.construct_fk_obj(fk_cols, tab_obj, fk_ref_cols)
                                tab_obj.fk_list.append(fk_obj)
                            elif fk_ref_tab != tab_name and self.is_fk_ref_valid(fk_ref_tab, fk_ref_cols):
                                # print(f"| <foreign_key_cols:\"{fmt_str(fk_cols)}\"> | <fk_ref_tab:\"{fmt_str(fk_ref_tab)}\"> | <fk_ref_cols:\"{fmt_str(fk_ref_cols)}\"> |")
                                ref_tab_obj = self.repo_name2tab[fk_ref_tab]
                                fk_obj = File.construct_fk_obj(fk_cols, ref_tab_obj, fk_ref_cols)
                                tab_obj.fk_list.append(fk_obj)
                            else:
                                self.memo.add((tab_name, fk_cols, fk_ref_tab, fk_ref_cols))
                                COUNTER_EXCEPT()
                                # raise Exception("CONSTRAINT FOREIGN KEY def error: references on create table not found!")
                                print("CONSTRAINT FOREIGN KEY def error: references on create table not found!")
                        else:
                            raise Exception("CONSTRAINT FOREIGN KEY def error: references on create table not found!")
                    # handle: CONSTRAINT [constraint_name] UNIQUE ([uniq_cols])
                    # n.b. UNQIUE and UNQIEU KEY are equivalent
                    elif "unique" in clause:
                        pattern = REGEX_DICT("constraint_unique_create_table")
                        result = re.findall(pattern, clause, re.IGNORECASE)
                        if len(result) == 1:
                            uk_cols = rm_kw(result[0])
                        else:
                            raise Exception("CONSTRAINT UNIQUE def error: match number must be 1!")
                        if self.is_uk_ref_valid(tab_obj, uk_cols):
                            # print(f"| <constriant_unique_cols:\"{fmt_str(ui_cols)}\"> |")
                            uk_obj = File.construct_key_obj("UniqueKey", uk_cols)
                            tab_obj.key_list.append(uk_obj)
                        else:
                            raise Exception("CONSTRAINT UNIQUE def error: references on create table not found!")
                    elif "check" in clause:
                        # print("TODO: handle constraint CHECK on create table")
                        pass
                    else:
                        raise Exception("CONSTRAINT handle error: unknown constraint type!")
                # handle primary key
                elif "primary key" in clause:
                    # n.b. It seems that no references-case on the statement starts with "primary key".
                    #      The statement starts with "primary key" means only using pre-defined cols to define pk,
                    #      the statement doesn't start with "primary key" means both create a new col and define a pk.
                    if clause.startswith("primary key"):
                        pk_cols = fmt_str(clause.split("key")[1].split('(')[1].split(')')[0]).split(',')
                        pk_cols = [c.strip() for c in pk_cols]
                        # print(f"| <primary_key_cols:\"{fmt_str(','.join(pk_cols))}\"> |")
                        pk_obj = File.construct_key_obj("PrimaryKey", pk_cols)
                        tab_obj.key_list.append(pk_obj)
                    else:
                        pk_col_defs = clause.split("primary key")[0].split()
                        pk_col = fmt_str(pk_col_defs[0].strip())
                        # pk_col_type = fmt_str(pk_col_defs[1].strip())  # unused for now

                        # handle the omission clause for both indicate pk and fk,
                        # e.g. [key_col] INTEGER PRIMARY KEY REFERENCES [ref_tab] ([ref_cols])
                        if " references " in clause:
                            fk_ref_def = clause.split("references")[1].strip().split(')', maxsplit=1)[0].split('(')
                            fk_ref_tab = fmt_str(fk_ref_def[0].split("on")[0]) if len(fk_ref_def) == 1 else fmt_str(fk_ref_def[0])
                            fk_ref_cols = fmt_str(fk_ref_def[1]) if len(fk_ref_def) == 2 else None
                            # print("FOREIGN KEY def error: references on create table not found!")
                            self.memo.add((tab_name, pk_col, fk_ref_tab, fk_ref_cols))
                            COUNTER_EXCEPT()

                        col_obj = Column(pk_col)
                        tab_obj.insert_col(col_obj)
                        tab_obj.col_name_seq.append(pk_col)

                        if self.is_pk_ref_valid(tab_obj, pk_col):
                            pk_obj = File.construct_key_obj("PrimaryKey", pk_col)
                            tab_obj.key_list.append(pk_obj)
                        else:
                            raise Exception("PRIMARY KEY def error: references on create table not found!")
                # handle foreign key
                elif clause.startswith("foreign key"):
                    # n.b. Slightly Similar to primary key, foreign key
                    #      has two different semantics according its keyword position.
                    #      However, one of the variant CONSTRAINT ... has been handled in front.
                    pattern = REGEX_DICT("startwith_fk_create_table")
                    result = re.findall(pattern, clause, re.IGNORECASE)[0]
                    # fk must have references, so its matching length is 3.
                    # FOREIGN KEY([fk_name]) REFERENCES [ref_tab_name]([ref_col_name])
                    if len(result) == 3:
                        fk_cols = fmt_str(result[0])
                        fk_ref_tab = fmt_str(result[1])
                        fk_ref_cols = fmt_str(result[2])
                    else:
                        raise Exception("FOREIGN KEY def error: match number must be 3!")
                    if self.is_fk_ref_valid(tab_obj, fk_cols) and \
                       self.is_fk_ref_valid(fk_ref_tab, fk_ref_cols):
                        # print(f"| <foreign_key_cols:\"{fmt_str(fk_cols)}\"> | <ref_tab_name:\"{fmt_str(fk_ref_tab)}\"> | <ref_col_name:\"{fmt_str(fk_ref_cols)}\"> |")
                        ref_tab_obj = self.repo_name2tab[fk_ref_tab]
                        fk_obj = File.construct_fk_obj(fk_cols, ref_tab_obj, fk_ref_cols)
                        tab_obj.fk_list.append(fk_obj)
                    else:
                        self.memo.add((tab_name, fk_cols, fk_ref_tab, fk_ref_cols))
                        COUNTER_EXCEPT()
                        # print("FOREIGN KEY def error: references on create table not found!")
                        # raise Exception("FOREIGN KEY def error: references on create table not found!")
                # handle unique key
                elif clause.startswith("unique key"):
                    pattern = REGEX_DICT("startwith_uk_create_table")
                    result = re.findall(pattern, clause, re.IGNORECASE)
                    if len(result) == 1:
                        uk_cols = result[0]
                    else:
                        raise Exception("UNIQUE KEY defined error: match number must be 1!")
                    if self.is_uk_ref_valid(tab_obj, uk_cols):
                        # print(f"| <unique_key_cols:\"{fmt_str(uk_cols)}\"> |")
                        uk_obj = File.construct_key_obj("UniqueKey", uk_cols)
                        tab_obj.key_list.append(uk_obj)
                    else:
                        raise Exception("UNIQUE KEY ref error: references on create table not found!")
                # handle candidate key
                elif len(re.findall("^key\s", clause)) == 1:
                    # KEY [key_name] ([key_col_0], ...)  # key_name is unused for now.
                    pattern = REGEX_DICT("candidate_key_create_table")
                    result = re.findall(pattern, clause, re.IGNORECASE)
                    if len(result) == 1:
                        key_cols = re.sub(pattern, "", result[0])  # rm internal parenthesis
                    else:
                        raise Exception("KEY defined error: match number must be 1!")
                    # TODO: check whether have references
                    if " references " in clause:
                        pass
                    if self.is_key_ref_valid(tab_obj, key_cols):
                        # print(f"| <key_cols:\"{fmt_str(key_cols)}\"> |")
                        key_obj = File.construct_key_obj("CandidateKey", key_cols)
                        tab_obj.key_list.append(key_obj)
                    else:
                        raise Exception("KEY ref error: references on create table not found!")
                # handle unique index
                elif clause.startswith("unique index"):
                    pattern = REGEX_DICT("startwith_ui_create_table")
                    result = re.findall(pattern, clause, re.IGNORECASE)[0]
                    if len(result) == 2:
                        # uniq_idx_name = result[0]
                        ui_cols = result[1]
                    else:
                        raise Exception("UNIQUE INDEX defined error: match number must be 2!")
                    if self.is_ui_ref_valid(tab_obj, ui_cols):
                        # print(f"| <unique_index_col:\"{fmt_str(ui_cols)}\"> |")
                        ui_obj = File.construct_key_obj("UniqueIndex", ui_cols)
                        tab_obj.key_list.append(ui_obj)
                    else:
                        raise Exception("UNIQUE INDEX ref error: references on create table not found!")
                # handle: UNIQUE ([uni_cols])
                elif clause.startswith("unique"):
                    pattern = REGEX_DICT("startwith_unique_create_table")
                    result = re.findall(pattern, clause, re.IGNORECASE)
                    if len(result) == 1:
                        uk_cols = re.sub("(\(.*\))", "", result[0])
                    else:
                        raise Exception("UNIQUE def error: match number must be 1!")
                    if self.is_ui_ref_valid(tab_obj, uk_cols):
                        # print(f"| <unique_cols:\"{fmt_str(uk_cols)}\"> |")
                        uk_obj = File.construct_key_obj("UniqueKey", uk_cols)
                        tab_obj.key_list.append(uk_obj)
                    else:
                        raise Exception("UNIQUE def error: references on create table not found!")
                # handle index
                elif clause.startswith("index"):
                    pattern = REGEX_DICT("startwith_index_create_table")
                    result = re.findall(pattern, clause, re.IGNORECASE)
                    if len(result) == 1:
                        index_cols = rm_kw(result[0])
                    else:
                        raise Exception("INDEX defined error: match number must be 1!")
                    if self.is_ui_ref_valid(tab_obj, index_cols):
                        # print(f"| <index_cols:\"{fmt_str(index_cols)}\"> |")
                        index_obj = File.construct_key_obj("Index", index_cols)
                        tab_obj.key_list.append(index_obj)
                    else:
                        raise Exception("INDEX ref error: references on create table not found!")
                # TODO: handle constraint CHECK
                # handle CONSTRAINT [constraint_name] CHECK ([check_conditions])
                elif "check" in clause:
                    # print("TODO: handle constraint CHECK on create table")
                    pass
                # handle data_compression
                elif clause.startswith("data_compression"):
                    pass
                # handle ordinary col definition
                # TODO: handle the col with references
                #       (e.g. UserID integer REFERENCES users (UserID) ON DELETE CASCADE,)
                else:
                    if len(clause.split()) == 1:
                        print(f"too few cols in clause to parse! | {clause}")
                        continue
                    # n.b.
                    # here are two branches to extract `c_name` and `c_type`:
                    # 1) as to the situation can only appears with punc wrapped which allows space in col name,
                    #    detect if clause includes punc like `, ', ", if True, extract by regex.
                    # 2) if False, extract by simple split on space,
                    #    it could asure that col name which not includes space would not be split unexpectedly.
                    # TODO: check when single line alter operation inputs
                    col_defs = clause.split(" default ")[0].split(" comment ")[0].strip()
                    if '`' in col_defs or '\'' in col_defs or '"' in col_defs:
                        try:
                            result = re.findall("([`|'|\"].*?[`|'|\"])", col_defs, re.IGNORECASE)[0]
                        except:
                            raise Exception(f"Regex match failed!" + traceback.format_exc())
                        else:
                            c_name = fmt_str(result)
                            c_type = clause.split(result)[1].split()[0]
                    else:
                        c_name = fmt_str(col_defs.split()[0])
                        c_type = fmt_str(col_defs.split()[1])
                    if not any(known_type in c_type for known_type in COL_DATA_TYPES):
                        print('unrecognized type: ' + c_type)
                        continue

                    # handle UNIQUE constraint in ordinary column
                    uniq_col_obj = File.construct_key_obj("UniqueColumn", c_name)
                    tab_obj.key_list.append(uniq_col_obj)

                    col_obj = Column(c_name)
                    if "not null" in clause:
                        col_obj.is_notnull = True
                    # add col_obj into table_obj
                    tab_obj.insert_col(col_obj)
                    tab_obj.col_name_seq.append(c_name)
            return tab_obj
        except Exception as e:
            print(e)
            COUNTER_EXCEPT()
            return None

    def parse_one_statement_alter_table(self, stmt):
        """Parse a SQL statement on alter table.

        Params
        ------
        - stmt: str

        Returns
        -------
        - None
        """
        # parse table name
        try:
            # "alter\stable\s(.*?)\s"
            # tab_name = fmt_str(stmt.split('alter table')[1].replace(" only ", ' ').split()[0])
            tab_name = fmt_str(re.match(REGEX_DICT("get_alter_table_name"), stmt, re.IGNORECASE).group(1))
            if tab_name not in self.repo_name2tab:
                print(f"Did not find this table on alter table: {tab_name}")
                return None
            tab_obj = self.repo_name2tab[tab_name]

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
            multicol_list = re.findall("\(.*?\)", stmt, re.IGNORECASE)
            stmt = re.sub("\(.*?\)", "[MULTI-COL]", stmt)
            clauses = stmt.split("alter table")[1].strip().split(',')
            with Timeout(seconds=1):
                # potential memory leak here, could be handled better.
                if len(multicol_list) != 0:
                    temp_list = list()
                    i = 0
                    while i in range(len(multicol_list)):
                        for c in clauses:
                            while "[MULTI-COL]" in c:
                                multicol = multicol_list[i]
                                c = c.replace("[MULTI-COL]", multicol, 1)
                                i += 1
                            temp_list.append(c)
                    clauses = temp_list

            # Parse each sub clause according its constraint type
            for clause in [c.strip() for c in clauses]:
                # handle pk on alter table for two variants.
                if "primary key" in clause:
                    if "add constraint" in clause:
                        pattern = REGEX_DICT("add_constraint_pk_alter_table")
                        clause = clause.split("add constraint")[1].strip()
                        result = re.findall(pattern, clause, re.IGNORECASE)[0]
                        if isinstance(result, str):
                            pk_cols = result
                        else:
                            raise Exception("ADD CONSTRAINT PRIMARY KEY error: match number must be 1!")
                    elif "add primary key" in clause:
                        pattern = REGEX_DICT("add_pk_alter_table")
                        clause = clause.split("add primary key")[1].strip()
                        result = re.findall(pattern, clause, re.IGNORECASE)[0]
                        if isinstance(result, str):
                            pk_cols = result
                        else:
                            raise Exception("ADD PRIMARY KEY error: match number not equal to 1!")
                    else:
                        raise Exception(f"Unknown pk variant: {clause}")
                    if self.is_pk_ref_valid(tab_name, pk_cols):
                        # print(f"| <primary_key_cols:\"{fmt_str(pk_cols)}\"> |")
                        pk_obj = File.construct_key_obj("PrimaryKey", pk_cols)
                        tab_obj.key_list.append(pk_obj)
                    else:
                        raise Exception("ADD PRIMARY KEY error: column(s) on alter table not found!")
                elif "foreign key" in clause:
                    # handle fk on alter table for two variants.
                    # 1) ADD CONSTRAINT [fk_alias] FOREIGN KEY([fk_col(s)]) REFERENCES [ref_table_name] ([ref_col_name])
                    # 2) ADD FOREIGN KEY ([fk_col(s)]) REFERENCES [ref_table_name] ([ref_col_name])
                    if "add constraint" in clause:
                        pattern = REGEX_DICT("add_constraint_fk_alter_table")
                        # multi alter statement for add constraint fk
                        clause = clause.split("add constraint")[1].strip()
                        result = re.findall(pattern, clause, re.IGNORECASE)[0]
                        # fk must have reference, so its len is 3 at least.
                        # 1. ADD CONSTRAINT [alias] FOREIGN KEY([fk_name]) REFERENCES [ref_table_name]([ref_col_name])
                        if len(result) == 3:
                            fk_cols, fk_ref_tab, fk_ref_cols = result
                        else:
                            raise Exception("ADD CONSTRAINT FOREIGN KEY error: match number not equal to 3!")
                    elif "add foreign key" in clause:
                        pattern = REGEX_DICT("add_fk_alter_table")
                        clause = clause.split("add foreign key")[1].strip()
                        result = re.findall(pattern, clause, re.IGNORECASE)[0]
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
                    if self.is_fk_ref_valid(tab_name, fk_cols) and \
                       self.is_fk_ref_valid(fk_ref_tab, fk_ref_cols):
                        # print(f"| <foreign_key_cols:\"{fmt_str(fk_cols)}\"> | <ref_table_name:\"{fmt_str(fk_ref_tab)}\"> | <ref_cols:\"{fmt_str(fk_ref_cols)}\"> |")
                        ref_tab_obj = self.repo_name2tab[fk_ref_tab]
                        fk_obj = File.construct_fk_obj(fk_cols, ref_tab_obj, fk_ref_cols)
                        tab_obj.fk_list.append(fk_obj)
                    else:
                        self.memo.add((tab_name, fk_cols, fk_ref_tab, fk_ref_cols))
                        COUNTER_EXCEPT()
                        # print("ADD FOREIGN KEY error: references on alter table not found!")
                        # raise Exception("ADD FOREIGN KEY error: references on alter table not found!")
                elif "unique" in clause:
                    # 1) handle ADD UNIQUE KEY
                    if "add unique key" in clause:
                        clause = clause.split("add unique key")[1].strip()
                        pattern = REGEX_DICT("add_unique_key_alter_table")
                        result = re.findall(pattern, clause, re.IGNORECASE)[0]
                        if len(result) == 2:
                            # uniq_key_name = result[0]  # unused for now
                            uniq_key_cols = result[1]
                        else:
                            raise Exception("ADD UNIQUE KEY error: match number not equal to 2!")
                        if self.is_uk_ref_valid(tab_name, uniq_key_cols):
                            # print(f"| <unique_key_cols:\"{fmt_str(uniq_key_cols)}\"> |")
                            uk_obj = File.construct_key_obj("UniqueKey", uniq_key_cols)
                            tab_obj.key_list.append(uk_obj)
                        else:
                            raise Exception("ADD UNIQUE KEY error: references on alter table not found!")
                    # 2) handle ADD UNIQUE INDEX
                    elif "add unique index" in clause:
                        pattern = REGEX_DICT("add_unique_index_alter_table")
                        clause = clause.split("add unique index")[1].strip()
                        result = re.findall(pattern, clause, re.IGNORECASE)
                        if len(result) == 1:
                            ui_cols = result[0]
                        else:
                            raise Exception("ADD UNIQUE INDEX error: match number not equal to 1!")
                        if self.is_ui_ref_valid(tab_name, ui_cols):
                            # print(f"| <unique_index_cols:\"{fmt_str(ui_cols)}\"> |")
                            ui_obj = File.construct_key_obj("UniqueIndex", ui_cols)
                            tab_obj.key_list.append(ui_obj)
                        else:
                            raise Exception("ADD UNIQUE INDEX error: references on alter table not found!")
                    # 3) handle ADD CONSTRAINT UNIQUE KEY
                    elif "add constraint" in clause:
                        pattern = REGEX_DICT("add_constraint_unique_alter_table")
                        result = re.findall(pattern, clause, re.IGNORECASE)
                        if len(result) == 1:
                            uk_cols = result[0]
                        else:
                            raise Exception("ADD CONSTRAINT UNIQUE error: match number not equal to 1!")
                        if self.is_uk_ref_valid(tab_name, uk_cols):
                            # print(f"| <constriant_unique_cols:\"{fmt_str(uk_cols)}\"> |")
                            uk_obj = File.construct_key_obj("UniqueKey", uk_cols)
                            tab_obj.key_list.append(uk_obj)
                        else:
                            raise Exception("ADD CONSTRIANT UNIQUE error: references on alter table not found!")
                    # 4) handle CREATE UNIQUE [constraint_name] INDEX
                    elif len(re.findall("create\s+unique\s*(clustered|nonclustered)?\s+index", clause, re.IGNORECASE)) == 1:
                        pattern = REGEX_DICT("create_unique_index_alter_table")
                        result = re.findall(pattern, clause, re.IGNORECASE)[0]
                        if len(result) == 2 or len(result) == 3:
                            ref_tab = result[0]
                            ref_cols = result[1]
                            # asc_or_desc = result[2]  # unused for now
                        else:
                            raise Exception("CREATE UNIQUE INDEX error: match number not equal to 2 or 3!")
                        if self.is_ui_ref_valid(ref_tab, ref_cols):
                            # print(f"| <unique_index_table:\"{fmt_str(ref_tab)}\"> | <unique_index_cols:\"{fmt_str(ref_cols)}\"> |")
                            ui_obj = File.construct_key_obj("UniqueIndex", ref_cols)
                            tab_obj.key_list.append(ui_obj)
                        else:
                            raise Exception("CREATE UNIQUE INDEX error: references on alter table not found!")
                    else:
                        raise Exception(f"UNIQUE error: unknown add unique variant! => {clause}")
                # handle add candidate key on alter table
                elif "add key" in clause:
                    pattern = REGEX_DICT("add_key_alter_table")
                    result = re.findall(pattern, clause, re.IGNORECASE)
                    if len(result) == 1:
                        key_cols = result[0]
                    else:
                        raise Exception("ADD KEY error: match number not equal to 1!")
                    if self.is_key_ref_valid(tab_name, key_cols):
                        # print(f"| <key_cols:\"{fmt_str(key_cols)}\"> |")
                        key_obj = File.construct_key_obj("CandidateKey", key_cols)
                        tab_obj.key_list.append(key_obj)
                    else:
                        raise Exception("ADD KEY error: references on alter table not found!")
                else:
                    print(f"Unhandled operation on alter table: {clause}")
        except Exception as e:
            print(e)
            COUNTER_EXCEPT()
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
        try:
            pattern = REGEX_DICT("create_index_or_unique_index")
            result = re.findall(pattern, stmt, re.IGNORECASE)[0]
            if len(result) == 3:
                idx_tab_name = result[0]
                # idx_type = result[1]  # unused for now
                idx_cols = result[2]
            else:
                raise Exception("CREATE INDEX def error: match number must be 3!")
            if self.is_ui_ref_valid(idx_tab_name, idx_cols):
                # print(f"| <index_table:\"{fmt_str(idx_tab_name)}\"> | <index_cols:\"{fmt_str(idx_cols)}\"> |")
                tab_obj = self.repo_name2tab[fmt_str(idx_tab_name)]
                idx_obj = File.construct_key_obj("UniqueIndex", idx_cols)\
                    if "create unique index" in stmt else File.construct_key_obj("Index", idx_cols)
                tab_obj.key_list.append(idx_obj)
            else:
                raise Exception("CREATE INDEX def error: references on CREATE INDEX not found!")
        except Exception as e:
            print(e)
            COUNTER_EXCEPT()
            return None

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
            with Timeout(seconds=3):
                stmt = sqlparse.format(stmt, strip_comments=True)
        except Exception as e:
            print(e)
            return
        else:
            stmt = ' '.join(stmt.split())

        # preprocess statement
        stmt = clean_stmt(stmt)
        # skip empty string
        if stmt == "" or stmt.startswith("insert into"):
            return
        elif "create table" in stmt:
            COUNTER()
            tab_obj = self.parse_one_statement_create_table(stmt)
            if tab_obj is not None:
                # self.name2tab[tab_obj.tab_name] = tab_obj
                self.repo_name2tab[tab_obj.tab_name] = tab_obj
        elif "alter table" in stmt:
            COUNTER()
            self.parse_one_statement_alter_table(stmt)
        elif "create index" in stmt or "create unique index" in stmt:
            COUNTER()
            self.parse_one_statement_create_index(stmt)
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
        if ';' in stmts:
            stmts = stmts.lower().split(';')
        else:
            stmts = stmts.lower()\
                .replace("create table", ";\ncreate table")\
                .replace(";\ncreate table", "create table", 1)\
                .replace("alter table", ";\nalter table")\
                .replace(";\nalter table", "alter table", 1)\
                .replace("create index", ";\ncreate index")\
                .replace(";\ncreate index", "create index", 1)\
                .replace("create unique index", ";\ncreate unique index")\
                .replace(";\ncreate unique index", "create unique index", 1)\
                .split(';')
        if stage == ParseStage.create:
            stmts = (s for s in stmts if "create table" in s)
        elif stage == ParseStage.alter:
            stmts = (s for s in stmts if "alter table" in s or "create index" in s or "create unique index" in s)
        elif stage == ParseStage.query:
            # TODO: filter statements which are SELECT ... JOIN ...
            return
        for s in stmts:
            if len(s) < STATEMENT_SIZE_LIMIT:
                self.parse_one_statement(s)
            else:
                print("skipping a long statement")


def parse_all_files(files, test_stmt=None):
    """Parse all SQL files according to the input file path list.

    Params
    ------
    - files: str
    - test_stmt: str, default=None

    Returns
    -------
    - parsed_file_list: list
    """
    parsed_file_list = list()
    for fp in files:
        print('-' * 84)
        with open(fp, encoding="utf-8") as f:
            try:
                if test_stmt is not None:
                    print("UNIT TEST")
                    print('-' * 77)
                    sql_file = File(test_stmt)
                    sql_file.parse(test_stmt)
                    exit()
                print(fp)
                hashid = fp.split('/')[-1]
                lines = f.readlines()
                stmts = ' '.join(lines)
                sql_file = File(hashid)
                sql_file.parse(stmts)
                parsed_file_list.append(sql_file)
            except Exception as e:
                print(e)
                continue
        print("succ: {}, except: {}".format(COUNTER.num - COUNTER_EXCEPT.num, COUNTER_EXCEPT.num))

    print("Totally succ: {}, except: {}".format(COUNTER.num - COUNTER_EXCEPT.num, COUNTER_EXCEPT.num))
    print(len(parsed_file_list))
    return parsed_file_list


def parse_repo_files(repo_obj):
    """Parse all SQL files in the same repository.
    - first stage: parse all CREATE TABLE statements in files,
                     and record all unresolved referred tuple in repo's memo.
    - second stage: parse repo again to handle ALTER TABLE statements
                      and all FKs for references.
    - third stage: parse repo to handle queries with JOINs statements.

    Benefit
    -------
    => Solve the reversed-orders cases(alter first and create later)
    => As much as possible solve the FKs' references missing cases.

    Params
    ------
    repo_obj: Repository

    Returns
    -------
    - parsed_repo: Repository
    """
    fpath_list = repo_obj.repo_fpath_list
    file_obj_queue = deque()
    repo_memo = dict()
    for stage in ParseStage:
        print('=' * 30, stage, '=' * 30)
        for fp in fpath_list:
            print('-' * 90)
            print(f"{stage}:\t{fp}")
            if stage == ParseStage.create:
                # handle CREATE TABLE clauses
                with open(fp, encoding="utf-8", errors="ignore") as f:
                    hashid = fp.split('/')[-1]
                    lines = f.readlines()
                    stmts = ' '.join(lines)
                    file_obj = File(hashid, repo_obj.name2tab)
                    try:
                        file_obj.parse(stmts, stage)
                    except Exception as e:
                        print("first stage failed | ", e)
                    finally:
                        # whatever parse success or failed, append file_obj to queue
                        file_obj_queue.append(file_obj)
                        # merge file_obj's name2tab dict to repo_obj's name2tab dict
                        # repo_obj.name2tab = repo_obj.name2tab | file_obj.name2tab
            elif stage == ParseStage.alter:
                # handle ALTER TABLE clauses
                with open(fp, encoding="utf-8", errors="ignore") as f:
                    lines = f.readlines()
                    stmts = ' '.join(lines)
                    with Pipeline(file_obj_queue) as file_obj:
                        try:
                            file_obj.parse(stmts, stage)
                        except Exception as e:
                            print("second stage failed | ", e)
                        finally:
                            # whatever parse success or failed, append file_obj to queue
                            if len(file_obj.memo) == 0:
                                continue
                            repo_memo[file_obj.hashid] = deepcopy(file_obj.memo)
            elif stage == ParseStage.fk:
                # handle FKs in each `file_obj.memo`
                # n.b. according missing referred table name,
                #      search if there is matched table object in repo.name2tab and its cols in tab_obj.name2col.
                with Pipeline(file_obj_queue) as file_obj:
                    if len(file_obj.memo) != 0:
                        for item in file_obj.memo:
                            tab_name, fk_col_name, ref_tab_name, ref_col_name = item
                            if tab_name in repo_obj.name2tab \
                                    and ref_tab_name in repo_obj.name2tab \
                                    and ref_col_name in repo_obj.name2tab[ref_tab_name].name2col:
                                tab_obj = repo_obj.name2tab[tab_name]
                                ref_tab_obj = repo_obj.name2tab[ref_tab_name]
                                fk_obj = File.construct_fk_obj(fk_col_name, ref_tab_obj, ref_col_name)
                                tab_obj.fk_list.append(fk_obj)
                                # remove handled items in repo_memo
                                repo_memo[file_obj.hashid].remove(item)
                                COUNTER_EXCEPT.minus()
                                print(f"Found FK {tab_obj.hashid}:{tab_obj.tab_name} in {ref_tab_obj.hashid}:{ref_tab_obj.tab_name} in memo")
                            else:
                                print(f"Not found FK {tab_name}:{fk_col_name}:{ref_tab_name}:{ref_col_name} in memo")
            elif stage == ParseStage.query:
                # TODO: handle queries with JOINs
                pass
        repo_obj.parsed_file_list = list(file_obj_queue)
        repo_obj.memo = repo_memo
        print(f"succ: {COUNTER.num - COUNTER_EXCEPT.num}, except: {COUNTER_EXCEPT.num}")

    return repo_obj


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

    def __init__(self, seconds=10, error_message="Timeout"):
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
    files = [f for f in glob.glob(os.path.join(INPUT_FOLDER, "*.sql"))]
    print(f"Total SQL files count: {len(files)}.")
    # test_stmt = test_badcase()

    # test_stmt = get_pk_def_case_on_create()
    # test_stmt = get_fk_def_case_on_create()
    # test_stmt = get_constraint_begin_on_create()
    # test_stmt = get_index_def_case_on_create()
    # test_stmt = get_add_constaint_unique_on_create()
    # test_stmt = get_uniq_case_on_create()
    # test_stmt = get_uniq_key_case_on_create()
    # test_stmt = get_key_case_on_create()
    # test_stmt = get_create_uniq_case_on_create()
    # test_stmt = get_add_pk_case_on_alter()
    # test_stmt = get_add_constraint_pk_case_on_alter()
    # test_stmt = get_add_fk_case_on_alter()
    # test_stmt = get_add_constraint_fk_case_on_alter()
    # test_stmt = get_add_uniq_key_case_on_alter()
    # test_stmt = get_add_uniq_idx_case_on_alter()
    # test_stmt = get_add_key_case_on_alter()

    parse_all_files(files)
    # parse_all_files(files, test_stmt)
