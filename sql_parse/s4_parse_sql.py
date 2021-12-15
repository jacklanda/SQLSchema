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
# TODO: handle PK [Done]                                      #
# TODO: handle FK [Done]                                      #
# TODO: handle multi-col keys [Done]                          #
# TODO: handle create unique index                            #
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
# TODO: enrich sql data types: raw, graphic, money, geography, cursor, rowversion, hierarchyid, uniqueidentifier, sql_variant, xml, inet, point, line, lseg, box, path, polygon, circle, regproc, tsvector, sysname
COL_DATA_TYPES = ["varchar", "serial", "long", "uuid", "bytea", "json", "string", "char", "binary", "blob", "clob", "text", "enum", "set", "number", "numeric", "bit", "int", "bool", "float", "double", "decimal", "date", "time", "year", "image", "real", "identifier", "raw", "graphic", "money", "geography", "cursor", "rowversion", "hierarchyid", "uniqueidentifier", "sql_variant", "xml", "inet", "cidr", "macaddr", "point", "line", "lseg", "box", "path", "polygon", "circle", "regproc", "tsvector", "sysname"]

REGEX_DICT = RegexDict()

COUNTER, COUNTER_EXCEPT = Counter(), Counter()


class Key:
    """Key class for Primary Key, Candidate Key, Unique Index and Unique Column object construction.
    A table can have only one primary key, which may consist of single or multiple fields.
    In this class, we use instance variable `is_pk` to represent this object is to save pk or others.

    Params
    ------
    - is_pk: bool
    - key_col_lst: list[str, ...]

    Returns
    -------
    - a Key object
    """

    def __init__(self, __is_pk, __key_col_lst):
        self.__is_pk = __is_pk  # `bool`
        # TODO: check if every col in list is valid.
        # TODO: two branches for `__key_col_lst`, str for single col and list of strs for multicol
        self.__key_col_lst = __key_col_lst  # `list[str, ...]`

    @property
    def is_pk(self):
        return self.__is_pk

    @property
    def key_col_lst(self):
        return self.__key_col_lst


class ForeignKey:
    """ForeignKey class for Foreign Key object construction.
    A table can have several different fks, which may consist of single or multiple fields.
    The several fk objects could be maintain in a list which should be a member of Table class.

    Params
    ------
    - key_col_lst: list[str, ...]
    - ref_tab_obj: Table
    - ref_col_lst: list[str, ...]

    Returns
    -------
    - a ForeignKey object
    """

    def __init__(self, __key_col_lst, __ref_tab_obj, __ref_col_lst):
        # TODO: check if every col in list is valid.
        # TODO: two branches for `__key_col_lst`, str for single col and list of strs for multicol
        self.__key_col_lst = __key_col_lst  # `list[str, ...]`
        if not isinstance(__ref_tab_obj, Table):
            raise ValueError("param __ref_tab_obj must be a Table object!")
        self.__ref_tab_obj = __ref_tab_obj  # `Table`
        # TODO: check if every col in list is valid.
        # TODO: two branches for `__ref_col_lst`, str for single col and list of strs for multicol
        self.__ref_col_lst = __ref_col_lst  # `list[str, ...]`

    @property
    def key_cols(self):
        return self.__key_col_lst

    @property
    def ref_tab(self):
        return self.__ref_tab_obj

    @property
    def ref_cols(self):
        return self.__ref_col_lst


class Column:
    """Construct a column object for a SQL column.

    Params
    ------
    - col_name: str
    - is_unique: bool, default=False
    - is_notnull: bool, default=False
    - is_unique_idx: bool, default=False
    - is_key: bool, default=False
    - is_pk_col: bool, default=False
    - is_fk_col: bool, default=False

    Returns
    -------
    - a Column object
    """

    def __init__(
        self,
        col_name,
        is_unique=False,
        is_notnull=False,
        is_unique_idx=False,
        is_key=False,
        is_pk_col=False,
        is_fk_col=False
    ):
        self.col_name = col_name
        self.is_unique = is_unique
        self.is_notnull = is_notnull
        self.is_key = is_key
        self.is_unique_idx = is_unique_idx
        self.is_pk_col = is_pk_col
        self.is_fk_col = is_fk_col

    def is_col_inferred_unique(self):
        return self.is_key or self.is_unique_idx or self.is_unique

    def is_col_inferred_notnull(self):
        return self.is_key or self.is_notnull

    def print_for_lm_components(self):
        """multi-line, new format, for classification csv
        """
        str_list = [self.cleansed_col_name()]
        if self.is_col_inferred_unique():
            str_list.append(TOKEN_UNIQUE)
        elif self.is_col_inferred_notnull():
            str_list.append(TOKEN_NOTNULL)
        else:
            str_list.append('')

        return str_list

    def cleansed_col_name(self):
        return self.col_name.strip('\'`\`"[]')


class Table:
    """Construct table object for a SQL table.

    Params
    ------
    - tab_name: str
    - hashid: str
    - key_lst: Optional[None, list[Key, ...]], default=None
    - fk_lst: Optional[None, list[ForeignKey, ...]], default=None

    Returns
    -------
    - a Table object
    """

    def __init__(self, tab_name, hashid, key_lst=None, fk_lst=None):
        self.tab_name = tab_name
        self.hashid = hashid
        self.key_lst = key_lst  # Optional[None, list[Key, ...]]
        self.fk_lst = fk_lst  # Optional[None, list[ForeignKey, ...]]
        self.name2col = dict()
        self.col_name_seq = list()  # log the order in which cols are added into the table (leftness etc. matter)

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
        return clean_tab_name.strip('\'`\`"[]')


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
    - stmts: str
    - hashid: str

    Returns
    -------
    - a file object
    """

    def __init__(self, stmts, hashid):
        self.hashid = hashid
        self.name2tab = dict()
        for s in stmts.split(';'):
            if len(s) < STATEMENT_SIZE_LIMIT:
                self.parse_one_statement(s)
            else:
                print('skipping a long statement')

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
        - True / False
        """
        # check ref tab is valid or not
        if isinstance(tab, Table):
            tab_obj = tab
        elif isinstance(tab, str):
            tab = fmt_str(tab)
            if tab in self.name2tab:
                tab_obj = self.name2tab[tab]
            else:
                print(f"Unknown ref table `{tab}`!", end=" | ")
                return False
        else:
            raise TypeError("References check error! Param `tab`'s type must be either Table or str!")
        # check ref col is valid or not
        if cols is not None:
            cols = fmt_str(cols).split(',')
            cols = (c.strip() for c in cols)
            for col in cols:
                if col not in tab_obj.name2col:
                    print(f"Unknown ref col `{col}` in ref table `{tab_obj.tab_name}`!", end=" | ")
                    return False
        return True

    def parse_one_statement_create_table(self, stmt):
        """Parse a SQL statement on create table.
        TODO: unhandled T-SQL for now, ignore for now.
              syntax from https://docs.microsoft.com/en-us/sql/t-sql/statements/create-table-transact-sql?view=sql-server-ver15

        Params
        ------
        - stmt: str

        Returns
        -------
        - tab_obj: Table
        """
        try:
            # parse table name, create obj
            tab_name = fmt_str(stmt.split("create table")[1].split('(')[0]) \
                .replace("if not exists", "").strip()
            tab_obj = Table(tab_name, self.hashid)
            # get all the clauses on create table
            clauses = stmt.split('(', 1)[1].strip()
            # remove the last found index of )
            clauses = "".join(clauses[i] for i in range(len(clauses)) if i != clauses.rfind(')'))
            # split by comma, use regex to ignore commas in matching parentheses
            # this regex pattern could ensure multi columns kept.
            clauses = (c.strip() for c in re.split(r",(?![^\(]*[\)])", clauses) if not c.isspace())
            for clause in clauses:
                # skip the clause which starts with `comment`
                if clause.startswith("comment"):
                    continue
                # handle col start with constraint
                elif clause.startswith("constraint"):
                    # handle: CONSTRAINT [constraint_name] PRIMARY KEY ([pk_cols])
                    if "primary key" in clause:
                        pattern = "\((.*?)\)"
                        result = re.findall(pattern, clause, re.IGNORECASE)
                        if len(result) >= 1:
                            pk_cols = rm_kw(result[0])
                        else:
                            raise Exception("CONSTRAINT PRIMARY KEY def error: match number must be 1!")
                        if self.is_pk_ref_valid(tab_obj, pk_cols):
                            print(f"| <primary_key_cols:\"{fmt_str(pk_cols)}\"> |")
                        else:
                            raise Exception("CONSTRAINT PRIMARY KEY def error: references on create table not found!")
                    # handle: CONSTRAINT [constraint_name]
                    #         FOREIGN KEY ([fk_cols]) REFERENCES [ref_table] ([ref_cols])
                    elif "foreign key" in clause:
                        pattern = "foreign\s*key\s*\(?(.*?)\)?\s*references\s*([`|'|\"]?.*?[`|'|\"]?)\s*\((.*?)\)"
                        result = re.findall(pattern, clause, re.IGNORECASE)[0]
                        if len(result) == 3:
                            fk_cols = result[0]
                            fk_ref_tab = result[1]
                            fk_ref_cols = result[2]
                        else:
                            raise Exception("CONSTRAINT FOREIGN KEY def error: match number must be 3!")
                        if self.is_fk_ref_valid(tab_obj, fk_cols):
                            fk_ref_tab = fmt_str(fk_ref_tab)
                            if fk_ref_tab == tab_name and self.is_fk_ref_valid(tab_obj, fk_ref_cols):
                                print(f"| <foreign_key_cols:\"{fmt_str(fk_cols)}\"> | <fk_ref_tab:\"{fmt_str(fk_ref_tab)}\"> | <fk_ref_cols:\"{fmt_str(fk_ref_cols)}\"> |")
                            elif fk_ref_tab != tab_name and self.is_fk_ref_valid(fk_ref_tab, fk_ref_cols):
                                print(f"| <foreign_key_cols:\"{fmt_str(fk_cols)}\"> | <fk_ref_tab:\"{fmt_str(fk_ref_tab)}\"> | <fk_ref_cols:\"{fmt_str(fk_ref_cols)}\"> |")
                            else:
                                raise Exception("CONSTRAINT FOREIGN KEY def error: references on create table not found!")
                        else:
                            raise Exception("CONSTRAINT FOREIGN KEY def error: references on create table not found!")
                    # handle: CONSTRAINT [constraint_name] UNIQUE ([uni_cols])
                    elif "unique" in clause:
                        pattern = "\((.*?)\)"
                        result = re.findall(pattern, clause, re.IGNORECASE)
                        if len(result) == 1:
                            ui_cols = result[0]
                        else:
                            raise Exception("CONSTRAINT UNIQUE def error: match number must be 1!")
                        if self.is_ui_ref_valid(tab_obj, ui_cols):
                            print(f"| <constriant_unique_cols:\"{fmt_str(ui_cols)}\"> |")
                        else:
                            raise Exception("CONSTRAINT UNIQUE def error: references on create table not found!")
                    # TODO: handle constraint CHECK
                    # handle: CONSTRAINT [constraint_name] CHECK ([check_conditions])
                    elif "check" in clause:
                        print("TODO: handle constraint CHECK on create table")
                    else:
                        raise Exception("CONSTRAINT handling error: unknown constraint type!")
                # handle primary key / unique key
                elif "primary key" in clause:
                    # n.b. It seems that no references-case on the statement starts with "primary key".
                    #      The statement starts with "primary key" means only using pre-defined cols to define pk,
                    #      the statement doesn't start with "primary key" means both create a new col and define a pk.
                    if clause.startswith("primary key"):
                        pk_cols = fmt_str(clause.split("key")[1].split('(')[1].split(')')[0]).split(',')
                        pk_cols = [c.strip() for c in pk_cols]
                        for col in pk_cols:
                            if col in tab_obj.name2col:
                                tab_obj.name2col[col].is_key \
                                    = tab_obj.name2col[col].is_notnull \
                                    = tab_obj.name2col[col].is_unique \
                                    = tab_obj.name2col[col].is_pk_col = True
                        print(f"| <primary_key_cols:\"{fmt_str(','.join(pk_cols))}\"> |")
                    else:
                        pk_col_defs = clause.split("primary key")[0].split()
                        pk_col_name = fmt_str(pk_col_defs[0].strip())
                        # pk_col_type = fmt_str(pk_col_defs[1].strip())  # unused for now
                        if "references" in clause:
                            ref_def = clause.split("references")[1].strip().split(')', maxsplit=1)[0].split('(')
                            ref_tab = fmt_str(ref_def[0].split("on")[0]) if len(ref_def) == 1 else fmt_str(ref_def[0])
                            ref_col = fmt_str(ref_def[1]) if len(ref_def) == 2 else None
                            if self.is_pk_ref_valid(ref_tab, ref_col):
                                print(f"| <primary_key_cols:\"{fmt_str(pk_col_name)}\"> "
                                      f"| <ref_tab_name:\"{fmt_str(ref_tab)}\"> | <ref_col_name:\"{fmt_str(ref_col)}\">")
                            else:
                                raise Exception("PRIMARY KEY def error: references on create table not found!")
                        else:
                            print(f"| <primary_key_cols:\"{fmt_str(pk_col_name)}\"> |")

                        col_obj = Column(pk_col_name)
                        col_obj.is_key \
                            = col_obj.is_unique \
                            = col_obj.is_notnull \
                            = col_obj.is_pk_col = True
                        tab_obj.insert_col(col_obj)
                        tab_obj.col_name_seq.append(pk_col_name)
                # handle foreign key
                elif clause.startswith("foreign key"):
                    # n.b. Slightly Similar to primary key, foreign key
                    #      has two different semantics according its keyword position.
                    #      however, one of the variant CONSTRAINT ... has been handled in front.
                    pattern = "foreign\s*key\s*\(?(.*?)\)?\s*references\s*([`|'|\"]?.*?[`|'|\"]?)\s*\((.*?)\)"
                    result = re.findall(pattern, clause, re.IGNORECASE)[0]
                    # fk must have references, so its matching length is 3.
                    # FOREIGN KEY([fk_name]) REFERENCES [ref_tab_name]([ref_col_name])
                    if len(result) == 3:
                        fk_cols = result[0]
                        ref_tab = result[1]
                        ref_cols = result[2]
                    else:
                        raise Exception("FOREIGN KEY def error: match number must be 3!")
                    if self.is_fk_ref_valid(tab_obj, fk_cols) and \
                       self.is_fk_ref_valid(ref_tab, ref_cols):
                        print(f"| <foreign_key_cols:\"{fmt_str(fk_cols)}\"> | <ref_tab_name:\"{fmt_str(ref_tab)}\"> | <ref_col_name:\"{fmt_str(ref_cols)}\"> |")
                    else:
                        raise Exception("FOREIGN KEY def error: references on create table not found!")
                elif clause.startswith("unique key"):
                    pattern = "unique\s*key\s*.*?\((.*?)\)"
                    result = re.findall(pattern, clause, re.IGNORECASE)
                    if len(result) == 1:
                        uk_cols = result[0]
                    else:
                        raise Exception("UNIQUE KEY defined error: match number must be 1!")
                    if self.is_uk_ref_valid(tab_obj, uk_cols):
                        print(f"| <unique_key_cols:\"{fmt_str(uk_cols)}\"> |")
                    else:
                        raise Exception("UNIQUE KEY ref error: references on create table not found!")
                # handle ordinary key
                elif len(re.findall("^key\s", clause)) == 1:
                    # KEY [key_name] ([key_col_0], ...)  # key_name is unused for now.
                    pattern = "\((.*)\)"
                    result = re.findall(pattern, clause, re.IGNORECASE)
                    if len(result) == 1:
                        key_cols = result[0]
                        key_cols = re.sub("(\(.*?\))", "", key_cols)  # rm internal parenthesis
                    else:
                        raise Exception("KEY defined error: match number must be 1!")
                    # TODO: check whether have references
                    if "references" in clause:
                        pass
                    if self.is_key_ref_valid(tab_obj, key_cols):
                        print(f"| <key_cols:\"{fmt_str(key_cols)}\"> |")
                    else:
                        raise Exception("KEY ref error: references on create table not found!")
                # handle unique index
                elif clause.startswith("unique index"):
                    pattern = "unique\s+index\s+([`|'|\"]?.*?[`|'|\"]?)\s*\((.*?)\)"
                    result = re.findall(pattern, clause, re.IGNORECASE)[0]
                    if len(result) == 2:
                        # uniq_idx_name = result[0]
                        ui_cols = result[1]
                    else:
                        raise Exception("UNIQUE INDEX defined error: match number must be 2!")
                    if self.is_ui_ref_valid(tab_obj, ui_cols):
                        print(f"| <unique_index_col:\"{fmt_str(ui_cols)}\"> |")
                    else:
                        raise Exception("UNIQUE INDEX ref error: references on create table not found!")
                # handle: UNIQUE ([uni_cols])
                elif clause.startswith("unique"):
                    pattern = "\((.*)\)"
                    result = re.findall(pattern, clause, re.IGNORECASE)
                    if len(result) == 1:
                        uniq_cols = re.sub("(\(.*\))", "", result[0])
                    else:
                        raise Exception("UNIQUE def error: match number must be 1!")
                    if self.is_ui_ref_valid(tab_obj, uniq_cols):
                        print(f"| <unique_cols:\"{fmt_str(uniq_cols)}\"> |")
                    else:
                        raise Exception("UNIQUE def error: references on create table not found!")
                # handle ordinary index
                elif clause.startswith("index"):
                    pattern = "index\s+.*\((.*?)\)"
                    result = re.findall(pattern, clause, re.IGNORECASE)
                    if len(result) == 1:
                        index_cols = rm_kw(result[0])
                    else:
                        raise Exception("UNIQUE INDEX defined error: match number must be 2!")
                    if self.is_ui_ref_valid(tab_obj, index_cols):
                        print(f"| <index_cols:\"{fmt_str(index_cols)}\"> |")
                    else:
                        raise Exception("INDEX ref error: references on create table not found!")
                # handle data_compression
                elif clause.startswith("data_compression"):
                    continue
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

                    col_obj = Column(c_name)
                    if "not null" in clause:
                        col_obj.is_notnull = True
                    if "unique" in clause:
                        col_obj.is_unique = True
                    if "primary key" in clause:
                        col_obj.is_key \
                            = col_obj.is_unique \
                            = col_obj.is_notnull \
                            = col_obj.is_pk_col = True
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
            tab_name = fmt_str(stmt.split('alter table')[1].replace(" only ", ' ').split()[0])
            if tab_name not in self.name2tab:
                print(f"Did not find this table: {tab_name}")
                return None
            # tab_obj = self.name2tab[tab_name]

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
            for clause in (c.strip() for c in clauses):
                if "primary key" in clause:
                    # handle pk on alter table for two variants.
                    pattern_0 = "primary\s*key\s*\((.*?)\)"
                    pattern_1 = "\((.*)\)"
                    if "add constraint" in clause:
                        clause = clause.split("add constraint")[1].strip()
                        result = re.findall(pattern_0, clause, re.IGNORECASE)[0]
                        if isinstance(result, str):
                            pk_cols = result
                        else:
                            raise Exception("ADD CONSTRAINT PRIMARY KEY error: match number must be 1!")
                    elif "add primary key" in clause:
                        clause = clause.split("add primary key")[1].strip()
                        result = re.findall(pattern_1, clause, re.IGNORECASE)[0]
                        if isinstance(result, str):
                            pk_cols = result
                        else:
                            raise Exception("ADD PRIMARY KEY error: match number not equal to 1!")
                    else:
                        raise Exception(f"Unknown pk variant: {clause}")
                    # TODO: check ref of pk in table?
                    if self.is_pk_ref_valid(tab_name, pk_cols):
                        print(f"| <primary_key_cols:\"{fmt_str(pk_cols)}\"> |")
                    else:
                        raise Exception("ADD PRIMARY KEY error: column(s) on alter table not found!")
                elif "foreign key" in clause:
                    # handle fk on alter table for two variants.
                    # 1) ADD CONSTRAINT [fk_alias] FOREIGN KEY([fk_col(s)]) REFERENCES [ref_table_name] ([ref_col_name])
                    # 2) ADD FOREIGN KEY ([fk_col(s)]) REFERENCES [ref_table_name] ([ref_col_name])
                    pattern_0 = "foreign\s*key\s*\(?(.*?)\)?\s*references\s*([`|'|\"]?.*?[`|'|\"]?)\s*\((.*?)\)"
                    pattern_1 = "\(([`|'|\"]?.*?[`|'|\"]?)\)\s*references\s*([`|'|\"]?.*?[`|'|\"]?)\s*\((.*?)\)"
                    if "add constraint" in clause:
                        # multi alter statement for add constraint fk
                        clause = clause.split("add constraint")[1].strip()
                        result = re.findall(pattern_0, clause, re.IGNORECASE)[0]
                        # fk must have reference, so its len is 3 at least.
                        # 1. ADD CONSTRAINT [alias] FOREIGN KEY([fk_name]) REFERENCES [ref_table_name]([ref_col_name])
                        if len(result) == 3:
                            fk_cols = result[0]
                            ref_tab = result[1]
                            ref_cols = result[2]
                        else:
                            raise Exception("ADD CONSTRAINT FOREIGN KEY error: match number not equal to 3!")
                    elif "add foreign key" in clause:
                        clause = clause.split("add foreign key")[1].strip()
                        result = re.findall(pattern_1, clause, re.IGNORECASE)[0]
                        if len(result) == 3:
                            fk_cols = result[0]
                            ref_tab = result[1]
                            ref_cols = result[2]
                        else:
                            raise Exception("ADD FOREIGN KEY error: match number not equal to 3!")
                    else:
                        raise Exception(f"Unknown fk variant: {clause}")
                    # check fk cols and its ref are valid or not
                    if self.is_fk_ref_valid(tab_name, fk_cols) and \
                       self.is_fk_ref_valid(ref_tab, ref_cols):
                        print(f"| <foreign_key_cols:\"{fmt_str(fk_cols)}\"> | <ref_table_name:\"{fmt_str(ref_tab)}\"> | <ref_cols:\"{fmt_str(ref_cols)}\"> |")
                    else:
                        raise Exception("ADD FOREIGN KEY error: references on alter table not found!")
                elif "unique" in clause:
                    # 1) handle ADD UNIQUE KEY
                    if "add unique key" in clause:
                        clause = clause.split("add unique key")[1].strip()
                        pattern = "(.*?)\s*\((.*?)\)"
                        result = re.findall(pattern, clause, re.IGNORECASE)[0]
                        if len(result) == 2:
                            # uniq_key_name = result[0]  # unused for now
                            uniq_key_cols = result[1]
                        else:
                            raise Exception("ADD UNIQUE KEY error: match number not equal to 2!")
                        if self.is_uk_ref_valid(tab_name, uniq_key_cols):
                            print(f"| <unique_key_cols:\"{fmt_str(uniq_key_cols)}\"> |")
                        else:
                            raise Exception("ADD UNIQUE KEY error: references on alter table not found!")
                    # 2) handle ADD UNIQUE INDEX
                    elif "add unique index" in clause:
                        # TODO: traverse uniq_idx_cols and assign with attribs they should have.
                        clause = clause.split("add unique index")[1].strip()
                        pattern = "\((.*?)\)"
                        result = re.findall(pattern, clause, re.IGNORECASE)
                        if len(result) == 1:
                            uniq_idx_cols = result[0]
                        else:
                            raise Exception("ADD UNIQUE INDEX error: match number not equal to 1!")
                        if self.is_ui_ref_valid(tab_name, uniq_idx_cols):
                            print(f"| <unique_index_cols:\"{fmt_str(uniq_idx_cols)}\"> |")
                        else:
                            raise Exception("ADD UNIQUE INDEX error: references on alter table not found!")
                    # 3) handle ADD CONSTRAINT UNIQUE KEY
                    elif "add constraint" in clause:
                        pattern = "add\s*constraint\s*.*?\((.*?)\)"
                        result = re.findall(pattern, clause, re.IGNORECASE)
                        if len(result) == 1:
                            constraint_uniq_cols = result[0]
                        else:
                            raise Exception("ADD CONSTRAINT UNIQUE error: match number not equal to 1!")
                        if self.is_ui_ref_valid(tab_name, constraint_uniq_cols):
                            print(f"| <constriant_unique_cols:\"{fmt_str(constraint_uniq_cols)}\"> |")
                        else:
                            raise Exception("ADD CONSTRIANT UNIQUE error: references on alter table not found!")
                    # 4) handle CREATE UNIQUE [constraint_name] INDEX
                    elif len(re.findall("create\s+unique\s*(clustered|nonclustered)?\s+index", clause, re.IGNORECASE)) == 1:
                        pattern = "on\s+(.*?)\s*\((.*?)\s*(ASC)?(DESC)?\)"
                        result = re.findall(pattern, clause, re.IGNORECASE)[0]
                        if len(result) == 2 or len(result) == 3:
                            ref_tab = result[0]
                            ref_cols = result[1]
                            # asc_or_desc = result[2]  # unused for now
                        else:
                            raise Exception("CREATE UNIQUE INDEX error: match number not equal to 2 or 3!")
                        if self.is_ui_ref_valid(ref_tab, ref_cols):
                            print(f"| <unique_index_table:\"{fmt_str(ref_tab)}\"> | <unique_index_cols:\"{fmt_str(ref_cols)}\"> |")
                        else:
                            raise Exception("CREATE UNIQUE INDEX error: references on alter table not found!")
                    else:
                        raise Exception(f"UNIQUE error: unknown add unique variant! => {clause}")
                elif "add key" in clause:
                    pattern = "\((.*?)\(?\d*\)"
                    result = re.findall(pattern, clause, re.IGNORECASE)
                    if len(result) == 1:
                        key_cols = result[0]
                    else:
                        raise Exception("ADD KEY error: match number not equal to 1!")
                    if self.is_key_ref_valid(tab_name, key_cols):
                        print(f"| <key_cols:\"{fmt_str(key_cols)}\"> |")
                    else:
                        raise Exception("ADD KEY error: references on alter table not found!")
                else:
                    print(f"Unhandled operation on alter table: {clause}")
        except Exception as e:
            print(e)
            COUNTER_EXCEPT()
            return None

    def parse_one_statement(self, stmt):
        """Parse single SQL statement split by semicolon `;`

        Params
        ------
        - stmt: str

        Returns
        -------
        - None
        """
        try:
            with Timeout(seconds=5):
                stmt = sqlparse.format(stmt, strip_comments=True)
        except Exception as e:
            print(e)
            return
        else:
            stmt = ' '.join(stmt.split()).lower()

        # preprocess statement
        stmt = clean_stmt(stmt)
        # skip empty string
        if stmt == "":
            return
        elif "create table" in stmt:
            COUNTER()
            tab_obj = self.parse_one_statement_create_table(stmt)
            if tab_obj is not None:
                self.name2tab[tab_obj.tab_name] = tab_obj
        elif "alter table" in stmt:
            COUNTER()
            self.parse_one_statement_alter_table(stmt)
        elif "create unique" in stmt:
            # TODO: impl this method for handle CREATE UNIQUE INDEX
            # self.parse_one_statement_create_unique(stmt)
            pass
        else:
            # check if the input statement is supported.
            # raise Exception(f"Unhandled table operation: {stmt}")
            # print(f"Unhandled table operation: {stmt}")
            pass


def parse_all_files(files, test_stmt=None):
    """Parse all SQL files according to the input file path list.

    Params
    ------
    - files: str
    - test_stmt: str, default=None

    Returns
    -------
    - None
    """
    parsed_file_list = list()
    # cnt_exception = 0
    # cnt_succ = 0
    for fp in files:
        print("-" * 77)
        with open(fp, encoding="utf-8") as f:
            try:
                if test_stmt is not None:
                    print("UNIT TEST")
                    print("-" * 77)
                    File(test_stmt, 0)
                    exit()
                print(fp)
                hashid = fp.split('/')[-1]
                lines = f.readlines()
                stmts = ' '.join(lines)
                # TODO: Optimize the extraction procedure
                parsed_file = File(stmts, hashid)
                parsed_file_list.append(parsed_file)
                # cnt_succ += 1
            except Exception as e:
                print(e)
                # cnt_exception += 1
                # COUNTER_EXCEPT()
                continue
        # print('succ: {}, except: {}'.format(cnt_succ, cnt_exception))
        print('succ: {}, except: {}'.format(COUNTER.num - COUNTER_EXCEPT.num, COUNTER_EXCEPT.num))

    print('Totally succ: {}, except: {}'.format(COUNTER.num - COUNTER_EXCEPT.num, COUNTER_EXCEPT.num))
    print(len(parsed_file_list))

    # dump parsed sql list obj
    pickle_output = os.path.join(OUTPUT_FOLDER, 's4_parsed_sql_file_list.pkl')
    pickle.dump(parsed_file_list, open(pickle_output, "wb"))

    # dump all the parsed sql statements to a csv file
    lm_output = os.path.join(OUTPUT_FOLDER, 's4_parsed_sql_into_lm.csv')
    with open(lm_output, 'w') as f:
        for f_obj in parsed_file_list:
            for tab_name in f_obj.name2tab:
                tab_obj = f_obj.name2tab[tab_name]
                lines = tab_obj.print_for_lm_multi_line()
                for l in lines:
                    f.write(l + '\n')


class Timeout:
    """Timeout class for timing and avoiding long-time string processing."""

    def __init__(self, seconds=10, error_message='Timeout'):
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
