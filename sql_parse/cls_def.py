# -*- coding: utf-8 -*-
# @author: Yang Liu
# @email: v-yangliu4@microsoft.com

from enum import Enum, unique
import utils

TYPE2BASETYPE = {"Numeric": 1}


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


class ColumnType:
    def __init__(self, col_type):
        self.__col_type = col_type
        self.__base_type = self.get_base_type(col_type)

    def get_base_type(self, col_type):
        return 1

    @property
    def col_type(self):
        return self.__col_type

    @property
    def base_type(self):
        return self.__base_type


@unique
class BaseColumnType(Enum):

    def __str__(self):
        return f"{self.name.lower()}"

    # @staticmethod
    # def get_list(this):
        # return {k.lower(): k for k, v in this.__members__.items()}


@unique
class Numeric(BaseColumnType):
    Number = 0
    Int = 1
    TinyInt = 2
    SmallInt = 3
    MediumInt = 4
    BigInt = 5
    Integer = 6
    Long = 7
    Numeric = 8
    Float = 9
    Double = 10
    DoublePrecision = 11
    Dec = 12
    Decimal = 13
    Real = 14


@unique
class Blooean(BaseColumnType):
    Bit = 0
    Bool = 1
    Boolean = 2


@unique
class Currency(BaseColumnType):
    Money = 0
    SmallMoney = 1


@unique
class String(BaseColumnType):
    Char = 0
    Varchar = 1
    String = 2
    Text = 3
    Longtext = 4
    MediumText = 5
    TinyText = 6
    Nchar = 7
    Ntext = 8
    Enum = 9


@unique
class Set(BaseColumnType):
    Set = 0


@unique
class Binary(BaseColumnType):
    Blob = 0
    LongBlob = 1
    MediumBlob = 2
    TinyBlob = 3
    Binary = 4
    VarBinary = 5


@unique
class ID(BaseColumnType):
    Uuid = 0
    Identity = 1
    Identifier = 2
    UniqueIdentifier = 3


@unique
class DateTime(BaseColumnType):
    Date = 0
    Time = 1
    DateTime = 2
    DateTime2 = 3
    SmallDatetime = 4
    DatetimeOffset = 5
    Year = 6
    Timestamp = 7


@unique
class Others(BaseColumnType):
    pass


@unique
class Unknown(BaseColumnType):
    pass


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
    - fk_tab_obj
    - fk_cols: list[Column]
    - ref_tab: Table
    - ref_cols: list[Column]

    Returns
    -------
    - a ForeignKey object
    """

    def __init__(self, fk_tab_obj, fk_col_list, ref_tab_obj, ref_col_list):
        self.__fk_tab_obj = fk_tab_obj
        self.__fk_col_list = fk_col_list
        if not isinstance(ref_tab_obj, Table):
            raise ValueError("param `ref_tab_obj` must be a Table object!")
        self.__ref_tab_obj = ref_tab_obj
        self.__ref_col_list = ref_col_list

    @property
    def fk_tab(self):
        return self.__fk_tab_obj

    @property
    def fk_cols(self):
        return self.__fk_col_list

    @property
    def ref_tab(self):
        return self.__ref_tab_obj

    @property
    def ref_cols(self):
        return self.__ref_col_list


class Index:
    """Index class for UniqueIndex and Index.

    Params
    ------
    - index_type: str
    - index_cols: list[Column]

    Attribs
    -------
    - index_type: str
    - index_cols: list[Column]

    Returns
    -------
    - an Index object
    """

    def __init__(self, index_type, index_cols):
        self.__index_type = index_type
        self.__index_cols = index_cols

    @property
    def index_type(self):
        return self.__index_type

    @property
    def index_cols(self):
        return self.__index_cols


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
        col_type=None,
        is_notnull=False,
    ):
        self.col_name = col_name
        self.col_type = col_type
        self.is_notnull = is_notnull

    def is_col_inferred_notnull(self):
        return self.is_notnull

    def print_for_lm_components(self):
        """multi-line, new format, for classification csv
        """
        str_list = [self.cleansed_col_name()]
        if self.is_col_inferred_notnull():
            # str_list.append(TOKEN_NOTNULL)
            pass
        else:
            str_list.append('')

        return str_list

    def cleansed_col_name(self):
        return self.col_name.strip('@\'`"[]')


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

    def __init__(self, tab_name, hashid, key_list=None, fk_list=None, index_list=None):
        self._tab_name = tab_name
        self._hashid = hashid
        self._key_list = key_list
        self._fk_list = fk_list
        self._index_list = index_list
        self._name2col = dict()
        self._col_name_seq = list()  # log the order in which cols are added into the table (leftness etc. matter)
        self._col_freq_aggregate = dict()
        self._col_freq_groupby = dict()

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
    def index_list(self):
        if self._index_list is None:
            self._index_list = list()
        return self._index_list

    @property
    def name2col(self):
        return self._name2col

    @property
    def col_name_seq(self):
        return self._col_name_seq

    @property
    def col_freq_aggregate(self):
        return self._col_freq_aggregate

    @property
    def col_freq_groupby(self):
        return self._col_freq_groupby

    def insert_col(self, col):
        """Insert a new column into the table object."""
        if col.col_name in self.name2col:
            # raise Exception(f"col already exists in table: {col.col_name}")
            return
        self.name2col[col.col_name] = col
        
    def print_for_lm_multi_line(self):
        file_name = self._hashid
        table_name = self.cleansed_table_name()

        # if(file_name == '8634986411516259819.sql'):
        #     debug = 1

        # if(file_name == '6400924341428078254.sql'):
        #     debug = 1
        components = ['', '', '', '']
        components[0] = file_name
        components[1] = utils.convert_camel_to_underscore(table_name)

        # get keys
        unique_key_col_names_for_unique = set()
        pk_col_names_for_notnull = set()
        for key in self.key_list:
            # first populate unique_key_col_names for [NOTNULL]
            # if primary key, all columns (even composite) need to be not null (the same is not true for Unique Index per https://stackoverflow.com/questions/386040/whats-wrong-with-nullable-columns-in-composite-primary-keys)
            if(key.key_type == 'PrimaryKey'):
                for key_col in key.key_col_list:
                    pk_col_names_for_notnull.add(key_col.col_name)

            # next populate unique_key_col_names for [UNIQUE]
            # index does not mean the cols have to be unique, skip
            if(key.key_type == 'Index'):
                # print('Index type')
                continue  # index, do nothing
            # CandidateKey corresponds to "Key" keyword in sql (MySQL), which however does not mean uniqueness (https://stackoverflow.com/questions/924265/what-does-the-key-keyword-mean)
            if(key.key_type == 'CandidateKey'):
                # print('CandidateKey')
                continue  # this does mean the col has to be unique, see 8948003700246411308.sql, table pregled, https://stackoverflow.com/questions/924265/what-does-the-key-keyword-mean

            # unique key and unique index may still have NULLs， per https://stackoverflow.com/questions/767657/how-do-i-create-a-unique-constraint-that-also-allows-nulls
            if(key.key_type == 'UniqueKey' or key.key_type == 'UniqueIndex'):
                continue

            # other keys with more than one col? then the col itself cannot be assumed unique
            if(len(key.key_col_list) > 1):
                continue

            # add the unique columns to unique_key_col_names, so that we can add [UNIQUE] label when we print
            for key_col in key.key_col_list:
                if(len(key.key_col_list) > 1):
                    debug = 1
                unique_key_col_names_for_unique.add(key_col.col_name)

        # there are cases for SQLLite that may have multiple PK defined on same col,
        # e.g., 4737724513843675438.sql, table `message', which has two PK defined on two cols in the same table
        all_pks_same_table = [key for key in self.key_list if key.key_type == 'PrimaryKey']
        has_multiple_pk_on_same_table = len(all_pks_same_table) > 1

        # this is equivalent to Column's print_for_lm_components(), though rewritten here
        col_lm_str_list = list()
        for col_name in self.col_name_seq:
            col_obj = self.name2col[col_name]
            notnull = (col_obj.is_notnull or (col_name in pk_col_names_for_notnull))
            # only if when there are no >1 PK defined on the same table, we treat such single-col PK as valid for UNIQUE
            unique = (not has_multiple_pk_on_same_table) and (col_name in unique_key_col_names_for_unique)

            label = '[UNIQUE]' if unique else '[NOTNULL]' if notnull else ''
            components[2] = utils.convert_camel_to_underscore(col_obj.cleansed_col_name())
            components[3] = label

            # replace possible ' ' and ',' to '_', to avoid CSV parsing issue
            # also make sure that all variables are single tokens
            components[1] = components[1].replace(' ', '_').replace(',', '_')
            components[2] = components[2].replace(' ', '_').replace(',', '_')

            col_lm_str_list.append(','.join(components))
        return col_lm_str_list

    # def print_for_lm_multi_line(self):
    #     """Generate text for language modeling."""
    #     # iterate cols in the order in which they are added
    #     col_lm_str_list = list()
    #     for col_name in self.col_name_seq:
    #         col_obj = self.name2col[col_name]
    #         components = col_obj.print_for_lm_components()
    #         components.insert(0, self.hashid)  # add file-name
    #         components.insert(1, self.cleansed_table_name())  # add file-name

    #         # skip line if components[0] (table-name) has bad punct (likely bad sql parse)
    #         if ' ' in components[1] or ',' in components[1]:
    #             print("skipping line for bad parse (punct in tab_name): " + components[1])
    #             continue
    #         # skip line if components[0] (table-name) has bad punct (likely bad sql parse)
    #         if ' ' in components[2] or ',' in components[2]:
    #             print("skipping line for bad parse (punct in col_name): " + components[2])
    #             continue
    #         col_lm_str_list.append(','.join(components))

    #     return col_lm_str_list

    def cleansed_table_name(self):
        """Clean table name."""
        clean_tab_name = self.tab_name
        if '.' in clean_tab_name:
            clean_tab_name = clean_tab_name.split('.')[1]
        return clean_tab_name.strip('#@\'`"[]')


class Pipeline:
    """Pipeline class for automatically manage queue's in & out."""

    def __init__(self, q_obj):
        self.q_obj = q_obj
        self.f_obj_tmp = self.q_obj.popleft()

    def __enter__(self):
        return self.f_obj_tmp

    def __exit__(self, type, value, traceback):
        self.q_obj.append(self.f_obj_tmp)
