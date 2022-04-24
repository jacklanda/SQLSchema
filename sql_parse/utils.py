# -*- coding: utf-8 -*-
# @author: Yang Liu
# @email: v-yangliu4@microsoft.com


import os
import re
import glob
import signal
from pprint import pprint
from random import sample
from encodings import aliases
from dataclasses import dataclass

import sqlparse
from bs4 import UnicodeDammit

BINARY_OP = ["=", "<", ">", "<=", ">="]

CHARSET_LIST = list(set([v for _, v in aliases.aliases.items()]))


class ColumnTypeDict:
    """Original SQL Column Type to Self-defined Column Type."""

    __column_type_dict = {
        "number": "Numeric",
        "int": "Numeric",
        "tinyint": "Numeric",
        "smallint": "Numeric",
        "mediumint": "Numeric",
        "bigint": "Numeric",
        "integer": "Numeric",
        "long": "Numeric",
        "numeric": "Numeric",
        "float": "Numeric",
        "double": "Numeric",
        "double precision": "Numeric",
        "dec": "Numeric",
        "decimal": "Numeric",
        "real": "Numeric",
        "serial": "Numeric",
        "bigserial": "Numeric",
        "binary_float": "Numeric",
        "binary_double": "Numeric",
        "decfloat": "Numeric",
        "byte": "Numeric",
        "single": "Numeric",
        "autonumber": "Numeric",
        "bit": "Boolean",
        "bool": "Boolean",
        "boolean": "Boolean",
        "money": "Currency",
        "smallmoney": "Currency",
        "currency": "Currency",
        "char": "String",
        "varchar": "String",
        "string": "String",
        "text": "String",
        "longtext": "String",
        "mediumtext": "String",
        "tinytext": "String",
        "nchar": "String",
        "ntext": "String",
        "character": "String",
        "character varying": "String",
        "varchar2": "String",
        "nvarchar2": "String",
        "graphic": "String",
        "vargraphic": "String",
        "set": "Set",
        "blob": "Binary",
        "longblob": "Binary",
        "mediumblob": "Binary",
        "tinyblob": "Binary",
        "binary": "Binary",
        "varbinary": "Binary",
        "raw": "Binary",
        "long raw": "Binary",
        "clob": "Binary",
        "nclob": "Binary",
        "dbclob": "Binary",
        "bfile": "Binary",
        "uuid": "ID",
        "identify": "ID",
        "identifier": "ID",
        "uniqueidentifier": "ID",
        "date": "DateTime",
        "time": "DateTime",
        "datetime": "DataTime",
        "datetime2": "DateTime",
        "smalldatetime": "DateTime",
        "datetimeoffset": "DateTime",
        "year": "DateTime",
        "timestamp": "DateTime",
        "interval": "DateTime",
        "interval year": "DateTime",
        "interval day": "DateTime",
        "point": "Geometric",
        "line": "Geometric",
        "lseg": "Geometric",
        "box": "Geometric",
        "path": "Geometric",
        "polygon": "Geometric",
        "circle": "Geometric",
        "sdo_geometriy": "Geometric",
        "sdo_topo_geometry": "Geometric",
        "sdo_georaster": "Geometric",
        "httpuritype": "URI",
        "xdburitype": "URI",
        "dburitype": "URI",
        "hyperlink": "URI",
        "ordaudio": "Media",
        "orddicom": "Media",
        "orddoc": "Media",
        "ordimage": "Media",
        "ordvideo": "Media",
        "ordimagesignature": "Media",
        "si_averagecolor": "Media",
        "si_color": "Media",
        "si_colorhistogram": "Media",
        "si_featurelist": "Media",
        "si_positionalcolor": "Media",
        "si_stillimage": "Media",
        "si_texture": "Media",
        "enum": "Enum",
        "sql_variant": "sql_variant",
        "xml": "XML",
        "cursor": "Cursor",
        "table": "Table",
        "image": "Image",
        "hierarchyid": "Hierarchyid",
        "rawid": "Rawid",
        "urowid": "Urowid",
        "anytype": "Anytype",
        "anydata": "Anydata",
        "anydataset": "Anydataset",
        "memo": "Memo",
    }

    @property
    def data(self):
        return self.__column_type_dict

    def __getitem__(self, __key):
        if __key not in self.__column_type_dict:
            raise KeyError("Please check input key for access related column type!")
        return self.__column_type_dict[__key]

    def __call__(self, __key):
        return self.__getitem__(__key)


class RegexDict:
    """Define and retrieve regex by index

    Examples
    --------
    - split_clause_by_comma: split CREATE TABLE block into single clauses according to.

    - get_create_table_name: extract table name on CREATE TABLE statement.

    - get_alter_table_name: extract table name on ALTER TABLE statement.

    - constraint_pk_create_table: extract CONSTRAINT PRIMARY KEY's cols in clause.
    ```SQL
    CREATE TABLE "xxPerson_o"
    (
        "FirstName"  VarChar(255)  NOT NULL,
        "PersonID"   Int           NOT NULL,
        "LastName"   VarChar(255)  NOT NULL,
        "MiddleName" VarChar(255)      NULL,
        "Gender"     Char(1)       NOT NULL,
        CONSTRAINT "PK_xxPerson_o" PRIMARY KEY ("PersonID")
    );
    ```

    - constraint_fk_create_table: extract CONSTRAINT FOREIGN KEY's
      cols, referred table and referred cols in clause.
    ```SQL
    CREATE TABLE public.video
    (
        id SERIAL PRIMARY KEY NOT NULL,
        user_id INT NOT NULL,
        CONSTRAINT video_user_id_fk FOREIGN KEY (user_id) REFERENCES "user" (id)
    );
    ```

    - constraint_unique_create_table: extract CONSTRAINT UNIQUE's cols in clause
    ```SQL
    create table studentCourse (
        ID                  integer         primary key auto_increment,
        studentID           integer             not null,
        courseID            integer             not null,
        CONSTRAINT cou_stu unique (courseID, studentID)
    );
    ```

    - startwith_fk_create_table: extract FK's
      cols, referred table and referred cols in clause which startswith FK.
    ```SQL
    CREATE TABLE raw_ip_addrs (
        tool_run_id                 UUID            NOT NULL,
        ip_addr                     INET            NOT NULL,
        PRIMARY KEY (tool_run_id, ip_addr),
        FOREIGN KEY (tool_run_id) REFERENCES tool_runs(id)
    );
    ```

    - startwith_uk_create_table: extract UNIQUE KEY's cols in clause which startswith UK.
    ```SQL
    CREATE TABLE `lkupcounty` (
        `countyId` int(11) NOT NULL AUTO_INCREMENT,
        `stateId` int(11) NOT NULL,
        `countyName` varchar(100) NOT NULL,
        `initialtimestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY `unique_county` (`stateId`,`countyName`),
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    ```

    - candidate_key_create_table: extract KEY's cols.
    ```SQL
    CREATE TABLE `guild_bank_eventlog` (
        `guildid` int(11) unsigned NOT NULL default '0' COMMENT 'Guild Identificator',
        KEY `guildid_key` (`guildid`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    ```

    - startwith_ui_create_table: extract UNIQUE INDEX's cols.
    ```SQL
    CREATE TABLE IF NOT EXISTS `RONDIER`.`Pays` (
        `idPays` INT NOT NULL AUTO_INCREMENT,
        UNIQUE INDEX `idPays_UNIQUE` (`idPays` ASC)
    )
    ENGINE = InnoDB;
    ```

    - startwith_unique_create_table: extract UNIQUE (KEY)'s cols.
    ```SQL
    CREATE TABLE acteurs
    (
        id     SERIAL UNIQUE,
        nom    VARCHAR(128) NOT NULL,
        prenom VARCHAR(128) NOT NULL,
        PRIMARY KEY (id),
        UNIQUE (nom, prenom)
    );
    ```

    - startwith_index_create_table: extract INDEX's cols.
    ```SQL
    CREATE TABLE `groups`  (
        `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT,
        `user_id` int(10) NOT NULL COMMENT '创建者（管理员）id',
        `delete_time` datetime(3) NULL DEFAULT NULL,
        INDEX `idx_id_user`(`id`, `user_id`, `delete_time`) USING BTREE COMMENT 'id与user_id的联合索引'
    ) ENGINE = InnoDB AUTO_INCREMENT = 23 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;
    ```

    - add_constraint_pk_alter_table: extract PK's cols.
    ```SQL
    ALTER TABLE [edfi].[SectionCharacteristic]
        ADD CONSTRAINT [SectionCharacteristic_PK] PRIMARY KEY CLUSTERED  ([SectionCharacteristicDescriptorId]);
    ```

    - add_pk_alter_table: extract PK's cols.
    ```SQL
    ALTER TABLE llx_rights_def ADD PRIMARY KEY pk_rights_def (id, entity);
    ```

    - add_constraint_fk_alter_table: extract FK's cols, referred table and referred cols.
    ```SQL
    ALTER TABLE "APP"."SKEWED_VALUES"
        ADD CONSTRAINT "SKEWED_VALUES_FK2" FOREIGN KEY ("STRING_LIST_ID_EID")
        REFERENCES "APP"."SKEWED_STRING_LIST" ("STRING_LIST_ID") ON DELETE NO ACTION ON UPDATE NO ACTION;
    ```

    - add_fk_alter_table: extract FK's cols, referred table and referred cols.
    ```SQL
    ALTER TABLE BOM ADD FOREIGN KEY (Parent) REFERENCES StockMaster (StockID);
    ```

    - add_unique_key_alter_table: extract UNIQUE KEY's cols.
    ```SQL
    ALTER TABLE `users`
        ADD PRIMARY KEY (`id`),
        ADD UNIQUE KEY `users_email_unique` (`email`);
    ```

    - add_unique_index_alter_table: extract UNIQUE INDEX's cols.
    ```SQL
    ALTER TABLE `patient`
        ADD UNIQUE INDEX `patient_id_UNIQUE` (`patient_id` ASC);
    ```

    - add_constraint_unique_alter_table: extract CONSTRAINT UNIQUE (KEY)'s cols.
    ```SQL
    ALTER TABLE KRCR_STYLE_T ADD CONSTRAINT UNIQUE INDEX KRCR_STYLE_TC0 (OBJ_ID);
    ```

    - create_unique_index_alter_table: extract CREATE UNIQUE INDEX's cols.
    ```SQL
    CREATE UNIQUE INDEX area_idx_gid ON area (gid);
    ```

    - add_key_alter_table: extract ADD KEY's cols.
    ```SQL
    ALTER TABLE `cf_commentmeta`
        ADD KEY `comment_id` (`comment_id`);
    ```

    - create_index_or_unique_index.
    ```SQL
    CREATE INDEX IDX_AC_TID ON IDN_OAUTH2_AUTHORIZATION_CODE(TOKEN_ID);
    ```
    """
    __regex_dict = {
        "split_clause_by_comma": r",(?![^\(]*[\)])",
        "get_create_table_name": "create\stable\s(if\snot\sexists\s)?(.*?)[\s|\(]",
        "get_alter_table_name": "alter\stable\s(only\s)?(.*?)\s",
        "constraint_pk_create_table": "\((.*?)\)",
        "constraint_fk_create_table": "foreign\s+key\s*.*?\((.*?)\)\s*references\s*([`|'|\"]?.*?[`|'|\"]?)\s*\((.*?)\)",
        "constraint_unique_create_table": "\((.*?)\)",
        "startwith_fk_create_table": "foreign\s*key\s*.*?\((.*?)\)\s*references\s*([`|'|\"]?.*?[`|'|\"]?)\s*\((.*?)\)",
        "startwith_fk_create_table_backup": "foreign\s*key\s*.*?\((.*?)\)\s*references\s*(.*?)\s+on",
        "startwith_uk_create_table": "unique\s*key\s*.*?\((.*?)\)",
        "candidate_key_create_table": "\((.*)\)",
        "startwith_ui_create_table": "unique\s+index\s+([`|'|\"]?.*?[`|'|\"]?)\s*\((.*?)\)",
        "startwith_unique_create_table": "\((.*)\)",
        "startwith_index_create_table": "index\s+.*\((.*?)\)",
        "add_constraint_pk_alter_table": "primary\s*key\s*\((.*?)\)",
        "add_pk_alter_table": "\((.*)\)",
        "add_constraint_fk_alter_table": "foreign\s*key\s*\(?(.*?)\)?\s*references\s*([`|'|\"]?.*?[`|'|\"]?)\s*\((.*?)\)",
        "add_fk_alter_table": "\(([`|'|\"]?.*?[`|'|\"]?)\)\s*references\s*([`|'|\"]?.*?[`|'|\"]?)\s*\((.*?)\)",
        "add_unique_key_alter_table": "(.*?)\s*\((.*?)\)",
        "add_unique_index_alter_table": "\((.*?)\)",
        "add_constraint_unique_alter_table": "add\s*constraint\s*.*?\((.*?)\)",
        "create_unique_index_alter_table": "on\s+(.*?)\s*\((.*?)\s*(ASC)?(DESC)?\)",
        "add_key_alter_table": "\((.*?)\(?\d*\)",
        "create_index_or_unique_index": "\s+on\s+(.*?)\s*(using\sbtree\s*)?\(\(?(.*?)\)?\)",
    }

    @property
    def data(self):
        return self.__regex_dict

    def __getitem__(self, __key):
        if __key not in self.__regex_dict:
            raise KeyError("Please check input key for access related regex!")
        return self.__regex_dict[__key]

    def __call__(self, __key):
        return self.__getitem__(__key)


@dataclass
class Counter:
    """Class for nums counting.
    Return the number which have plus 1 after a function call.

    Params
    ------
    - None

    Returns
    -------
    - num: int
    """
    __num: int = 0

    @property
    def num(self):
        return self.__num

    def add(self):
        self.__num += 1
        return self.__num

    __call__ = add

    def minus(self):
        self.__num -= 1
        return self.__num


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


def fmt_str(s):
    """Remove puncs like `, ', " for string which wrapped with them.

    Params
    ------
    - s: str

    Returns
    -------
    - str
    """
    return s.replace('\'', '').replace('"', '').replace('`', '').strip() if isinstance(s, str) else ""


def rm_kw(s):
    """Remove keywords like asc, desc after cols"""
    return s.replace(" asc", "").replace(" desc", "").replace(" ASC", "").replace(" DESC", "").strip()


def norm_colname(s):
    s_input = s
    s = s.replace('[', '').replace(']', '')
    if '(' in s:
        s = s.split('(', 1)[0].strip()
    elif ')' in s:
        s = s.split(')', 1)[0].strip()
    if "::" in s:
        s = s.rsplit("::", 1)[-1]
    return s.strip() if s != "" else s_input


def clean_stmt(stmt):
    """Remove useless keyword in SQL, e.g. COMMENT"""
    # remove COMMENT ...
    # pattern = "(\s+comment\s*[\s|=]?\s*['|\"\`].*?['|\"\`])[,|\n|;]"
    # result = re.findall(pattern, stmt, re.IGNORECASE)
    pat = re.compile("(\s+comment\s*[\s|=]?\s*['|\"\`].*?['|\"\`])[,|\n|;]", re.IGNORECASE)
    result = pat.findall(stmt)
    for item in result:
        stmt = stmt.replace(item, "")
    # remove type size with parentheses
    pat = re.compile("\(\d+[,\s*\d*]*\)", re.IGNORECASE)
    stmt = pat.sub("", stmt)
    # stmt = re.sub("\(\d+[,\s*\d*]*\)", "", stmt, re.IGNORECASE)
    return stmt


def split_string(s, sep, maxsplit=1, get_first=False):
    s_raw, s_lower, sep = s, s.lower(), sep.lower()
    if sep in s_lower:
        if not get_first:
            bgn_idx = s_lower.find(sep)
            if bgn_idx == -1:
                print("split error!")
            return s_raw[int(bgn_idx + len(sep)):] if bgn_idx != -1 else s_lower.split(sep, maxsplit)[1]
        else:
            end_idx = s_lower.find(sep)
            if end_idx == -1:
                print("split error!")
            return s_raw[:int(end_idx)] if end_idx != -1 else s_lower.split(sep, maxsplit)[0]
    else:
        return s_raw


def convert_camel_to_underscore(s):
    if not s:
        return s
    res = [s[0].lower()]
    within_upper = False
    for i, c in enumerate(s[1:], start=1):  # s[1:]:
        # encounter first upper, insert "_" and lower char
        if (within_upper == False and c in ('ABCDEFGHIJKLMNOPQRSTUVWXYZ')):
            within_upper = True
            # if the previous char is not "_", append "_"
            if(s[i - 1] in ('ABCDEFGHIJKLMNOPQRSTUVWXYZ').lower()):
                res.append('_')
            res.append(c.lower())
        # countinue to encounter upper, insert a lower char
        elif (within_upper == True and c in ('ABCDEFGHIJKLMNOPQRSTUVWXYZ')):
            res.append(c.lower())
        # encounter lower/digit, etc.
        else:
            within_upper = False
            res.append(c.lower())

    return ''.join(res)


def query_stmt_split(fpath, filter_join_query=False):

    def from_multitables(s):
        clause = s.split("from")[1].split("where")[0]
        return True if ',' in clause else False

    split_by_newline = list()
    split_by_semicolon = list()
    # with open(fpath, "r", errors="ignore") as fp:
    lines = open_sql_file(fpath)
    # lines = fp.readlines()
    stmt = ""
    for line in lines:
        if len(line.strip()) == 0:
            if stmt.strip() != "":
                split_by_newline.append(stmt)
            stmt = ""
            continue
        stmt += line
    # """
    if stmt not in split_by_newline:
        split_by_newline.append(stmt)
    # """
    for stmt in split_by_newline:
        sub_stmts = stmt.split(';')
        try:
            with Timeout(3):
                if filter_join_query:
                    split_by_semicolon += [sqlparse.format(s.strip(), strip_comments=True)
                                        for s in sub_stmts
                                        if s != '\n'
                                        and "select " in s.lower()
                                        and "from " in s.lower()
                                        and (("join " in s.lower())
                                                or ("where " in s.lower() and from_multitables(s.lower())))
                                        and any(op in s for op in BINARY_OP)]
                elif not filter_join_query:
                    split_by_semicolon += [sqlparse.format(s.strip(), strip_comments=True)
                                        for s in sub_stmts
                                        if s != '\n' and "select " in s.lower() and "from " in s.lower()]
        except:
            continue

    stmts = [' '.join(s.split()) for s in split_by_semicolon]

    # return [convert_camel_to_underscore(s) for s in stmts if any(op in s for op in BINARY_OP)]
    # return [s for s in stmts if any(op in s for op in BINARY_OP)] if filter_join_query else stmts
    return stmts


def calc_col_cov(table_lhs, table_rhs):
    n2c_lhs = table_lhs.name2col
    n2c_rhs = table_rhs.name2col
    lost_nums = 0
    for col_name, _ in n2c_rhs.items():
        if col_name not in n2c_lhs:
            lost_nums += 1
    return lost_nums


def get_chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def open_sql_file(fpath):
    # print("open a sql file")
    try:
        with open(fpath, encoding="utf-8", errors="strict") as f:
            return f.readlines()
    except UnicodeError:
        # print("sql file open failed with utf8")
        blob = open(fpath, "rb").read()
        charset = UnicodeDammit(blob).original_encoding
        if charset in CHARSET_LIST:
            with open(fpath, encoding=charset, errors="strict") as f:
                return f.readlines()
    except UnicodeError:
        for charset in CHARSET_LIST:
            try:
                with open(fpath, encoding=charset, errors="strict") as f:
                    return f.readlines()
            except UnicodeError:
                pass
    # print(f"all charset parse fail at: {fpath}")
    try:
        with open(fpath, encoding="utf-8", errors="ignore") as f:
            return f.readlines()
    except:
        return list()


if __name__ == "__main__":
    # test statement split
    # """
    import time
    from pickle import load, dump
    from parse_join_query import SqlparseParser, print_join_obj
    from repo_parse_sql import Repository

    sample_num = 10000
    # fpath = f"data/samples/fpath_list_{str(sample_num)}_{time.strftime('%Y_%m_%d_%H:%M:%S')}.pkl"
    INPUT_FOLDER = os.path.join(os.getcwd(), "data/s3_sql_files_crawled_all_vms")
    files = [f for f in glob.glob(os.path.join(INPUT_FOLDER, "*.sql"))]
    # print()
    # fp_list = sample(files, sample_num)
    fp_list = files
    # fpath = "data/samples/fpath_list_11k_2022_01_18_02:15:15.pkl"
    # fp_list = sample(load(open(fpath, "rb")), 100)
    # fp_list = ["failed_cases.txt"]
    # dump(fp_list, open(fpath, "wb"))
    # fp_list = files
    total = 0
    parse_succ = 0
    parser = SqlparseParser()
    for fp in fp_list:
        print('-' * 120)
        stmt_list = query_stmt_split(fp)
        for s in stmt_list:
            total += 1
            print('*' * 120)
            s = s.lower()
            s = fmt_str(s)
            try:
                with Timeout(1):
                    s = sqlparse.format(s, strip_comments=True)
            except:
                pass
            s = ' '.join(s.split())
            try:
                if "join " in s:
                    query_obj_list = parser.parse_statement_select_join_sqlparse(s)
                    print(s)
                    print()
                    for query_obj in query_obj_list:
                        for join_obj in query_obj.binary_joins:
                            print_join_obj(join_obj)
                            print()
                    parse_succ += 1
                elif "where " in s:
                    query_obj_list = parser.parse_statement_select_where_sqlparse(s)
                    print(s)
                    print()
                    for query_obj in query_obj_list:
                        for join_obj in query_obj.binary_joins:
                            print_join_obj(join_obj)
                            print()
                    parse_succ += 1
            except:
                pass
            print(f"parse coverage({parse_succ}/{total}):", parse_succ / total)
    print(f"parse coverage({parse_succ}/{total}):", parse_succ / total)
    exit()
    # """

    # test for RegexDict
    regex_dict = RegexDict()
    print(regex_dict["add_constraint_pk_alter_table"])
    print(regex_dict("add_fk_alter_table"))
    print(regex_dict.data)

    # test for Counter
    counter = Counter()
    for i in range(100):
        print(counter())

    # test for fmt_str
    s = """CREATE TABLE `ast_AppMenus_M` (
    `menuId` VARCHAR(64) NOT NULL,
    `menuTreeId` VARCHAR(256) NOT NULL,
    `menuIcon` VARCHAR(256) NULL DEFAULT NULL,
    `menuAction` VARCHAR(256) NULL DEFAULT NULL,
    `menuCommands` VARCHAR(64) NULL DEFAULT NULL,
    `menuDisplay` TINYINT(1) NOT NULL,
    `menuHead` TINYINT(1) NOT NULL,
    `menuLabel` VARCHAR(256) NULL DEFAULT NULL,
    PRIMARY KEY (`menuId`));"""

    print(fmt_str(s.lower()))

    # test for rm_kw
    s = """CREATE TABLE IF NOT EXISTS `RONDIER`.`Pays` (
    `idPays` INT NOT NULL AUTO_INCREMENT,
    `Pays` VARCHAR(45) NOT NULL,
    PRIMARY KEY (`idPays`),
    UNIQUE INDEX `idPays_UNIQUE` (`idPays` ASC))
    UNIQUE INDEX `Pays_UNIQUE` (`Pays` DESC))
    ENGINE = InnoDB;"""

    print(rm_kw(s.lower()))

    # test for clean_stmt
    s = """create table sys_dept (
    dept_id           bigint(20)      not null auto_increment    comment '部门id',
    parent_id         bigint(20)      default 0                  comment '父部门id',
    ancestors         varchar(50)     default ''                 comment '祖级列表',
    dept_name         varchar(30)     default ''                 comment '部门名称',
    order_num         int(4)          default 0                  comment '显示顺序',
    leader            varchar(20)     default null               comment '负责人',
    primary key (dept_id)
    ) engine=innodb auto_increment=200 comment = '部门表';"""

    print(clean_stmt(s.lower()))
