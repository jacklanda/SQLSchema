# -*- coding: utf-8 -*-
# @author: Yang Liu
# @email: v-yangliu4@microsoft.com


import re
from dataclasses import dataclass


class RegexDict:
    """Define and retrieve regex by index

    Examples
    --------
    - split_clause_by_comma: split CREATE TABLE block into single clauses according to.

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
    """
    __regex_dict = {
        "split_clause_by_comma": r",(?![^\(]*[\)])",
        "constraint_pk_create_table": "\((.*?)\)",
        "constraint_fk_create_table": "foreign\s*key\s*\(?(.*?)\)?\s*references\s*([`|'|\"]?.*?[`|'|\"]?)\s*\((.*?)\)",
        "constraint_unique_create_table": "\((.*?)\)",
        "startwith_fk_create_table": "foreign\s*key\s*\(?(.*?)\)?\s*references\s*([`|'|\"]?.*?[`|'|\"]?)\s*\((.*?)\)",
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
    return s.replace(" asc", "").replace(" desc", "").strip()


def clean_stmt(stmt):
    """Remove useless keyword in SQL, e.g. COMMENT"""
    # remove COMMENT ...
    pattern = "(\s+comment\s*[\s|=]?\s*['|\"\`].*['|\"\`])[,|\n|;]"
    result = re.findall(pattern, stmt, re.IGNORECASE)
    for each in result:
        stmt = stmt.replace(each, "")
    return stmt


if __name__ == "__main__":
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
