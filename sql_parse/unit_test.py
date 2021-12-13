import unittest


def test_badcase():
    return r"""CREATE TABLE "user" (
        "user_id" serial NOT NULL,
        "last_name" varchar(20),
        "user_handle" varchar(20) UNIQUE,
        "user_phone" varchar(20),
        "user_hash" varchar(250) NOT NULL,
        "is_dispatcher" boolean default false,
        CONSTRAINT user_pk PRIMARY KEY ("user_id")
    );
    """


def get_pk_def_case_on_create():
    return r"""-- case 0
    CREATE TABLE `myauth_user_evaluation`  (
      `id` int(11) NOT NULL AUTO_INCREMENT,
      `user_id` double NOT NULL,
      `movie_id` double NOT NULL,
      `score` double NOT NULL,
      PRIMARY KEY (`id`, `user_id`, `movie_id`) USING BTREE,
      UNIQUE INDEX `myauth_user_evaluation_user_id_movie_id_6d16dd29_uniq`(`user_id`, `movie_id`) USING BTREE
    ) ENGINE = InnoDB AUTO_INCREMENT = 32132 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;
    -- case 1
    CREATE TABLE liberacaoproblema (  
        idliberacao integer NOT NULL,  
        idproblema integer NOT NULL,  
        PRIMARY KEY (idliberacao,idproblema),
    );
    -- case 2
    CREATE TABLE "LoginFailures" (
        "ipaddress" VARCHAR(100)    PRIMARY KEY NOT NULL,
        "failureCount"  INT NOT NULL,
        "lastFailure"   TIMESTAMP   NOT NULL
    );
    -- case 3
   CREATE TABLE "user" (
        "id"                  SERIAL   PRIMARY KEY,
        "login"               TEXT     NOT NULL UNIQUE,
        "password"            TEXT     NOT NULL
    );
    CREATE TABLE "contact_person" (
        "id"                  INTEGER  PRIMARY KEY REFERENCES "user" ON UPDATE RESTRICT ON DELETE CASCADE, /* связь один-к-одному */
#         "id"                  INTEGER  PRIMARY KEY REFERENCES "user" ("id"),
        "surname"             TEXT     NOT NULL,
        "name"                TEXT     NOT NULL,
        "patronymic"          TEXT     NOT NULL
    ); 
    -- case 4
    CREATE TABLE IF NOT EXISTS users(
        UserID SERIAL PRIMARY KEY,
        UserName varchar(255) UNIQUE,
        FirstName varchar(255),
        LastName varchar(255),
        Email varchar(255),
        Type varchar(50)
    );
    CREATE TABLE IF NOT EXISTS credentials(
        PasswordHash varchar(255),
        Salt integer,
        UserName varchar(255) PRIMARY KEY REFERENCES users(UserName, FirstName, LastName) ON DELETE CASCADE
    );"""


def get_index_def_case_on_create():
    return r"""-- case 0
    CREATE TABLE `requirements` (
      `id` INT( 10 ) UNSIGNED NOT NULL AUTO_INCREMENT ,
      `id_srs` INT( 10 ) UNSIGNED NOT NULL ,
      `req_doc_id` varchar(16) default NULL ,
      `title` VARCHAR( 100 ) NOT NULL ,
      `scope` TEXT,
      `status` char(1) default 'v' NOT NULL,
      `type` char(1) default NULL,
      `id_author` INT( 10 ) UNSIGNED NULL,
      `create_date` date NOT NULL default '0000-00-00',
      `id_modifier` INT( 10 ) UNSIGNED NULL,
      `modified_date` date NOT NULL default '0000-00-00',
      PRIMARY KEY ( `id` ) ,
      INDEX ( `id_srs` , `status` ),
      KEY `req_doc_id` (`req_doc_id`)
    ) TYPE=MyISAM;
    -- case 1
    CREATE TABLE QRTZ_BLOB_TRIGGERS (
        SCHED_NAME VARCHAR(120) NOT NULL,
        TRIGGER_NAME VARCHAR(200) NOT NULL,
        TRIGGER_GROUP VARCHAR(200) NOT NULL,
        BLOB_DATA BLOB NULL,
        PRIMARY KEY (SCHED_NAME,TRIGGER_NAME,TRIGGER_GROUP),
        INDEX (SCHED_NAME,TRIGGER_NAME, TRIGGER_GROUP)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    -- case 2
    CREATE TABLE i18n (
        id int(10) NOT NULL auto_increment,
        locale varchar(6) NOT NULL,
        model varchar(255) NOT NULL,
        foreign_key int(10) NOT NULL,
        field varchar(255) NOT NULL,
        content mediumtext,
        PRIMARY KEY (id),
        INDEX locale (locale),
        INDEX model (model),
        INDEX row_id (foreign_key),
        INDEX field (field)
    );
    """


def get_fk_def_case_on_create():
    return r"""-- case 0
    CREATE TABLE author(
        author_id INT,
        author_name VARCHAR(20),
        author_city VARCHAR(20),
        author_country VARCHAR(20),
        PRIMARY KEY(author_id)
    );
    CREATE TABLE publisher(
        publisher_id INT,
        publisher_name VARCHAR(20),
        publisher_city VARCHAR(20),
        publisher_country VARCHAR(20),
        PRIMARY KEY(publisher_id)
    );
    CREATE TABLE category(
        category_id INT,
        description VARCHAR(30),
        PRIMARY KEY(category_id) 
    );
    CREATE TABLE catalogue(
        book_id INT,
        book_title VARCHAR(30),
        author_id INT,
        publisher_id INT,
        category_id INT,
        year INT,
        price INT,
        PRIMARY KEY(book_id),
        FOREIGN KEY(author_id) REFERENCES author(author_id),
        FOREIGN KEY(publisher_id) REFERENCES publisher(publisher_id),
        FOREIGN KEY(category_id) REFERENCES category(category_id),
    );
    -- case 1
    create table master(id int primary key);
    create table detail(
    id int primary key, 
    x bigint, 
    foreign key(`x`) references master(`id`) on delete cascade
    );"""


def get_constraint_begin_on_create():
    return r"""--case 0
    CREATE TABLE qrtz_job_details
    (
      SCHED_NAME VARCHAR2(120) NOT NULL,
      JOB_NAME  VARCHAR2(200) NOT NULL,
      JOB_GROUP VARCHAR2(200) NOT NULL,
      DESCRIPTION VARCHAR2(250) NULL,
      JOB_CLASS_NAME   VARCHAR2(250) NOT NULL,
      IS_DURABLE VARCHAR2(1) NOT NULL,
      IS_NONCONCURRENT VARCHAR2(1) NOT NULL,
      IS_UPDATE_DATA VARCHAR2(1) NOT NULL,
      REQUESTS_RECOVERY VARCHAR2(1) NOT NULL,
      JOB_DATA BLOB NULL,
      CONSTRAINT QRTZ_JOB_DETAILS_PK PRIMARY KEY (SCHED_NAME,JOB_NAME,JOB_GROUP)
    );
    -- case 1
    CREATE TABLE `DATABASECHANGELOG` (
      `ID` VARCHAR(63) NOT NULL, 
      `AUTHOR` VARCHAR(63) NOT NULL, 
      `FILENAME` VARCHAR(200) NOT NULL, 
      `DATEEXECUTED` DATETIME NOT NULL, 
      `ORDEREXECUTED` INT NOT NULL, 
      `EXECTYPE` VARCHAR(10) NOT NULL, 
      `MD5SUM` VARCHAR(35) NULL, 
      `DESCRIPTION` VARCHAR(255) NULL, 
      `COMMENTS` VARCHAR(255) NULL, 
      `TAG` VARCHAR(255) NULL, 
      `LIQUIBASE` VARCHAR(20) NULL, 
      CONSTRAINT `PK_DATABASECHANGELOG` PRIMARY KEY (`ID`, `AUTHOR`, `FILENAME`)
    );
    -- case 2
    CREATE TABLE qrtz_triggers
    (
      SCHED_NAME VARCHAR2(120) NOT NULL,
      TRIGGER_NAME VARCHAR2(200) NOT NULL,
      TRIGGER_GROUP VARCHAR2(200) NOT NULL,
      JOB_NAME  VARCHAR2(200) NOT NULL,
      JOB_GROUP VARCHAR2(200) NOT NULL,
      DESCRIPTION VARCHAR2(250) NULL,
      NEXT_FIRE_TIME NUMBER(13) NULL,
      PREV_FIRE_TIME NUMBER(13) NULL,
      PRIORITY NUMBER(13) NULL,
      TRIGGER_STATE VARCHAR2(16) NOT NULL,
      TRIGGER_TYPE VARCHAR2(8) NOT NULL,
      START_TIME NUMBER(13) NOT NULL,
      END_TIME NUMBER(13) NULL,
      CALENDAR_NAME VARCHAR2(200) NULL,
      MISFIRE_INSTR NUMBER(2) NULL,
      JOB_DATA BLOB NULL,
      CONSTRAINT QRTZ_TRIGGERS_PK PRIMARY KEY (SCHED_NAME,TRIGGER_NAME,TRIGGER_GROUP),
      CONSTRAINT QRTZ_TRIGGER_TO_JOBS_FK FOREIGN KEY (SCHED_NAME,JOB_NAME,JOB_GROUP)
      REFERENCES QRTZ_JOB_DETAILS(SCHED_NAME,JOB_NAME,JOB_GROUP)
    );
    -- case 3
    CREATE TABLE `QRTZ_TRIGGERS` (
      `sched_name` varchar(120) NOT NULL,
      `trigger_name` varchar(200) NOT NULL,
      `trigger_group` varchar(200) NOT NULL,
      `job_name` varchar(200) NOT NULL,
      `job_group` varchar(200) NOT NULL,
      `description` varchar(250) DEFAULT NULL,
      `next_fire_time` bigint DEFAULT NULL,
      `prev_fire_time` bigint DEFAULT NULL,
      `priority` int DEFAULT NULL,
      `trigger_state` varchar(16) NOT NULL,
      `trigger_type` varchar(8) NOT NULL,
      `start_time` bigint NOT NULL,
      `end_time` bigint DEFAULT NULL,
      `calendar_name` varchar(200) DEFAULT NULL,
      `misfire_instr` smallint DEFAULT NULL,
      `job_data` blob,
      PRIMARY KEY (`sched_name`,`trigger_name`,`trigger_group`),
      KEY `sched_name` (`sched_name`,`job_name`,`job_group`),
      CONSTRAINT `QRTZ_TRIGGERS_ibfk_1` FOREIGN KEY (`sched_name`, `job_name`, `job_group`) REFERENCES `QRTZ_JOB_DETAILS` (`sched_name`, `job_name`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    CREATE TABLE `QRTZ_BLOB_TRIGGERS` (
      `sched_name` varchar(120) NOT NULL,
      `trigger_name` varchar(200) NOT NULL,
      `trigger_group` varchar(200) NOT NULL,
      `blob_data` blob,
      PRIMARY KEY (`sched_name`,`trigger_name`,`trigger_group`),
      CONSTRAINT `QRTZ_BLOB_TRIGGERS_ibfk_1` FOREIGN KEY (`sched_name`, `trigger_name`, `trigger_group`) REFERENCES `QRTZ_TRIGGERS` (`sched_name`, `trigger_name`, `trigger_group`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    -- case 4
    create table studentCourse (
      ID                  integer         primary key auto_increment,
      studentID           integer             not null,
      courseID            integer             not null,
      CONSTRAINT cou_stu unique (courseID, studentID)
    );
    -- case 5
    CREATE TABLE IF NOT EXISTS "events" (
      "event_id" VARCHAR NOT NULL,
      "stream_id" VARCHAR NOT NULL,
      "tenant_id" VARCHAR NOT NULL,
      "base_version" INT NOT NULL,
      CONSTRAINT "log_pk" PRIMARY KEY ("event_id"),
      CONSTRAINT "unq_str_ten_bver" UNIQUE("stream_id", "tenant_id", "base_version")
    );
    -- case 6
    CREATE TABLE "#__contentitem_tag_map" (
      "type_alias" character varying(255) NOT NULL DEFAULT '',
      "core_content_id" integer NOT NULL,
      "content_item_id" integer NOT NULL,
      "tag_id" integer NOT NULL,
      "tag_date" timestamp without time zone DEFAULT '1970-01-01 00:00:00' NOT NULL,
     CONSTRAINT "uc_ItemnameTagid" UNIQUE ("type_alias", "content_item_id", "tag_id")
    );
    """


def get_uniq_case_on_create():
    return r"""-- case 0
    CREATE TABLE taxon (
       taxon_id     INT(10) UNSIGNED NOT NULL auto_increment,
       ncbi_taxon_id    INT(10),
       parent_taxon_id  INT(10) UNSIGNED,
       node_rank    VARCHAR(32),
       genetic_code TINYINT UNSIGNED,
       mito_genetic_code TINYINT UNSIGNED,
       left_value   INT(10) UNSIGNED,
       right_value  INT(10) UNSIGNED,
       PRIMARY KEY (taxon_id),
       UNIQUE (ncbi_taxon_id),
       UNIQUE (left_value),
       UNIQUE (right_value)
    ) TYPE=INNODB;
    -- case 1
    CREATE TABLE taxon_name (
      taxon_id     INT(10) UNSIGNED NOT NULL,
      name     VARCHAR(255) NOT NULL,
      name_class   VARCHAR(32) NOT NULL,
      UNIQUE (taxon_id,name,name_class)
    ) TYPE=INNODB;
    """


def get_uniq_key_case_on_create():
    return r"""-- case 0
    CREATE TABLE AA (
      pk varchar(3) NOT NULL DEFAULT '',  
      col_int_nokey int(11) DEFAULT NULL,  
      col_int_key int(11) DEFAULT NULL,  
      col_varchar_key varchar(52) DEFAULT NULL,  
      col_varchar_nokey varchar(52) DEFAULT NULL,  
      PRIMARY KEY (pk),  
      UNIQUE KEY col_varchar_key (col_varchar_key),  
      KEY col_int_key (col_int_key)
    );
    -- case 1
    CREATE TABLE IF NOT EXISTS `netscaler_services_vservers` (
      `sv_id` int(11) NOT NULL AUTO_INCREMENT,  
      `device_id` int(11) NOT NULL,  
      `vsvr_name` varchar(128) NOT NULL,  
      `svc_name` varchar(128) NOT NULL,  
      `service_weight` int(11) NOT NULL,  
      PRIMARY KEY (`sv_id`),  
      UNIQUE KEY `index` (`device_id`,`vsvr_name`,`svc_name`)
    ) ENGINE=InnoDB  DEFAULT CHARSET=utf8;
    """


def get_key_case_on_create():
    """
    Can't handle for now:
    ```SQL
    CREATE TABLE IF NOT EXISTS ts_kv (
      key varchar(255) NOT NULL,
    );
    ```
    """
    return r"""-- case 0
    CREATE TABLE `portal_buyer_bind_service` (
      `c_id` varchar(35) NOT NULL DEFAULT '',
      `c_userid` varchar(35) DEFAULT NULL COMMENT '用户id（shop_buyer中的c_uid）',
      `c_keyid` varchar(35) DEFAULT NULL COMMENT 'keyid',
      `c_texnum` varchar(30) DEFAULT NULL COMMENT '税号',
      `c_serviceid` varchar(30) DEFAULT NULL COMMENT '服务单位id',
      `crmNo` varchar(10) DEFAULT NULL,
      `departId` varchar(16) DEFAULT NULL COMMENT '部门id',
      `dt_adddate` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '添加时间',
      `dt_editdate` datetime DEFAULT NULL COMMENT '修改时间',
      `c_sysid` char(2) DEFAULT NULL COMMENT '数据来源',
      `c_syncstate` char(2) DEFAULT NULL COMMENT '是否已同步',
      `c_areacode` char(8) DEFAULT '' COMMENT '地区',
      `c_realname` varchar(35) DEFAULT NULL COMMENT '真实姓名',
      `c_faxnum` varchar(20) DEFAULT NULL COMMENT '传真号',
      `c_servicetaxnum` varchar(30) DEFAULT NULL COMMENT '服务税号',
      `c_taxauthorityid` varchar(35) DEFAULT NULL,
      `c_companyname` varchar(100) DEFAULT NULL COMMENT '公司名称',
      `c_address` varchar(100) DEFAULT NULL COMMENT '公司地址',
      PRIMARY KEY (`c_id`),
      KEY `c_texnum` (`c_texnum`) USING BTREE,
      KEY `c_serviceid` (`c_serviceid`) USING BTREE,
      KEY `c_keyidAndcrmNo` (`c_keyid`,`crmNo`) USING BTREE,
      KEY `c_userid` (`c_userid`) USING BTREE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='用户和服务单位关联中间表';
    -- case 1
    CREATE TABLE `i18n` (
      `id` int(10) NOT NULL AUTO_INCREMENT,
      `locale` varchar(6) COLLATE utf8_unicode_ci NOT NULL,
      `model` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
      `foreign_key` int(10) NOT NULL,
      `field` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
      `content` text COLLATE utf8_unicode_ci,
      PRIMARY KEY (`id`),
      KEY `locale` (`locale`),
      KEY `model` (`model`),
      KEY `row_id` (`foreign_key`),
      KEY `field` (`field`)
    ) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;"""


def get_data_compression_case_on_alter():
    return r"""-- case 0
    CREATE TABLE #result
        ( ID                        INT IDENTITY(1,1)   NOT NULL
        , DROP_INDEX_STATEMENT      NVARCHAR(4000)      NULL
        , CREATE_INDEX_STATEMENT    NVARCHAR(4000)      NULL
        , [index_columns]           NVARCHAR(4000)      NULL
        , included_columns          NVARCHAR(4000)      NULL
        , filter                    NVARCHAR(4000)      NULL
        , dbname                    SYSNAME             NULL
        , tableName                 SYSNAME             NULL
        , index_id                  INT                 NULL
        , partition_number          INT                 NULL
        , index_name                SYSNAME             NULL
        , index_type                SYSNAME             NULL
        , is_primary_key            VARCHAR(3)          NULL
        , is_unique                 VARCHAR(3)          NULL
        , is_disabled               VARCHAR(3)          NULL
        , row_count                 BIGINT              NULL
        , reserved_MB               DECIMAL(10,2)       NULL
        , size_MB                   DECIMAL(10,2)       NULL
        , fill_factor               TINYINT             NULL
        , user_seeks                BIGINT              NULL
        , user_scans                BIGINT              NULL
        , user_lookups              BIGINT              NULL
        , user_updates              BIGINT              NULL
        , filegroup_desc            SYSNAME             NULL
        , data_compression_desc     SYSNAME             NULL
    )"""


def get_add_constraint_pk_case_on_alter():
    return r"""-- case 0
    CREATE TABLE gtfs.frequencies (
        feed_index integer NOT NULL,
        trip_id text NOT NULL,
        start_time text NOT NULL,
        end_time text NOT NULL,
        headway_secs integer NOT NULL,
        exact_times integer,
        start_time_seconds integer,
        end_time_seconds integer,
        CONSTRAINT frequencies_end_time_check CHECK (((end_time)::interval = (end_time)::interval)),
        CONSTRAINT frequencies_start_time_check CHECK (((start_time)::interval = (start_time)::interval))
    );
    ALTER TABLE ONLY gtfs.frequencies
    ADD CONSTRAINT frequencies_pkey PRIMARY KEY (feed_index, trip_id, start_time);"""


def get_add_pk_case_on_alter():
    return r"""-- case 0
    CREATE TABLE `serveur` (
      `idserveur` varchar(255) NOT NULL,
      `nom` varchar(45) DEFAULT NULL,
      `IP` varchar(45) DEFAULT NULL,
      `DNS` varchar(45) DEFAULT NULL,
      `etat` varchar(255) DEFAULT NULL,
      `localisation` varchar(45) DEFAULT NULL,
      `passerelle` varchar(45) DEFAULT NULL,
      `dernieremodification` date DEFAULT NULL,
      `dureegarantie` varchar(45) DEFAULT NULL,
      `typeserveur` varchar(45) DEFAULT NULL,
      `avatar` varchar(255) DEFAULT NULL,
      `serveur dapplications_idserveur` varchar(255) NOT NULL,
      `serveur de partage_idserveur` varchar(255) NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

    ALTER TABLE `serveur`
      ADD PRIMARY KEY (`idserveur`,`serveur dapplications_idserveur`,`serveur de partage_idserveur`)
      NOT DEFERRABLE INITIALLY IMMEDIATE;"""


def get_add_constraint_fk_case_on_alter():
    return r"""-- case 0
    CREATE TABLE `books` (
      `id` bigint(20) NOT NULL,
      `is_nugas` tinyint(4) NOT NULL,
      `is_ngaji` tinyint(4) NOT NULL,
      `is_doabanguntidur` tinyint(4) NOT NULL,
      `is_doabelumtidur` tinyint(4) NOT NULL,
      `book_content` text NOT NULL,
      `is_subuh` tinyint(4) NOT NULL,
      `is_dzuhur` tinyint(4) NOT NULL,
      `is_azhar` tinyint(4) NOT NULL,
      `is_maghrib` tinyint(4) NOT NULL,
      `is_isya` tinyint(4) NOT NULL,
      `date` timestamp(1) NOT NULL DEFAULT current_timestamp(1) ON UPDATE current_timestamp(1),
      `bookisreviewed` tinyint(4) DEFAULT NULL,
      `Surah_id` bigint(20) NOT NULL,
      `Students_id` bigint(20) NOT NULL,
      `Students_Teacher_id` bigint(20) NOT NULL,
      `Students_Class_id` bigint(20) NOT NULL,
      `updated_at` timestamp NULL DEFAULT NULL,
      `created_at` timestamp NULL DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    CREATE TABLE `students` (
      `id` bigint(20) NOT NULL,
      `name` varchar(255) NOT NULL,
      `address` varchar(255) NOT NULL,
      `phone_number` varchar(100) NOT NULL,
      `number` varchar(255) NOT NULL,
      `username` varchar(255) NOT NULL,
      `password` varchar(255) NOT NULL,
      `Teacher_id` bigint(20) NOT NULL,
      `Class_id` bigint(20) NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    
    CREATE TABLE `surah` (
      `id` bigint(20) NOT NULL,
      `surah_name` varchar(255) NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

    ALTER TABLE `books`
      ADD CONSTRAINT `fk_Books_Students1` FOREIGN KEY (`Students_id`,`Students_Teacher_id`,`Students_Class_id`) REFERENCES `students` (`id`, `Teacher_id`, `Class_id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
      ADD CONSTRAINT `fk_Books_Surah1` FOREIGN KEY (`Surah_id`) REFERENCES `surah` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION;"""


def get_add_fk_case_on_alter():
    return r"""-- case 0
    CREATE TABLE Cliente(
    id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE,
    id_contacto INT(11) UNSIGNED NOT NULL,
    id_datos INT(11) UNSIGNED NOT NULL,
    nombre_comercial VARCHAR(255),
    razon_social VARCHAR(255),
    rfc VARCHAR(20),
    domiciio_fiscal VARCHAR(255),
    PRIMARY KEY (id)
    );
    CREATE TABLE Contacto(
    id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE,
    nombre_completo VARCHAR(255),
    correo_electronico VARCHAR(255),
    telefono VARCHAR(255),
    pagina_web VARCHAR(255),
    localidad VARCHAR(255),
    estado VARCHAR(255),
    PRIMARY KEY (id)
    );
    ALTER TABLE Cliente ADD FOREIGN KEY ("id_contacto") REFERENCES Contacto ("id");
    ALTER TABLE Cliente ADD FOREIGN KEY id_contacto_idxfk (id_contacto) REFERENCES Contacto (id);
    ALTER TABLE Cliente ADD FOREIGN KEY id_contacto_idxfk (`id_contacto`, `nombre_comercial`, `rfc`) REFERENCES Contacto ("id", "nombre_completo", "estado");"""


def get_add_uniq_key_case_on_alter():
    return r"""CREATE TABLE `webdb` (
      `latitude` varchar(100) NOT NULL,
      `longtitude` varchar(100) NOT NULL,
      `lokasi` varchar(100) NOT NULL,
      `tanggal` date NOT NULL,
      `deskripsi` varchar(100) NOT NULL,
      `gambar` varchar(255) DEFAULT NULL,
      `id` int(10) NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    ALTER TABLE `webdb`
      ADD PRIMARY KEY (`id`),
      ADD UNIQUE KEY `latitude` (`latitude`,`longtitude`,`lokasi`,`tanggal`,`deskripsi`);
    """


def get_add_uniq_idx_case_on_alter():
    return r"""create table llx_element_element
    (
      rowid             integer AUTO_INCREMENT PRIMARY KEY,
      sourceid          integer NOT NULL,
      sourcetype        varchar(12) NOT NULL,
      targetid          integer NOT NULL,
      targettype        varchar(12) NOT NULL
    ) ENGINE=innodb;

    ALTER TABLE llx_element_element
      ADD UNIQUE INDEX idx_element_element_idx1 (sourceid, sourcetype, targetid, targettype);"""


def get_add_constaint_unique_on_create():
    return r"""create table Folder (
            folderid int identity (1, 1) primary key,
            displayname nvarchar(100) not null,
            parent_folderid int null
        );

        go

        ALTER TABLE Folder
            ADD CONSTRAINT uniqueFolderName UNIQUE (parent_folderid, displayname);"""


def get_create_uniq_case_on_create():
    return """-- case 0
    CREATE TABLE acl_classes (
      id INT UNSIGNED IDENTITY NOT NULL, 
      class_type NVARCHAR(200) NOT NULL, 
      PRIMARY KEY (id));
    CREATE UNIQUE INDEX UNIQ_69DD750638A36066 ON acl_classes (class_type) WHERE class_type IS NOT NULL;
    -- case 1
    CREATE TABLE "APP"."IDXS" (
        "INDEX_ID" BIGINT NOT NULL, 
        "CREATE_TIME" INTEGER NOT NULL, 
        "DEFERRED_REBUILD" CHAR(1) NOT NULL, 
        "INDEX_HANDLER_CLASS" VARCHAR(4000), 
        "INDEX_NAME" VARCHAR(128), 
        "INDEX_TBL_ID" BIGINT, 
        "LAST_ACCESS_TIME" INTEGER NOT NULL, 
        "ORIG_TBL_ID" BIGINT, 
        "SD_ID" BIGINT
    );
    CREATE UNIQUE INDEX "APP"."UNIQUEINDEX" ON "APP"."IDXS" ("INDEX_NAME", "ORIG_TBL_ID");
    -- case 2
    CREATE TABLE acl_entries (
        id SERIAL NOT NULL, 
        class_id INT NOT NULL, 
        object_identity_id INT DEFAULT NULL, 
        security_identity_id INT NOT NULL, 
        field_name VARCHAR(50) DEFAULT NULL, 
        ace_order SMALLINT NOT NULL, 
        mask INT NOT NULL, 
        granting BOOLEAN NOT NULL, 
        granting_strategy VARCHAR(30) NOT NULL, 
        audit_success BOOLEAN NOT NULL, 
        audit_failure BOOLEAN NOT NULL, 
        PRIMARY KEY(id)
    );
    CREATE UNIQUE INDEX UNIQ_46C8B806EA000B103D9AB4A64DEF17BCE4289BF4 
        ON acl_entries (class_id, object_identity_id, field_name, ace_order);"""


class MyTestCase(unittest.TestCase):
    def test_something(self):
        self.assertEqual(True, False)  # add assertion here


if __name__ == '__main__':
    unittest.main()
