# ddl to run inside docker container / or vm for creation of new picturae upload batch database


CREATE TABLE IF NOT EXISTS picturaetaxa_added (newtaxID INTEGER PRIMARY KEY AUTO_INCREMENT,
                                                TimestampCreated VARCHAR(128) NOT NULL ,
                                                TimestampModified VARCHAR(128) NOT NULL ,
                                                batch_MD5 VARCHAR(128),
                                                fullname VARCHAR(512),
                                                name varchar(512),
                                                family varchar(512),
                                                hybrid BOOLEAN,
                                                CreatedByAgentID VARCHAR(128),
                                                ModifiedByAgentID VARCHAR(128));



CREATE TABLE IF NOT EXISTS  picturae_batch (batchID INTEGER PRIMARY KEY AUTO_INCREMENT,
                                             batch_MD5 VARCHAR(128) NOT NULL ,
                                             TimestampCreated VARCHAR(128) NOT NULL ,
                                             TimestampModified VARCHAR(128) NOT NULL ,
                                             StartTimeStamp TEXT NOT NULL ,
                                             EndTimeStamp TEXT NOT NULL,
                                             batch_size INTEGER,
                                             CreatedByAgentID VARCHAR(128),
                                             ModifiedByAgentID VARCHAR(128));


CREATE TABLE IF NOT EXISTS `all_data_alembo` (`recordID` INTEGER PRIMARY KEY AUTO_INCREMENT,
                                              `CsvBatch` VARCHAR(128) NOT NULL,
                                              `Barcode` VARCHAR(32) NOT NULL,
                                              `FolderBarcode` VARCHAR(32) NOT NULL,
                                              `ParentBarcode` VARCHAR(32) DEFAULT NULL,
                                              `jpg_path` VARCHAR(32) NOT NULL,
                                              `Handwritten` BOOLEAN DEFAULT NULL,
                                              `Family` VARCHAR(128) NOT NULL,
                                              `Genus` VARCHAR(128) DEFAULT NULL,
                                              `Species` VARCHAR(128) DEFAULT NULL,
                                              `Qualifier` VARCHAR(32) DEFAULT NULL,
                                              `RANK_1` VARCHAR(32) DEFAULT NULL,
                                              `EPITHET_1` VARCHAR(128) DEFAULT NULL,
                                              `RANK_2` VARCHAR(32) DEFAULT NULL,
                                              `EPITHET_2` VARCHAR(128) DEFAULT NULL,
                                              `Hybrid` BOOLEAN DEFAULT NULL,
                                              `cover_notes` VARCHAR(128) DEFAULT NULL,
                                              `sheet_notes` VARCHAR(128) DEFAULT NULL,
                                              `CollectorNumber` VARCHAR(32) DEFAULT NULL,
                                              `CollectorID_1` VARCHAR(32) DEFAULT NULL,
                                              `CollectorID_2` VARCHAR(32) DEFAULT NULL,
                                              `CollectorID_3` VARCHAR(32) DEFAULT NULL,
                                              `CollectorID_4` VARCHAR(32) DEFAULT NULL,
                                              `CollectorID_5` VARCHAR(32) DEFAULT NULL,
                                              `Country` VARCHAR(128) DEFAULT NULL,
                                              `State` VARCHAR(128) DEFAULT NULL,
                                              `County` VARCHAR(128) DEFAULT NULL,
                                              `Locality` VARCHAR(256) DEFAULT NULL,
                                              `Verbatim_Date` VARCHAR(128) DEFAULT NULL,
                                              `start_month` INTEGER DEFAULT NULL,
                                              `start_day` INTEGER DEFAULT NULL,
                                              `start_year` INTEGER DEFAULT NULL,
                                              `end_month` INTEGER DEFAULT NULL,
                                              `end_day` INTEGER DEFAULT NULL,
                                              `end_year` INTEGER DEFAULT NULL,
                                              `duplicate` BOOLEAN DEFAULT NULL,
                                              `in_expedition` BOOLEAN DEFAULT NULL,
                                              `coords_tr` BOOLEAN DEFAULT NULL,
                                              `acc_herb_tr` BOOLEAN DEFAULT NULL,
                                              `hab_spec_tr` BOOLEAN DEFAULT NULL,
                                              `complete` BOOLEAN DEFAULT NULL ) ;
# DROP TABLE picturaetaxa_added;
# DROP TABLE picturae_batch;


