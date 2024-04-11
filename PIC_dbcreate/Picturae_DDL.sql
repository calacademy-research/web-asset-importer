--ddl to run inside docker container / or vm for creation of new picturae upload batch database


CREATE TABLE IF NOT EXISTS picturaetaxa_added (newtaxID INTEGER PRIMARY KEY AUTO_INCREMENT,
                                                TimestampCreated VARCHAR(128) NOT NULL ,
                                                TimestampModified VARCHAR(128) NOT NULL ,
                                                batch_MD5 VARCHAR(128),
                                                CatalogNumber VARCHAR(20) NOT NULL ,
                                                fullname VARCHAR(512),
                                                name varchar(512),
                                                family varchar(512),
                                                hybrid BIT,
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
-- DROP TABLE picturaetaxa_added;
-- DROP TABLE picturae_batch;


