#!/bin/bash
# mysqldump -u [username] -p -h [hostname] [database_name] taxon > taxon_dump.sql

mysqldump -u botanist -p -h ntobiko casbotany taxon > taxon_dump.sql

echo -n 'YourASCI' | xxd -ps

# sed -i 's/_binary ''/1/g' taxon_dump.sql  # Replace _binary '' with 1
# enter password
# in taxon_dump.sql
# delete : LOCK TABLES `taxon` WRITE;
# delete:  ENGINE=InnoDB AUTO_INCREMENT=259815 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci
# change:  Auto Increment to INTEGER PRIMARY KEY
# change: int(11) to INTEGER
# change: bit(1) to INTEGER
sqlite3 casbotany_lite.db < taxon_dump.sql