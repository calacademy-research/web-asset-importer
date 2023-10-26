#!/bin/bash
touch casbotany_lite.db
python casbotany_lite_creator.py
# gets data dump of taxon tree
mysqldump -u [username] -p -h [hostname] [database] taxon > taxon_dump.sql
# will need to insert steps to clean up taxon_dump.sql
python convert_mysql_sqlite.py -f taxon_dump.sql

sqlite3 casbotany_lite.db < taxon_dump.sql


