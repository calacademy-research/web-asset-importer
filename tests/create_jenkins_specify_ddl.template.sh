#!/bin/bash
username="username"
password="password"

# Databases and tables
databases=("casich" "casiz" "casbotany")
tables=("agent" "geography" "taxon" "agent")

cleanup (){
  rm -f all_ddl.sql
  rm -f cleaned_db.sql
}

trap cleanup EXIT

# Dump the databases
for db in "${databases[@]}"; do
    mysqldump -h ntobiko -u $username -p$password --no-data --routines --triggers --databases $db >> all_ddl.sql
done

# Dump the tables for each database
for db in "${databases[@]}"; do
    echo "USE \`$db\`;" >> all_ddl.sql

    for table in "${tables[@]}"; do
      mysqldump -h ntobiko -u $username -p$password $db $table >> all_ddl.sql
    done
    # adding reduced collectionobject table
    mysqldump -h ntobiko -u $username -p$password $db collectionobject --where="CollectionObjectID <= 10" >> all_ddl.sql
done

# Remove constraint and definer lines
sed -e '/^  CONSTRAINT/d' -e "s/ DEFINER=\`$username\`@\`%\`//g" all_ddl.sql > cleaned_db.sql

# removing trailing commas after column lists
sed 'x;1d;G;/;$/s/,\n)/\n)/;$!s/\n.*//' cleaned_db.sql > specify_jenkins_ddl.sql

# scping to jenkins environment
scp specify_jenkins_ddl.sql ibss-alt@10.2.22.2:/ibss-alt/jenkins_data/workspace/jenkins_ddls/
