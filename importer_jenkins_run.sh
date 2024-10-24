#!/bin/bash
# Run botany import script and check its exit status
# Ensure no recursive sourcing


source ./importer_jenkins_config.sh

trap cleanup EXIT

./botany_import.sh
if [ $? -eq 0 ]; then
  echo "botany import successful"
else
  echo "botany import failed"
  exit 1
fi

# inserting SU catalog number to casich
echo '''UPDATE casich.collectionobject SET CatalogNumber="CAS-SU 000006" WHERE CatalogNumber="CAS-ICH000006";''' | \
docker exec -i mariadb-specify mariadb -u root -ppassword

# Run ichthyology import script and check its exit status
./ich_import.sh
if [ $? -eq 0 ]; then
  echo "ich import successful"
else
  echo "ich_import.sh failed"
  exit 1
fi

# Run iz import script and check its exit status
#./iz_import.sh
#if [ $? -eq 0 ]; then
#  echo "iz import successful"
#else
#  echo "iz import failed"
#  exit 1
#fi

##### Picturae csv import test

