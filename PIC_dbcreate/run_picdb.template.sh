#!/bin/bash
docker run --name picbatch-mysql -e MYSQL_ROOT_PASSWORD=password -e MYSQL_DATABASE=picbatch -d -p 3309:3306 -v $(pwd)/picdb_data:/var/lib/mysql mysql:8

cp ./Picturae_DDL.sql $(pwd)/picdb_data/Picturae_DDL.sql

docker exec -i picbatch-mysql mysql -u root -ppassword picbatch < ./Picturae_DDL.sql