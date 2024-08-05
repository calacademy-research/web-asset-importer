#!/bin/bash

image_password="test_password"
metadata_requirements_path="metadata_tools/requirements.txt"
python_path=$(pwd)
lockfile="/tmp/$(basename "$0").lock"

setup() {
    docker stop mariadb-specify
    docker rm mariadb-specify
    docker stop picbatch-mysql
    docker rm picbatch-mysql
}

cleanup() {
    rm -f "$lockfile"
    echo "Lockfile removed."
    docker stop mariadb-specify
    docker rm mariadb-specify
    docker stop picbatch-mysql
    docker rm picbatch-mysql
    echo "DELETE FROM images.images;" | docker exec -i mysql-images mysql -u root -p$image_password
    # cleaning up mounted server attachments folder
    find ../web-asset-server-ci/attachments -type f -delete
}
