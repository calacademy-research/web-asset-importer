#!/bin/bash

image_password="test_password"
metadata_requirements_path="metadata_tools/requirements.txt"
python_path=$(pwd)
lockfile="/tmp/importer_jenkins.lock"

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
    find ../stable_cas-server/attachments -type f -delete
    rm -r venv
    # removing tmp pip files
    rm -rf /tmp/pip-*
}


replace_https_with_http() {
    local file="$1"
    if [[ -f "$file" ]]; then
        # Replace 'https' with 'http' in place
        sed -i.bak 's/https/http/g' "$file"
        rm -f "${file}.bak"
        echo "Replaced 'https' with 'http' in $file."
    else
        echo "File $file not found."
    fi
}

convert_to_http() {
  files=(
      "tests/test_server.py"
      "views/web_asset_store.xml"
  )
  # Loop through each file and run the replacement function
  for file in "${files[@]}"; do
      replace_https_with_http "$file"
  done
}
