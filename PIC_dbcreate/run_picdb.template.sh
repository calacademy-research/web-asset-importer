#!/bin/bash
password="password"
docker run --name picbatch-mysql -e MYSQL_ROOT_PASSWORD=$password -e MYSQL_DATABASE=picbatch -d -p 3309:3306 -v $(pwd)/picdb_data:/var/lib/mysql mysql:8

cp ./Picturae_DDL.sql $(pwd)/picdb_data/Picturae_DDL.sql


# Function to check if MySQL is ready
check_mysql() {
  docker exec picbatch-mysql mysqladmin ping -u root -p$password --host=127.0.0.1 --silent
}

# Maximum timeout duration (in seconds)
TIMEOUT=60
# Interval between checks (in seconds)
INTERVAL=2
# Start time
start_time=$(date +%s)

# Wait for the MySQL service to initialize
while true; do
  if check_mysql; then
    echo "MySQL is up and running."
    break
  fi

  current_time=$(date +%s)
  elapsed=$((current_time - start_time))

  if [ $elapsed -ge $TIMEOUT ]; then
    echo "MySQL initialization timed out after $TIMEOUT seconds."
    exit 1
  fi

  echo "Waiting for MySQL to initialize... (elapsed time: $elapsed seconds)"
  sleep $INTERVAL
done

# run DDL
docker exec -i picbatch-mysql mysql -u root -p$password --host=127.0.0.1 picbatch < ./Picturae_DDL.sql