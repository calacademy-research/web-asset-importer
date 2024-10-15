#!/bin/bash

# Variables
DB_NAME="picbatch"
DB_USER="root"
DB_PASSWORD="your_password"  # Replace with your actual password
DB_PORT="3309"
BACKUP_DIR="/path/to/picbatch_backups"  # Replace with your actual backup directory
LOG_FILE="/path/to/log.txt"  # output log file

# make backup directory if not already existing
mkdir -p $BACKUP_DIR

# Get the current date
CURRENT_DATE=$(date +"%Y-%m-%d")

# Define the backup file name
BACKUP_FILE="$BACKUP_DIR/${DB_NAME}_backup_$CURRENT_DATE.sql"

# Perform the mysqldump
mysqldump -h 127.0.0.1 -u$DB_USER -p$DB_PASSWORD -P$DB_PORT $DB_NAME > $BACKUP_FILE

# Check if mysqldump was successful
if [ $? -eq 0 ]; then
    # Log backup completed successfully
    echo "Backup completed successfully: $CURRENT_DATE" >> $LOG_FILE
else
    # Log backup failed
    echo "Backup failed: $CURRENT_DATE" >> $LOG_FILE
fi

# Delete backup files older than 30 days
find $BACKUP_DIR -name "*.sql" -type f -mtime +30 -exec rm -f {} \;

# Log script finished
echo "Backup script finished: $CURRENT_DATE" >> $LOG_FILE

echo "Backup and cleanup complete."
