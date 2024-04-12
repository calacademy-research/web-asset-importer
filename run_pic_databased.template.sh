#!/bin/bash

dates=/path/to/dates.txt
lockfile="path/to/lockfile"

if [ -e ${lockfile} ] && kill -0 `cat ${lockfile}`; then
  echo "Import already running, stop."
  exit
fi

# Create the lockfile
echo $$ > ${lockfile}

trap 'rm -f "${lockfile}"; exit' INT TERM EXIT

# Read from dates.txt and process each date
while IFS= read -r date; do
  echo "Processing date: $date"
  python3 client_tools.py -d "$date" -e True Botany_PIC import
done < "$dates"

# Remove the lockfile
rm -f ${lockfile}
