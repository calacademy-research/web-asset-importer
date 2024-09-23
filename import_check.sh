#!/bin/bash

cd "$(dirname "$0")"

if grep -q -A 20 -B 15 -e "Traceback" "$1" || grep -q -e "!DOCTYPE HTML" "$1"; then
  exit 1
else
  echo pass
  exit 0
fi

