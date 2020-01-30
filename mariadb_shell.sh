#!/bin/bash
if [ -z "$PASS" ]
then
  echo "$0: please specify MariaDB password via PASS=..."
  exit 1
fi
mysql -h127.0.0.1 -P13306 -p"${PASS}"
