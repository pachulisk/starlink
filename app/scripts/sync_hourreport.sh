#!/bin/bash
curl -X 'POST' \
  'http://107.172.190.217:8000/sync_task_celery' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "table_name": "hourreport",
  "gwid": "97935833-c028-4f7b-ad5f-26f296cf935a",
  "column": "happendate"
}'


# */3 * * * * /usr/bin/bash -x /home/ubuntu/app/app/scripts/sync_hourreport.sh