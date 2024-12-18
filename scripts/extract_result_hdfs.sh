#!/usr/bin/env bash

if ! [[ "$PWD" = tmp ]]
then
  cd /tmp || exit 1
fi

hdfs dfs -cat /user/root/output/part-r-00000 > /tmp/result.txt
