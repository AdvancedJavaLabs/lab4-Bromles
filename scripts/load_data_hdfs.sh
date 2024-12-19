#!/usr/bin/env bash

if ! [[ "$PWD" = "tmp" ]]
then
  cd /tmp || exit 1
fi

hdfs dfs -put -f ./*.csv /user/root/input
