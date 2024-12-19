#!/usr/bin/env bash

if ! [[ "$PWD" = tmp ]]
then
  cd /tmp || exit 1
fi

./setup_hdfs.sh
./load_data_hdfs.sh
./launch_job_hdfs.sh
./extract_result_hdfs.sh
./cleanup_hdfs.sh
