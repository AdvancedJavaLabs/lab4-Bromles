#!/usr/bin/env bash

cd ./tmp || exit 1

./setup_hdfs.sh
./load_data_hdfs.sh
./launch_job_hdfs.sh
./extract_result_hdfs.sh
./cleanup_hdfs.sh
