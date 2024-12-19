#!/usr/bin/env bash

if [[ -n $1 ]]
then
  reducerCount=$1
else
  reducerCount=1
fi

if [[ -n $2 ]]
then
  datablockSizeKb=$2
else
  datablockSizeKb=16384
fi

if ! [[ "$PWD" = tmp ]]
then
  cd /tmp || exit 1
fi

hadoop jar lab4-parcalc-bromles-0.0.1.jar org.itmo.lab4.SalesAnalyzer input output "$reducerCount" "$datablockSizeKb"
