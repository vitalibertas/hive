#!/bin/bash

# Pick the dates
START_DATE=`date --date="yesterday" "+%-Y-%0m-%0d"`
END_DATE=`date "+%-Y-%0m-%0d"`

# Run the queries
hive --hiveconf START_DATE=$START_DATE --hiveconf END_DATE=$END_DATE -f /home/user/hive/es_wordpress.sql
