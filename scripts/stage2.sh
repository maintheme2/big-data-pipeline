#!/bin/bash

hdfs dfs -mkdir -p project/warehouse/avsc
hdfs dfs -put output/accidents.avsc project/warehouse/avsc

password=$(head -n 1 secrets/.hive.pass)

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team26 -p $password -f sql/db.hql > output/hive_results.txt 
# 2> /dev/null

# q1
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team26 -p $password -f sql/q1.hql
echo "state,avg_severity" > output/q1.csv 
hdfs dfs -cat project/output/q1/* >> output/q1.csv

# q2
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team26 -p $password -f sql/q2.hql
echo "city,accidents_number" > output/q2.csv 
hdfs dfs -cat project/output/q2/* >> output/q2.csv