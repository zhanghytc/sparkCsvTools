#!/bin/bash

work_dir=/data/softs/spark-csv-tools

#export SPARK_HOME=/data/softs/spark-2.4.4-bin-hadoop2.7
#export PATH=$SPARK_HOME/bin:$PATH


check_res(){
res=$1
if [ $res -ne 0 ];then
    exit 1
fi
}

jars=''

addJars() {
   local jar_dir=$1
   for jar in `ls $jar_dir`; do
   if [ -n "$jars" ]; then
    jars=$jars,${jar_dir}/$jar
   else
    jars=${jar_dir}/$jar
   fi
   done
}


out_dir=/data/janusgraph
local_path=/data/softs/spark-csv-tools/output

check_success_file(){
out_dir=$1
successfile=$out_dir/$db/$dt/_SUCCESS
hadoop fs -test -e $successfile
check_res $?

update_dt=`hadoop fs -ls $successfile | awk -F ' ' '{print $6" "$7}'`
check_res $?
}

addJars ${work_dir}/lib
echo "lib-jars:" $jars
configs=${work_dir}/application.conf

daily_job()
{
spark-submit \
--master yarn \
--deploy-mode cluster \
--class com.tairong.TrSparkCsvTools \
--jars $jars \
--files application.conf \
--conf spark.driver.extraClassPath=./ \
--conf spark.executor.extraClassPath=./ \
sparkcsv-1.0-SNAPSHOT.jar \
-c application.conf \
-h
}

daily_job

# TODO 增加删除local_path 中csv文件
rm -rf $local_path/*.csv

hdfs dfs -get $out_dir/*.csv $local_path


#hdfs dfs -get 

