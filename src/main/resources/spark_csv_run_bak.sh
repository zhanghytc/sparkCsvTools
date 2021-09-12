#!/bin/bash

work_dir=/data/spark

export SPARK_HOME=/www/allyes/tool/spark-2.1.0-bin-hadoop2.6
export PATH=$SPARK_HOME/bin:$PATH


check_res(){
res=$1
if [ $res -ne 0 ];then
    exit 1
fi
}

check_success_file(){
out_dir=$1
successfile=$out_dir/$db/$dt/_SUCCESS
hadoop fs -test -e $successfile
check_res $?

update_dt=`hadoop fs -ls $successfile | awk -F ' ' '{print $6" "$7}'`
check_res $?
}

jars=''

addJars() {
   local jar_dir=$1
   for jar in `ls $jar_dir`; do
   if [ -n "$jars" ]; then
    jars=$jars,$jar
     echo "$jars is not empty"
   else
    jars=${jar_dir}/$jar
   fi
   done
}
addJars ${work_dir}/bin
configs=${work_dir}/conf/awise-eng.conf

daily_job()
{
delete_out_dir ${app} ${alg}
echo "app-before:" ${app}
spark-submit \
   --name awise.eng    \
   --master yarn \
   --deploy-mode cluster \
   --conf spark.default.parallelism=100 \
   --jars $jars \
   --files $configs \
   --num-executors 10 \
   --driver-memory 3g \
   --executor-memory 7g \
   --executor-cores 1 \
   --class com.allyes.awise.eng.flow.AwiseFlowRunner ${work_dir}/bin/awiseeng-flow-1.0.0.jar $dt ${app} ${alg}
echo "" ${app}
check_success_file ${eng_success_path}
}
daily_job
