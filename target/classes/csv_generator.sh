#!/bin/bash
export HIVE_HOME=/data/softs/apache-hive-2.3.8-bin/bin
export PATH=$HIVE_HOME/bin:$PATH
# 修改点
database=test
tables=("account" "customer" "related_person")

# TODO 遍历每一个table
function csv_generator(){
tables=$1
for table in ${tables[*]}; do
   hive -e "
   set hive.cli.print.header=true;
   select * from $database.$table
   " | sed 's/[\t]/^/g' > $table.csv
done
}

csv_generator $tables