#!/bin/bash
hive -e "
set hive.cli.print.header=true;
select * from test.account
" | sed 's/[\t]/^/g' > account.csv