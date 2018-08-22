#!/bin/bash
# FileName:      spark_submit_script.sh
# Description:   Print all the date during the two days you inpute.

if [[ $# -le 2 || $# -gt 3 ]]; then
	echo "Usage: $0 2018-01-01 2018-01-18 or $0 2018/01/01 2018/01/18 or $0 20180101 20180118 [-][/][.]['']"
	exit 1
fi

START_DAY=$(date -d "$1" +%s)
END_DAY=$(date -d "$2" +%s)
# The spliter bettwen year, month and day.
SPLITER=${3}

# Declare an array to store all the date during the two days you inpute.
declare -a DATE_ARRAY

function getDateRange 
{
	if [[ $# -ne 3 ]]; then
		echo "Usage: getDateRange 2018-01-01 2018-01-18 or getDateRange 2018/01/01 2018/01/18 or getDateRange 20180101 20180118 [-][/][.]['']"
		exit 1
	fi
	
	START_DAY_TMP=${1}
	END_DAY_TMP=${2}
	SPLITER_TMP=${3}
	I_DATE_ARRAY_INDX=0
	
	while (( "${START_DAY_TMP}" <= "${END_DAY_TMP}" )); do
		cur_day=$(date -d @${START_DAY_TMP} +"%Y${SPLITER_TMP}%m${SPLITER_TMP}%d")
		DATE_ARRAY[${I_DATE_ARRAY_INDX}]=${cur_day}
		
		START_DAY_TMP=$((${START_DAY_TMP}+86400))
		((I_DATE_ARRAY_INDX++))
		
	done
}

getDateRange "${START_DAY}" "${END_DAY}" "${SPLITER}"

. /etc/profile.d/custom.sh 

for SINGLE_DAY in ${DATE_ARRAY[@]};
do
	echo `spark-submit --master yarn --deploy-mode client --packages "mysql:mysql-connector-java:6.0.6" --num-executors 4 --executor-memory 4G --class "com.cm.data.datasync.ReadLogDb2HDFS" /home/ubuntu/target/data_analysis-1.0.1.jar order_log_${SINGLE_DAY} 4`
done

exit 0
