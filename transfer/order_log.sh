#!/bin/bash

if [[ $# -le 2 || $# -gt 3 ]]; then
	echo "Usage: $0 2018-01-01 2018-01-18 or $0 2018/01/01 2018/01/18 or $0 20180101 20180118 [-][/][.]['']"
	exit 1
fi

START_DAY=$(date -d "$1" +%s)
END_DAY=$(date -d "$2" +%s)
# The spliter bettwen year, month and day.
SPLITER=''

# Declare an array to store all the date during the two days you inpute.
declare -a DATE_ARRAY

function getDateRange(){
	if [[ $# -ne 3 ]]; then
		echo "Usage: getDateRange 2018-01-01 2018-01-18 or getDateRange 2018/01/01 2018/01/18 or getDateRange 20180101 20180118 [-][/][.]['']"
		exit 1
	fi
	
	START_DAY_TMP=${1}
	END_DAY_TMP=${2}
	SPLITER_TMP=''
	I_DATE_ARRAY_INDX=0
	
	while (( "${START_DAY_TMP}" <= "${END_DAY_TMP}" )); do
		cur_day=$(date -d @${START_DAY_TMP} +"%Y${SPLITER_TMP}%m${SPLITER_TMP}%d")
		DATE_ARRAY[${I_DATE_ARRAY_INDX}]=${cur_day}
		
		START_DAY_TMP=$((${START_DAY_TMP}+86400))
		((I_DATE_ARRAY_INDX++))
		
	done
}

getDateRange "${START_DAY}" "${END_DAY}" "${SPLITER}"

kinit -kt chen.keytab chenweidong@HADOOP.COM

hostname="gz-cdbrg-nupxmf67.sql.tencentcdb.com:62382"
db_username="cmoutbigdata"
db_password="bd@#782018"
db_name="pay_chaomeng_log"
conn_str="jdbc:mysql://${hostname}/${db_name}?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&serverTimezone=Asia/Shanghai&useSSL=true&dontTrackOpenResources=true&defaultFetchSize=10000&useCursorFetch=true"
db_table=${3}
d20180420=`date -d "20180420" +"%Y%m%d"`

for SINGLE_DAY in ${DATE_ARRAY[@]};

	do
		d2=`date -d "${SINGLE_DAY} 2 day ago" +"%Y%m%d"`
		
		target_dir="hdfs://master.prodcdh.com:8020/tmp/cm_ods_mask/${db_table}_${SINGLE_DAY}"
		
		hdfs dfs -rm -r /tmp/cm_ods_mask/${db_table}_${SINGLE_DAY}
		
		sqoop import -Dorg.apache.sqoop.splitter.allow_text_splitter=true	\
		--connect ${conn_str} \
		--username ${db_username} \
		--password ${db_password} \
		--query "SELECT	* FROM ${db_table}_${SINGLE_DAY} WHERE \$CONDITIONS"	\
		--split-by id	\
		-m 2	\
		--hive-table cm_ods_mask.${db_table}_delta	\
		--hive-drop-import-delims	\
		--hive-partition-key yyMMdd	\
		--hive-partition-value ${SINGLE_DAY}    \
		--target-dir "${target_dir}" 	\
		--compress	\
		--compression-codec org.apache.hadoop.io.compress.SnappyCodec	\
		--hive-overwrite         \
		--null-string '\\N'      \
		--null-non-string '\\N'  \
		--hive-import
		
		if [ ${SINGLE_DAY} -le ${d20180420} ]; then
			hive -S -v -e "
			CREATE TABLE IF NOT EXISTS cm_ods_safe.${db_table} LIKE cm_ods_mask.${db_table}_delta;
			
			ALTER TABLE cm_ods_safe.${db_table} SET FILEFORMAT ORCFile;
			
			CREATE TABLE IF NOT EXISTS cm_ods_mask.${db_table} LIKE cm_ods_safe.${db_table};

			INSERT OVERWRITE TABLE cm_ods_safe.${db_table} PARTITION (yyMMdd=${SINGLE_DAY}) SELECT id,uid,pay_price,obtain_price,rebate_price,award_price,pay_status,order_status,type,createtime,finishtime,shelltime,transaction_id,out_trade_no,last_agent,paycode,openid,pay_openid,cashier_id,code,auth_id,sub_auth_id,ms_sub_id,limit_status,auth_status,user_type,attach,t1_status,user_ip,bank_type,bank_status,shop_status,re_status,user_pay,query_status,mch_code,ori_id,pay_type,fail_reason,unionid,ua,third_info,pay_costtime,query_costtime,total_costtime,shop_sub_id,fc_status,device_id,'date',channel_discount,platform_discount,close_status,close_num FROM cm_ods_mask.${db_table}_delta WHERE yyMMdd=${SINGLE_DAY};
			
			INSERT OVERWRITE TABLE cm_ods_mask.${db_table} PARTITION (yyMMdd=${SINGLE_DAY}) SELECT id,uid,pay_price,obtain_price,rebate_price,award_price,pay_status,order_status,type,createtime,finishtime,shelltime,transaction_id,out_trade_no,last_agent,paycode,openid,pay_openid,cashier_id,code,auth_id,sub_auth_id,ms_sub_id,limit_status,auth_status,user_type,attach,t1_status,user_ip,bank_type,bank_status,shop_status,re_status,user_pay,query_status,mch_code,ori_id,pay_type,fail_reason,unionid,ua,third_info,pay_costtime,query_costtime,total_costtime,shop_sub_id,fc_status,device_id,'date',channel_discount,platform_discount,close_status,close_num FROM cm_ods_mask.${db_table}_delta WHERE yyMMdd=${SINGLE_DAY};
			
			ALTER TABLE cm_ods_mask.${db_table}_delta DROP PARTITION (yyMMdd=${d2});
			"
		else
			hive -S -v -e "
			CREATE TABLE IF NOT EXISTS cm_ods_safe.${db_table} LIKE cm_ods_mask.${db_table}_delta;
			
			ALTER TABLE cm_ods_safe.${db_table} SET FILEFORMAT ORCFile;
			
			CREATE TABLE IF NOT EXISTS cm_ods_mask.${db_table} LIKE cm_ods_safe.${db_table};

			INSERT OVERWRITE TABLE cm_ods_safe.${db_table} PARTITION (yyMMdd=${SINGLE_DAY}) SELECT id,uid,pay_price,obtain_price,rebate_price,red_price,award_price,pay_status,order_status,type,createtime,finishtime,shelltime,transaction_id,out_trade_no,last_agent,paycode,openid,pay_openid,cashier_id,code,auth_id,sub_auth_id,ms_sub_id,limit_status,auth_status,user_type,attach,t1_status,user_ip,bank_type,bank_status,shop_status,re_status,user_pay,query_status,mch_code,ori_id,pay_type,fail_reason,unionid,ua,third_info,pay_costtime,query_costtime,total_costtime,shop_sub_id,fc_status,device_id,'date',channel_discount,platform_discount,close_status,close_num FROM cm_ods_mask.${db_table}_delta WHERE yyMMdd=${SINGLE_DAY};
			
			INSERT OVERWRITE TABLE cm_ods_mask.${db_table} PARTITION (yyMMdd=${SINGLE_DAY}) SELECT id,uid,pay_price,obtain_price,rebate_price,red_price,award_price,pay_status,order_status,type,createtime,finishtime,shelltime,transaction_id,out_trade_no,last_agent,paycode,openid,pay_openid,cashier_id,code,auth_id,sub_auth_id,ms_sub_id,limit_status,auth_status,user_type,attach,t1_status,user_ip,bank_type,bank_status,shop_status,re_status,user_pay,query_status,mch_code,ori_id,pay_type,fail_reason,unionid,ua,third_info,pay_costtime,query_costtime,total_costtime,shop_sub_id,fc_status,device_id,'date',channel_discount,platform_discount,close_status,close_num FROM cm_ods_mask.${db_table}_delta WHERE yyMMdd=${SINGLE_DAY};
			
			ALTER TABLE cm_ods_mask.${db_table}_delta DROP PARTITION (yyMMdd=${d2});
			"
		fi
		
	done

exit 0
