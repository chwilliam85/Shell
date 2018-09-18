#!/bin/bash
#支付订单数据每日定时同步

kinit -kt chen.keytab chenweidong@HADOOP.COM

hostname="gz-cdbrg-nupxmf67.sql.tencentcdb.com:62382"
db_username="cmoutbigdata"
db_password="bd@#782018"
db_name="pay_chaomeng_log"
conn_str="jdbc:mysql://${hostname}/${db_name}?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&serverTimezone=Asia/Shanghai&useSSL=true&dontTrackOpenResources=true&defaultFetchSize=10000&useCursorFetch=true"
db_table=${1}
SINGLE_DAY=${2}


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

hive -S -v -e "
CREATE TABLE IF NOT EXISTS cm_ods_safe.${db_table} LIKE cm_ods_mask.${db_table}_delta;

ALTER TABLE cm_ods_safe.${db_table} SET FILEFORMAT ORCFile;

CREATE TABLE IF NOT EXISTS cm_ods_mask.${db_table} LIKE cm_ods_safe.${db_table};

INSERT OVERWRITE TABLE cm_ods_safe.${db_table} PARTITION (yyMMdd=${SINGLE_DAY}) SELECT id,uid,pay_price,obtain_price,rebate_price,red_price,award_price,pay_status,order_status,type,createtime,finishtime,shelltime,transaction_id,out_trade_no,last_agent,paycode,openid,pay_openid,cashier_id,code,auth_id,sub_auth_id,ms_sub_id,limit_status,auth_status,user_type,attach,t1_status,user_ip,bank_type,bank_status,shop_status,re_status,user_pay,query_status,mch_code,ori_id,pay_type,fail_reason,unionid,ua,third_info,pay_costtime,query_costtime,total_costtime,shop_sub_id,fc_status,device_id,'date',channel_discount,platform_discount,close_status,close_num FROM cm_ods_mask.${db_table}_delta WHERE yyMMdd=${SINGLE_DAY};

INSERT OVERWRITE TABLE cm_ods_mask.${db_table} PARTITION (yyMMdd=${SINGLE_DAY}) SELECT id,uid,pay_price,obtain_price,rebate_price,red_price,award_price,pay_status,order_status,type,createtime,finishtime,shelltime,transaction_id,out_trade_no,last_agent,paycode,openid,pay_openid,cashier_id,code,auth_id,sub_auth_id,ms_sub_id,limit_status,auth_status,user_type,attach,t1_status,user_ip,bank_type,bank_status,shop_status,re_status,user_pay,query_status,mch_code,ori_id,pay_type,fail_reason,unionid,ua,third_info,pay_costtime,query_costtime,total_costtime,shop_sub_id,fc_status,device_id,'date',channel_discount,platform_discount,close_status,close_num FROM cm_ods_mask.${db_table}_delta WHERE yyMMdd=${SINGLE_DAY};

ALTER TABLE cm_ods_mask.${db_table}_delta DROP PARTITION (yyMMdd=${d2});
"
