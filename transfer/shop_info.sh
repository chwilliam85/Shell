#!/bin/bash

kinit -kt chen.keytab chenweidong@HADOOP.COM

hostname="172.18.205.119:63950"
db_username="cmoutbigdata"
db_password="bd@#782018"
db_name="pay_chaomeng"
conn_str="jdbc:mysql://${hostname}/${db_name}?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&serverTimezone=Asia/Shanghai&useSSL=true&dontTrackOpenResources=true&defaultFetchSize=10000&useCursorFetch=true"

db_table=${1}
d1=`date -d "1 day ago" +"%Y%m%d"`
d2=`date -d "2 day ago" +"%Y%m%d"`

target_dir="hdfs://master.prodcdh.com:8020/tmp/cm_ods_mask/${db_table}"

hdfs dfs -rm -r /tmp/cm_ods_mask/${db_table}

sqoop import	\
--connect ${conn_str} \
--username ${db_username} \
--password ${db_password} \
--query "SELECT	* FROM ${db_table} WHERE \$CONDITIONS"	\
-m 1 \
--hive-table cm_ods_mask.${db_table}_delta	\
--hive-drop-import-delims	\
--hive-partition-key yyMMdd	\
--hive-partition-value ${d1}	\
--target-dir "${target_dir}"	\
--compress	\
--compression-codec org.apache.hadoop.io.compress.SnappyCodec	\
--hive-overwrite         \
--null-string '\\N'      \
--null-non-string '\\N'  \
--hive-import

hive -S -v -e "
CREATE TABLE IF NOT EXISTS cm_ods_safe.${db_table} AS SELECT uid,service_code,shop_name,provinces,address,realname,phone,bank_kh_name,bank_card_id,khh,khh_name,bank_code,rate,pay_limit,createtime,auth_type,auth_status,location,location_name,permit_num,wx_id,alipay_id,fc_rate1,yb_fc_rate1,fc_rate2,yb_fc_rate2,fc_rate3,yb_fc_rate3,area_code,pay_num,complain_status,is_sign,voicebox_num,usevoicebox_num,need_jzb,shop_time,corporate_account,is_yp FROM cm_ods_mask.${db_table}_delta WHERE 1=2;

ALTER TABLE cm_ods_safe.${db_table} SET FILEFORMAT ORCFile;

INSERT OVERWRITE TABLE cm_ods_safe.${db_table} SELECT uid,service_code,shop_name,provinces,address,realname,case when (length(phone) <= 4) then replace(phone,substr(phone,4,4),'*') else phone end as phone,case when (length(encode(bank_kh_name,'UTF-8')) <= 9) then replace(bank_kh_name,substr(bank_kh_name,2,1),'*') when (length(encode(bank_kh_name,'UTF-8')) <= 12) then replace(bank_kh_name,substr(bank_kh_name,2,2),repeat('*',2)) when (length(encode(bank_kh_name,'UTF-8')) <= 24) then replace(bank_kh_name,substr(bank_kh_name,2,length(bank_kh_name)-2),repeat('*',length(bank_kh_name))) else bank_kh_name end as bank_kh_name,case when (length(bank_card_id) > 0) then sha2(bank_card_id,256) else bank_card_id end as bank_card_id,khh,khh_name,bank_code,rate,pay_limit,createtime,auth_type,auth_status,location,location_name,permit_num,wx_id,alipay_id,fc_rate1,yb_fc_rate1,fc_rate2,yb_fc_rate2,fc_rate3,yb_fc_rate3,area_code,pay_num,complain_status,is_sign,voicebox_num,usevoicebox_num,need_jzb,shop_time,corporate_account,is_yp FROM cm_ods_mask.${db_table}_delta;

CREATE TABLE IF NOT EXISTS cm_ods_mask.${db_table} LIKE cm_ods_safe.${db_table};

INSERT OVERWRITE TABLE cm_ods_mask.${db_table} SELECT uid,service_code,shop_name,provinces,address,realname,phone,bank_kh_name,bank_card_id,khh,khh_name,bank_code,rate,pay_limit,createtime,auth_type,auth_status,location,location_name,permit_num,wx_id,alipay_id,fc_rate1,yb_fc_rate1,fc_rate2,yb_fc_rate2,fc_rate3,yb_fc_rate3,area_code,pay_num,complain_status,is_sign,voicebox_num,usevoicebox_num,need_jzb,shop_time,corporate_account,is_yp FROM cm_ods_mask.${db_table}_delta;

ALTER TABLE cm_ods_mask.${db_table}_delta DROP PARTITION (yyMMdd=${d2});
"