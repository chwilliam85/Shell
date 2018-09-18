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
CREATE TABLE IF NOT EXISTS cm_ods_safe.${db_table} AS SELECT id,uid,service_code,shop_name,provinces,address,realname,phone,service_phone,bank_kh_name,bank_card_id,khh,khh_name,bank_code,rate,pay_limit,createtime,auth_type,auth_status,location,location_name,permit_num,wx_id,alipay_id,fc_rate1,yb_fc_rate1,fc_rate2,yb_fc_rate2,fc_rate3,yb_fc_rate3,lasttime,type,type_lv,old_type_lv,all_amount,noact_amount,amount,bank_all_amount,bank_amount,day,total_amount,fc_amount,fc_all_amount,area_code,logo_img,server_id,new_type,new_level,ms_zfb_status,ms_wx_status,default_bank,reserve_bank,reserve_time,gxms_pass_time,hlb_pass_time,dlb_pass_time,wzpa_pass_time,wzpa_cater_time,shop_time,default_bank_zfb,reserve_bank_zfb,reserve_time_zfb FROM cm_ods_mask.${db_table}_delta WHERE 1=2;

ALTER TABLE cm_ods_safe.${db_table} SET FILEFORMAT ORCFile;

INSERT OVERWRITE TABLE cm_ods_safe.${db_table} SELECT id,uid,service_code,shop_name,provinces,address,case when (length(encode(realname,'UTF-8')) <= 9) then replace(realname,substr(realname,2,1),'*') when (length(encode(realname,'UTF-8')) <= 12) then replace(realname,substr(realname,2,2),repeat('*',2)) when (length(encode(realname,'UTF-8')) <= 24) then replace(realname,substr(realname,2,length(realname)-2),repeat('*',length(realname))) else realname end as realname,case when (length(phone) >= 4) then replace(phone,substr(phone,4,4),repeat('*',4)) else phone end as phone,service_phone,case when (length(encode(bank_kh_name,'UTF-8')) <= 9) then replace(bank_kh_name,substr(bank_kh_name,2,1),'*') when (length(encode(bank_kh_name,'UTF-8')) <= 12) then replace(bank_kh_name,substr(bank_kh_name,2,2),repeat('*',2)) when (length(encode(bank_kh_name,'UTF-8')) <= 24) then replace(bank_kh_name,substr(bank_kh_name,2,length(bank_kh_name)-2),repeat('*',length(bank_kh_name))) else bank_kh_name end as bank_kh_name,case when (length(bank_card_id) > 0) then sha2(bank_card_id,256) else bank_card_id end as bank_card_id,khh,khh_name,bank_code,rate,pay_limit,createtime,auth_type,auth_status,location,location_name,permit_num,wx_id,alipay_id,fc_rate1,yb_fc_rate1,fc_rate2,yb_fc_rate2,fc_rate3,yb_fc_rate3,lasttime,type,type_lv,old_type_lv,all_amount,noact_amount,amount,bank_all_amount,bank_amount,day,total_amount,fc_amount,fc_all_amount,area_code,logo_img,server_id,new_type,new_level,ms_zfb_status,ms_wx_status,default_bank,reserve_bank,reserve_time,gxms_pass_time,hlb_pass_time,dlb_pass_time,wzpa_pass_time,wzpa_cater_time,shop_time,default_bank_zfb,reserve_bank_zfb,reserve_time_zfb FROM cm_ods_mask.${db_table}_delta;

CREATE TABLE IF NOT EXISTS cm_ods_mask.${db_table} LIKE cm_ods_safe.${db_table};

INSERT OVERWRITE TABLE cm_ods_mask.${db_table} SELECT id,uid,service_code,shop_name,provinces,address,realname,phone,service_phone,bank_kh_name,bank_card_id,khh,khh_name,bank_code,rate,pay_limit,createtime,auth_type,auth_status,location,location_name,permit_num,wx_id,alipay_id,fc_rate1,yb_fc_rate1,fc_rate2,yb_fc_rate2,fc_rate3,yb_fc_rate3,lasttime,type,type_lv,old_type_lv,all_amount,noact_amount,amount,bank_all_amount,bank_amount,day,total_amount,fc_amount,fc_all_amount,area_code,logo_img,server_id,new_type,new_level,ms_zfb_status,ms_wx_status,default_bank,reserve_bank,reserve_time,gxms_pass_time,hlb_pass_time,dlb_pass_time,wzpa_pass_time,wzpa_cater_time,shop_time,default_bank_zfb,reserve_bank_zfb,reserve_time_zfb FROM cm_ods_mask.${db_table}_delta;

ALTER TABLE cm_ods_mask.${db_table}_delta DROP PARTITION (yyMMdd=${d2});
"