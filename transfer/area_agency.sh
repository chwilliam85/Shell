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
CREATE TABLE IF NOT EXISTS cm_ods_safe.${db_table} AS SELECT aid,areaid,areaname,pid,level,realname,password,mobile,photo,address,bank_type,bank_card_num,idcard,createtime,status FROM cm_ods_mask.${db_table}_delta WHERE 1=2;
ALTER TABLE cm_ods_safe.${db_table} SET FILEFORMAT ORCFile;

INSERT OVERWRITE TABLE cm_ods_safe.${db_table} SELECT aid,areaid,areaname,pid,level,case when (length(encode(realname,'UTF-8')) <= 9) then replace(realname,substr(realname,2,1),'*') when (length(encode(realname,'UTF-8')) <= 12) then replace(realname,substr(realname,2,2),repeat('*',2)) when (length(encode(realname,'UTF-8')) <= 24) then replace(realname,substr(realname,2,length(realname)-2),repeat('*',length(realname))) else realname end as realname,password,case when (length(mobile) >= 4) then replace(mobile,substr(mobile,4,4),repeat('*',4)) else mobile end as mobile,photo,address,bank_type,case when (length(bank_card_num) > 0) then sha2(bank_card_num,256) else bank_card_num end as bank_card_num,case when (length(idcard) > 0) then sha2(idcard,256) else idcard end as idcard,createtime,status FROM cm_ods_mask.${db_table}_delta;

CREATE TABLE IF NOT EXISTS cm_ods_mask.${db_table} LIKE cm_ods_safe.${db_table};

INSERT OVERWRITE TABLE cm_ods_mask.${db_table} SELECT aid,areaid,areaname,pid,level,realname,password,mobile,photo,address,bank_type,bank_card_num,idcard,createtime,status FROM cm_ods_mask.${db_table}_delta;

ALTER TABLE cm_ods_mask.${db_table}_delta DROP PARTITION (yyMMdd=${d2});
"