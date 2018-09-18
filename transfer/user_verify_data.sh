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
CREATE TABLE IF NOT EXISTS cm_ods_safe.${db_table} AS SELECT id,uid,img1,img1_status,img1_exif,img2,img2_status,img2_exif,img3,img3_status,img3_exif,img4,img4_status,img4_exif,img5,img5_status,img5_exif,img6,img6_status,img6_exif,location,address,video,video_status,status,createtime,confirmtime,submittime,shop_type,mark,auth_realname,auth_person_card,auth_shop_name,bank_num,lasttime,shop_sub_id,license_status,authtime,user_up_confirmtime,user_up_changetime,user_up_submittime,user_up_type,head_salesman,salesman_id,salesman_name,server_id,redeal_status,openid,username,help_openid,help_name,permit_num_user,permit_name_user,permit_address_user,permit_auth,other,img7,img7_status,img7_exif,img8,img8_status,img8_exif,corporate_account,chief_agent,auth_queue,queue_time FROM cm_ods_mask.${db_table}_delta WHERE 1=2;

ALTER TABLE cm_ods_safe.${db_table} SET FILEFORMAT ORCFile;

INSERT OVERWRITE TABLE cm_ods_safe.${db_table} SELECT id,uid,img1,img1_status,img1_exif,img2,img2_status,img2_exif,img3,img3_status,img3_exif,img4,img4_status,img4_exif,img5,img5_status,img5_exif,img6,img6_status,img6_exif,location,address,video,video_status,status,createtime,confirmtime,submittime,shop_type,mark,case when (length(encode(auth_realname,'UTF-8')) <= 9) then replace(auth_realname,substr(auth_realname,2,1),'*') when (length(encode(auth_realname,'UTF-8')) <= 12) then replace(auth_realname,substr(auth_realname,2,2),repeat('*',2)) when (length(encode(auth_realname,'UTF-8')) <= 24) then replace(auth_realname,substr(auth_realname,2,length(auth_realname)-2),repeat('*',length(auth_realname))) else auth_realname end as auth_realname,case when (length(auth_person_card) > 0) then sha2(auth_person_card,256) else auth_person_card end as auth_person_card,auth_shop_name,case when (length(bank_num) > 0) then sha2(bank_num,256) else bank_num end as bank_num,lasttime,shop_sub_id,license_status,authtime,user_up_confirmtime,user_up_changetime,user_up_submittime,user_up_type,head_salesman,salesman_id,salesman_name,server_id,redeal_status,openid,username,help_openid,help_name,permit_num_user,permit_name_user,permit_address_user,permit_auth,other,img7,img7_status,img7_exif,img8,img8_status,img8_exif,corporate_account,chief_agent,auth_queue,queue_time FROM cm_ods_mask.${db_table}_delta;

CREATE TABLE IF NOT EXISTS cm_ods_mask.${db_table} LIKE cm_ods_safe.${db_table};

INSERT OVERWRITE TABLE cm_ods_mask.${db_table} SELECT id,uid,img1,img1_status,img1_exif,img2,img2_status,img2_exif,img3,img3_status,img3_exif,img4,img4_status,img4_exif,img5,img5_status,img5_exif,img6,img6_status,img6_exif,location,address,video,video_status,status,createtime,confirmtime,submittime,shop_type,mark,auth_realname,auth_person_card,auth_shop_name,bank_num,lasttime,shop_sub_id,license_status,authtime,user_up_confirmtime,user_up_changetime,user_up_submittime,user_up_type,head_salesman,salesman_id,salesman_name,server_id,redeal_status,openid,username,help_openid,help_name,permit_num_user,permit_name_user,permit_address_user,permit_auth,other,img7,img7_status,img7_exif,img8,img8_status,img8_exif,corporate_account,chief_agent,auth_queue,queue_time FROM cm_ods_mask.${db_table}_delta;

ALTER TABLE cm_ods_mask.${db_table}_delta DROP PARTITION (yyMMdd=${d2});
"