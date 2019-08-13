# 说明：filter.day没传入，就取前一天时间
# 同步hive表某个partition数据，处理后到另一个hive表对应的partition中
alpha:
{'spark.aispeech.read.hive.table.name':'fact_ba.alpha_fact_kf_freeswitch_detail',
'spark.aispeech.read.hive.table.columns':'event_name,product_id,session_id,phone_number,message_event,log_time',
'spark.aispeech.read.hive.table.partitions':'{"p_day":"20190807"}',
'spark.aispeech.write.table.name':'dm_ba.alpha_dm_kf_freeswitch_state',
'spark.aispeech.write.table.partitions':'{"p_day":"20190807"}',
'spark.memory.fraction':'0.8',
'spark.memory.storageFraction':'0.2',
'spark.memory.offHeap.enabled':'true',
'spark.memory.offHeap.size':'2048mb'}

# 测试下用prod环境、用recordId统计交互数，是否相等，保存到dm_ba里面对应的alpha的表
prod
{'spark.aispeech.read.hive.table.name':'fact_ba.fact_kf_freeswitch_detail',
'spark.aispeech.read.hive.table.columns':'event_name,product_id,session_id,phone_number,message__event,log_time',
'spark.aispeech.read.hive.table.partitions':'{"p_day":"20190807"}',
'spark.aispeech.write.table.name':'dm_ba.alpha_dm_kf_freeswitch_state',
'spark.aispeech.write.table.partitions':'{"p_day":"20190807"}',
'spark.aispeech.write.table.schema.columns':'product_id string comment "精灵", session_id string comment "会话ID", phone_number string comment "电话号码", begintime string comment "开始时间", begintimeStamp bigint comment "开始时间戳_毫秒", endtime string comment "结束时间", endtimestamp string comment "结束时间戳_毫秒", duration int comment "通话时长_毫秒", interaction int comment "交互次数", state string comment "最终状态"',
'spark.aispeech.write.table.schema.partitions':'p_day string comment "日期" ',
'spark.memory.fraction':'0.8',
'spark.memory.storageFraction':'0.2',
'spark.memory.offHeap.enabled':'true',
'spark.memory.offHeap.size':'2048mb'}
