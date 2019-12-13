// alpha,fs和mrcp通路, 离线，从detail_hour到通路的表
alpha
{'spark.aispeech.queryName':'alpha_dm_kf_parallelism_hour',
'spark.write.table':'dm_ba.alpha_dm_kf_parallelism_state_new',
'spark.write.columns':'module, product_id, maxnum, day_hour',
'spark.read.table.freeswitch':'fact_ba.alpha_fact_kf_freeswitch_detail_hour',
'spark.read.table.uniMrcp':'fact_ba.alpha_fact_kf_unimrcp_detail_hour',
'spark.monitor.urls':'https://oapi.dingtalk.com/robot/send?access_token=0c44f10f6b0a02b06838010851293c2b8c61cb4e2e3849bcbb395172e72988a2',
'spark.job.log.level':'WARN',
'spark.sql.shuffle.partitions':'1',
'spark.job.sink':'hive'}


prod
{'spark.aispeech.queryName':'dm_kf_parallelism_hour',
'spark.write.table':'dm_ba.dm_kf_parallelism_state_new',
'spark.write.columns':'module, product_id, maxnum, day_hour',
'spark.read.table.freeswitch':'fact_ba.fact_kf_freeswitch_detail_hour',
'spark.read.table.uniMrcp':'fact_ba.fact_kf_unimrcp_detail_hour',
'spark.monitor.urls':'https://oapi.dingtalk.com/robot/send?access_token=0c44f10f6b0a02b06838010851293c2b8c61cb4e2e3849bcbb395172e72988a2',
'spark.job.log.level':'WARN',
'spark.sql.shuffle.partitions':'1',
'spark.job.sink':'hive'}