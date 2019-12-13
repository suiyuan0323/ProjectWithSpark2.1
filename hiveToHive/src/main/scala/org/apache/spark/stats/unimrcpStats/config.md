{'spark.aispeech.queryName':'alpha_dm_kf_unimrcp_stats',
'spark.query.table':'fact_ba.alpha_fact_kf_unimrcp_detail',
'spark.query.columns':'module,event_name,product_id,session_id,phone_number,message__event,log_time',
'spark.query.day.partition':'p_day',
'spark.write.columns':'product_id,session_id,phone_number,begintime,begintimestamp,endtime,endtimestamp,duration,interaction,state',
'spark.write.table':'dm_ba.alpha_dm_kf_unimrcp_stats',
'spark.write.day.partition':'p_day',
'spark.monitor.urls':'https://oapi.dingtalk.com/robot/send?access_token=0c44f10f6b0a02b06838010851293c2b8c61cb4e2e3849bcbb395172e72988a2',
'spark.job.log.level':'WARN',
'spark.job.sink':'console'}