OnlinePhoneJob：算通路数，每秒一个数，插入es
groupBy window、module
agg( fun(module, session, productId)=> Array[pid-count] )


prod-fs
{'spark.aispeech.queryName':'kf_parallelism_module_pid_fs'
'spark.aispeech.redis.key.prefix':'prod_idata_baBusiness_',
'spark.aispeech.write.es.index':'ba_kf_parallelism-',
'spark.aispeech.read.kafka.topics':'ba-prod-trace-log',
'spark.aispeech.data.module':'freeswitch',
'spark.aispeech.data.eventName':'charge,call-end',  
'spark.aispeech.data.eventName.callOn':'charge',
'spark.aispeech.data.eventName.callEnd':'call-end',


'spark.aispeech.job.master':'yarn',
'spark.job.log.level':'WARN',
'spark.aispeech.job.source':'kafka',
'spark.aispeech.job.sink':'es',
'spark.aispeech.data.interval':'1 seconds',
'spark.aispeech.data.watermark.delay':'150 seconds',
'spark.aispeech.trigger.time':'1 seconds',
'spark.aispeech.read.kafka.startOffsets':'latest',
'spark.aispeech.read.kafka.brokers':'10.24.1.10:6667,10.24.1.21:6667,10.24.1.32:6667,10.24.1.43:6667',
'spark.aispeech.read.kafka.failOnDataLoss':'false',
'spark.aispeech.read.kafka.maxOffsetsPerTrigger':'15000',
'spark.aispeech.redis.key.replicas':'4',
'spark.aispeech.redis.key.replicas.timeout.millisecond':'1',
'spark.aispeech.redis.key.timeOut.max':'1800',
'spark.aispeech.redis.key.timeOut.min':'60',
'spark.aispeech.redis.address':'10.24.1.114:6380,10.24.1.114:6379,10.24.1.115:6379,10.24.1.115:6380,10.24.1.116:6379,10.24.1.116:6380',
'spark.aispeech.redis.password':'AIspeech-324bb',
'spark.aispeech.write.es.nodes':'10.24.1.44,10.24.1.23,10.24.1.24',
'spark.aispeech.write.es.port':'9200',
'spark.aispeech.write.es.type':'summary',
'spark.aispeech.write.es.nodes.wan.only':'false',
'spark.aispeech.write.es.batch.size.bytes':'1mb',
'spark.aispeech.write.es.batch.size.entries':'300',
'spark.aispeech.write.es.batch.write.refresh':'false',
'spark.aispeech.checkpoint':'hdfs://insight/user/rsbj_ba_backend/business/',
'spark.monitor.urls':'https://oapi.dingtalk.com/robot/send?access_token=0c44f10f6b0a02b06838010851293c2b8c61cb4e2e3849bcbb395172e72988a2',
'spark.memory.fraction':'0.80',
'spark.memory.storageFraction':'0.20',
'spark.driver.userClassPathFirst':'true',
'spark.executor.userClassPathFirst':'true',
'spark.kryo.referenceTracking':'false',
'spark.yarn.driver.memoryOverhead':'1024mb',
'spark.yarn.executor.memoryOverhead':'1024mb',
'spark.cleaner.periodicGC.interval':'15min',
'spark.sql.shuffle.partitions':'2', 
'spark.network.timeout':'300s',
'spark.driver.extraJavaOptions':'-XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+ParallelRefProcEnabled -XX:+CMSClassUnloadingEnabled',
'spark.executor.extraJavaOptions':'-XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+ParallelRefProcEnabled -XX:+CMSClassUnloadingEnabled',
'spark.serializer':'org.apache.spark.serializer.KryoSerializer',
'spark.scheduler.listenerbus.eventqueue.size':'100000'}


prod-mrcp
'spark.aispeech.queryName':'kf_parallelism_module_pid_mrcp',
'spark.aispeech.read.kafka.topics':'ba-prod-trace-log',
'spark.aispeech.redis.key.prefix':'prod_idata_baBusiness_',
'spark.aispeech.write.es.index':'ba_kf_parallelism-',
'spark.aispeech.data.module':'uniMrcp',
'spark.aispeech.data.eventName':'connect_to_asr,close_to_asr',
'spark.aispeech.data.eventName.callOn':'connect_to_asr',
'spark.aispeech.data.eventName.callEnd':'close_to_asr',


alpha-mrcp
'spark.aispeech.read.kafka.topics':'ba-alpha-trace-log',
'spark.aispeech.redis.key.prefix':'alpha_idata_baBusiness_',
'spark.aispeech.write.es.index':'ba_kf_parallelism_alpha-',
'spark.aispeech.queryName':'alpha_kf_parallelism_module_pid_mrcp',
'spark.aispeech.data.module':'uniMrcp',
'spark.aispeech.data.eventName':'connect_to_asr,close_to_asr',
'spark.aispeech.data.eventName.callOn':'connect_to_asr',
'spark.aispeech.data.eventName.callEnd':'close_to_asr',

alpha-fs
'spark.aispeech.read.kafka.topics':'ba-alpha-trace-log',
'spark.aispeech.redis.key.prefix':'alpha_idata_baBusiness_',
'spark.aispeech.write.es.index':'ba_kf_parallelism_alpha-',
'spark.aispeech.queryName':'alpha_kf_parallelism_module_pid_fs',
'spark.aispeech.data.module':'freeswitch',
'spark.aispeech.data.eventName':'charge,call-end',
'spark.aispeech.data.eventName.callOn':'charge',
'spark.aispeech.data.eventName.callEnd':'call-end',