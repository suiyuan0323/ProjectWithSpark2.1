try catch

// 计算耗时比较大，给storage少点，不需要缓存，基本都是在内存中参与计算，所以rdd也不要压缩 'spark.rdd.compress':'',

// 1、from_json,
// 2、gc得log日志，试试只打印full gc得日志
// 3、spark得日志级别


{'spark.aispeech.job.source':'kafka',
'spark.aispeech.job.sink':'kafka',
'spark.aispeech.queryName':'zipkin-span',
'spark.aispeech.job.log.level':'WARN',
'spark.aispeech.checkpoint':'hdfs://insight/user/rsbj_ba_backend/business/',
'spark.aispeech.read.kafka.startOffsets':'latest',
'spark.aispeech.read.kafka.brokers':'10.24.1.10:6667,10.24.1.21:6667,10.24.1.32:6667,10.24.1.43:6667',
'spark.aispeech.read.kafka.topics':'ba-prod-zipkin-log',
'spark.aispeech.read.kafka.failOnDataLoss':'false',
'spark.aispeech.read.kafka.maxOffsetsPerTrigger':'500000',
'spark.aispeech.write.kafka.brokers':'10.24.1.10:6667,10.24.1.21:6667,10.24.1.32:6667,10.24.1.43:6667',
'spark.aispeech.write.kafka.topic':'bz_ba_zipkin_spanSpark',
'spark.aispeech.data.watermark.delay':'30 seconds',
'spark.aispeech.span.window.duration':'6 minutes',
'spark.aispeech.span.slide.duration':'30 seconds',
'spark.aispeech.trigger.time':'30 seconds',
'spark.monitor.urls':'https://oapi.dingtalk.com/robot/send?access_token=0c44f10f6b0a02b06838010851293c2b8c61cb4e2e3849bcbb395172e72988a2',
'spark.memory.fraction':'0.80',
'spark.memory.storageFraction':'0.20',
'spark.memory.offHeap.enabled':'true',
'spark.memory.offHeap.size':'2048mb',
'spark.debug.maxToStringFields':'100',
'spark.yarn.driver.memoryOverhead':'1024mb',
'spark.yarn.executor.memoryOverhead':'1024mb',
'spark.cleaner.periodicGC.interval':'15min',
'spark.sql.shuffle.partitions':'1000',
'spark.network.timeout':'300s',
'spark.rdd.compress':'true',
'spark.driver.extraJavaOptions':'-XX:+UseConcMarkSweepGC -XX:+UseParNewGC',
'spark.executor.extraJavaOptions':'-XX:+UseConcMarkSweepGC -XX:+UseParNewGC',
'spark.serializer':'org.apache.spark.serializer.KryoSerializer'}

'spark.aispeech.write.es.partition':'3'