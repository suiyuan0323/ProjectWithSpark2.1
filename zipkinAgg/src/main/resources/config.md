线上 1m
{'spark.aispeech.job.source':'kafka',
'spark.aispeech.job.sink':'es',
'spark.aispeech.queryName':'zipkin-summary-1m',
'spark.aispeech.write.es.timeColumn':'beginTime',
'spark.aispeech.job.log.level':'WARN',
'spark.aispeech.read.kafka.startOffsets':'latest',
'spark.aispeech.read.kafka.brokers':'10.24.1.10:6667,10.24.1.21:6667,10.24.1.32:6667,10.24.1.43:6667',
'spark.aispeech.read.kafka.topics':'bz_ba_zipkin_spanSpark',
'spark.aispeech.read.kafka.failOnDataLoss':'false',
'spark.aispeech.read.kafka.maxOffsetsPerTrigger':'300000',
'spark.aispeech.write.es.nodes':'10.24.1.44,10.24.1.23,10.24.1.24',
'spark.aispeech.write.es.port':'9200',
'spark.aispeech.write.es.batch.write.retry.wait':'30s',
'spark.aispeech.write.es.batch.write.retry.count':'60',
'spark.aispeech.write.es.type':'summary',
'spark.aispeech.write.es.index':'zipkin_summary_1m-',
'spark.aispeech.checkpoint':'hdfs://insight/user/rsbj_ba_backend/zipkin/summary/1m/',
'spark.aispeech.job.type':'1m',
'spark.aispeech.job.summaryType':'summary',
'spark.aispeech.data.agg.window.duration':'1 minutes',
'spark.aispeech.data.span.watermark.delay':'5 minutes',
'spark.aispeech.trigger.time':'1 minutes',
'spark.memory.fraction':'0.8',
'spark.memory.storageFraction':'0.2',
'spark.yarn.driver.memoryOverhead':'1024mb',
'spark.yarn.executor.memoryOverhead':'1024mb',
'spark.cleaner.periodicGC.interval':'15min',
'spark.monitor.urls':'https://oapi.dingtalk.com/robot/send?access_token=0c44f10f6b0a02b06838010851293c2b8c61cb4e2e3849bcbb395172e72988a2',
'spark.rdd.compress':'true',
'spark.network.timeout':'300s',
'spark.driver.extraJavaOptions':'-XX:+UseG1GC -XX:+UseCompressedOops -Xms3000M -Xmn600M',
'spark.executor.extraJavaOptions':'-XX:+UseG1GC -XX:+UseCompressedOops -Xms3000M -Xmn600M -XX:MetaspaceSize=100m -XX:MaxMetaspaceSize=100m',
'spark.serializer':'org.apache.spark.serializer.KryoSerializer',
'spark.aispeech.summary.sql.shuffle.partitions':'500',
'spark.aispeech.summaryAll.sql.shuffle.partitions':'50',
'spark.aispeech.summary.es.partitions':'3',
'spark.aispeech.summaryAll.es.partitions':'1',
'spark.scheduler.listenerbus.eventqueue.size':'100000'}



// 不设置trigger，处理完成之后立即触发
// partition，5分钟有几百条，200个多了。如果5分钟几十万，200个不够。测一下
// 测试过程，5分钟300-500条，本来200个，不合理，
// 1、设置600个，开始倾斜，有的7小时，有的6s，还有出现一个job失败，立即重启一个job，但是失败的job还是active，
// 2、2019年9月10日：改为20个在测试，主要是all的partition 2个就够了
// 3、20个partition，有的11min，每个partition不到500条(最大300)。 每个core大小5G不够呀，做shuffle呢。so，内存大点，partition少点。


// idata配置  file log4j.properties
// 'spark.speculation':'true', 关了吧，有的task 被kill了


'spark.aispeech.queryName':'zipkin-summary-5m',
'spark.aispeech.queryName':'zipkin-summary-5m-all',
'spark.aispeech.queryName':'zipkin-summary-1m',
'spark.aispeech.queryName':'zipkin-summary-1m-all',

5m
{'spark.aispeech.job.source':'kafka',
'spark.aispeech.job.sink':'es',
'spark.aispeech.job.log.level':'WARN',
'spark.aispeech.write.es.timeColumn':'beginTime',
'spark.aispeech.job.type':'5m',
'spark.aispeech.job.summaryType':'summary',
'spark.aispeech.queryName':'zipkin-summary-5m',
'spark.aispeech.read.kafka.startOffsets':'latest',
'spark.aispeech.read.kafka.brokers':'10.24.1.10:6667,10.24.1.21:6667,10.24.1.32:6667,10.24.1.43:6667',
'spark.aispeech.read.kafka.topics':'bz_ba_zipkin_spanSpark',
'spark.aispeech.read.kafka.failOnDataLoss':'false',
'spark.aispeech.read.kafka.maxOffsetsPerTrigger':'800000',
'spark.aispeech.write.es.nodes':'10.24.1.44,10.24.1.23,10.24.1.24',
'spark.aispeech.write.es.port':'9200',
'spark.aispeech.write.es.batch.write.retry.wait':'30s',
'spark.aispeech.write.es.batch.write.retry.count':'60',
'spark.aispeech.write.es.type':'summary',
'spark.aispeech.write.es.index':'zipkin_summary_5m-',
'spark.aispeech.checkpoint':'hdfs://insight/user/rsbj_ba_backend/zipkin/summary/',
'spark.aispeech.data.agg.window.duration':'5 minutes',
'spark.aispeech.data.span.watermark.delay':'5 minutes',
'spark.aispeech.trigger.time':'5 minutes',
'spark.monitor.urls':'https://oapi.dingtalk.com/robot/send?access_token=0c44f10f6b0a02b06838010851293c2b8c61cb4e2e3849bcbb395172e72988a2',
'spark.memory.fraction':'0.8',
'spark.memory.storageFraction':'0.2',
'spark.yarn.executor.memoryOverhead':'2048mb',
'spark.yarn.driver.memoryOverhead':'2048mb',
'spark.cleaner.periodicGC.interval':'15min',
'spark.rdd.compress':'true',
'spark.network.timeout':'300s',
'spark.driver.extraJavaOptions':'-XX:+UseConcMarkSweepGC -XX:+UseParNewGC',
'spark.executor.extraJavaOptions':'-XX:+UseConcMarkSweepGC -XX:+UseParNewGC',
'spark.serializer':'org.apache.spark.serializer.KryoSerializer',
'spark.aispeech.summary.sql.shuffle.partitions':'1000',
'spark.aispeech.summaryAll.sql.shuffle.partitions':'100',
'spark.aispeech.summary.es.partitions':'3',
'spark.aispeech.summaryAll.es.partitions':'1',
'spark.scheduler.listenerbus.eventqueue.size':'100000'}