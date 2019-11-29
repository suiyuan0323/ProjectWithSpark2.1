    {'spark.driver.extraJavaOptions':'-XX:+UseG1GC -XX:+UseCompressedOops -Xms2000M -Xmn400M',
'spark.executor.extraJavaOptions':'-XX:+UseConcMarkSweepGC -XX:+UseParNewGC',
'spark.rdd.compress':'true',
'spark.serializer':'org.apache.spark.serializer.KryoSerializer',
'spark.aispeech.job.source':'kafka',
'spark.aispeech.job.sink':'es',
'spark.aispeech.queryName':'',

必须写
'spark.aispeech.write.es.nodes.wan.only':'false',
'spark.aispeech.write.es.nodes':'10.24.1.44,10.24.1.23,10.24.1.24',
'spark.aispeech.write.es.type':'summary',
'spark.aispeech.write.es.index':'zipkin_span-',
'spark.aispeech.write.es.timeColumn':'time',

spark.aispeech.write.es.timeColumn：如果没配置，就write就是构建index和writer。


'spark.aispeech.write.es.batch.size.bytes':'2mb',
'spark.aispeech.write.es.batch.size.entries':'5000',
'spark.aispeech.write.es.batch.write.refresh':'false',
'spark.aispeech.write.es.batch.write.retry.wait':'30s',
'spark.aispeech.write.es.batch.write.retry.count':'3',
'spark.aispeech.write.es.port':'9200',

// 可以有
'spark.aispeech.write.es.partition':'3'





}


