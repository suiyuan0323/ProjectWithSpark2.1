// 公共
'spark.driver.extraJavaOptions':'-XX:+UseConcMarkSweepGC -XX:+UseParNewGC',
'spark.executor.extraJavaOptions':'-XX:+UseConcMarkSweepGC -XX:+UseParNewGC',
'spark.serializer':'org.apache.spark.serializer.KryoSerializer',
'spark.rdd.compress':'true',
'spark.aispeech.job.source':'kafka',
'spark.aispeech.job.sink':'es',
'spark.aispeech.queryName':'',

// es 如果没配置，就write就是构建index和writer。
'spark.aispeech.write.es.timeColumn':'time',
// es must 自己填
'spark.aispeech.write.es.index':'zipkin_span-',
'spark.aispeech.write.es.type':'summary',
// es must sink
'spark.aispeech.write.es.nodes.wan.only':'false',
'spark.aispeech.write.es.nodes':'10.24.1.44,10.24.1.23,10.24.1.24',
'spark.aispeech.write.es.port':'9200',
'spark.aispeech.write.es.batch.size.bytes':'2mb',
'spark.aispeech.write.es.batch.size.entries':'5000',
'spark.aispeech.write.es.batch.write.refresh':'false',
'spark.aispeech.write.es.batch.write.retry.wait':'30s',
'spark.aispeech.write.es.batch.write.retry.count':'6',
'spark.es.net.http.auth.user':'rsbj_ba_backend',
'spark.es.net.http.auth.pass':'ba_backend@Speech1024',

// kafka


// hive











