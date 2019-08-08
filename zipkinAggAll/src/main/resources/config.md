所有调用service的。
serviceName fromService path method
service     all         all  all

线上 1m
{'spark.aispeech.read.kafka.startOffsets':'latest',
'spark.aispeech.read.kafka.brokers':'10.24.1.10:6667,10.24.1.21:6667,10.24.1.32:6667,10.24.1.43:6667',
'spark.aispeech.read.kafka.topics':'bz_ba_zipkin_spanSpark',
'spark.aispeech.write.es.nodes':'10.24.1.44,10.24.1.23,10.24.1.24',
'spark.aispeech.write.es.port':'9200',
'spark.aispeech.write.es.type':'summary',
'spark.aispeech.write.es.index':'zipkin_summary_1m-',
'spark.aispeech.checkpoint':'/user/rsbj_ba_backend/zipkin/checkpoint/summaryAll/1m/',
'spark.aispeech.job.type':'1mAll',
'spark.aispeech.data.agg.window.duration':'1 minutes',
'spark.aispeech.data.span.watermark.delay':'10 minutes',
'spark.aispeech.trigger.time':'60 seconds',
'spark.memory.fraction':'0.8',
'spark.memory.storageFraction':'0.2',
'spark.memory.offHeap.enabled':'true',
'spark.memory.offHeap.size':'2048mb'}

5m
{'spark.aispeech.read.kafka.startOffsets':'latest',
'spark.aispeech.read.kafka.brokers':'10.24.1.10:6667,10.24.1.21:6667,10.24.1.32:6667,10.24.1.43:6667',
'spark.aispeech.read.kafka.topics':'bz_ba_zipkin_spanSpark',
'spark.aispeech.write.es.nodes':'10.24.1.44,10.24.1.23,10.24.1.24',
'spark.aispeech.write.es.port':'9200',
'spark.aispeech.write.es.type':'summary',
'spark.aispeech.write.es.index':'zipkin_summary_5m-',
'spark.aispeech.checkpoint':'/user/rsbj_ba_backend/zipkin/checkpoint/summaryAll/5m/',
'spark.aispeech.job.type':'5mAll',
'spark.aispeech.data.agg.window.duration':'5 minutes',
'spark.aispeech.data.span.watermark.delay':'10 minutes',
'spark.aispeech.trigger.time':'60 seconds',
'spark.memory.fraction':'0.8',
'spark.memory.storageFraction':'0.2',
'spark.memory.offHeap.enabled':'true',
'spark.memory.offHeap.size':'2048mb'}