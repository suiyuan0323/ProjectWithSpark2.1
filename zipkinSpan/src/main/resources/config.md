{'spark.aispeech.read.kafka.startOffsets':'latest',
'spark.aispeech.read.kafka.brokers':'10.24.1.10:6667,10.24.1.21:6667,10.24.1.32:6667,10.24.1.43:6667',
'spark.aispeech.read.kafka.topics':'ba-prod-zipkin-log',
'spark.aispeech.write.kafka.topic':'bz_ba_zipkin_spanSpark',
'spark.aispeech.data.exclude.services':'ddsserver,dm_dispatch,cdmsvr-v2',
'spark.aispeech.data.watermark.delay':'2 minutes',
'spark.aispeech.span.window.duration':'15 minutes',
'spark.aispeech.span.slide.duration':'5 minutes',
'spark.aispeech.checkpoint':'/user/rsbj_ba_backend/zipkin/checkpoint/span/',
'spark.aispeech.trigger.time':'60 seconds',
'spark.memory.fraction':'0.8',
'spark.memory.storageFraction':'0.2',
'spark.memory.offHeap.enabled':'true',
'spark.memory.offHeap.size':'2048mb',
'spark.aispeech.read.kafka.maxOffsetperTrigger':'1500000'}



测试
{'spark.aispeech.read.kafka.startOffsets':'latest',
'spark.aispeech.read.kafka.brokers':'10.12.6.57:6667,10.12.6.58:6667,10.12.6.59:6667',
'spark.aispeech.read.kafka.topics':'ba-test-zipkin-log',
'spark.aispeech.write.kafka.topic':'bz_ba_zipkin_spanSpark',
'spark.aispeech.data.exclude.services':'ddsserver,dm_dispatch,cdmsvr-v2',
'spark.aispeech.data.watermark.delay':'2 minutes',
'spark.aispeech.span.window.duration':'15 minutes',
'spark.aispeech.span.slide.duration':'5 minutes',
'spark.aispeech.checkpoint':'/user/rsbj_ba_backend/zipkin/checkpoint/span/',
'spark.aispeech.trigger.time':'60 seconds',
'spark.memory.fraction':'0.8',
'spark.memory.storageFraction':'0.2',
'spark.memory.offHeap.enabled':'true',
'spark.memory.offHeap.size':'2048mb',
'spark.aispeech.read.kafka.maxOffsetperTrigger':'1500000'}
