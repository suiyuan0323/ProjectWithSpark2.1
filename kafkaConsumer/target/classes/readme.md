消费kafka
kafka-clients-0.10.0.1.jar,kafka-streams-0.10.0.1.jar,kafka_2.11-0.10.0.1.jar,spark-sql-kafka-0-10_2.11-2.1.0.jar,spark-streaming-kafka-0-10_2.11-2.1.0.jar

{'spark.aispeech.read.kafka.startOffsets':'latest',
'spark.aispeech.read.kafka.brokers':'10.24.1.10:6667,10.24.1.21:6667,10.24.1.32:6667,10.24.1.43:6667',
'spark.aispeech.read.kafka.topics':'bz_ba_zipkin_spanSpark',
'spark.aispeech.checkpoint':'/user/rsbj_ba_backend/zipkin/checkpoint/summary/test/',
'spark.aispeech.trigger.time':'60 seconds',
'spark.memory.fraction':'0.8',
'spark.memory.storageFraction':'0.2',
'spark.memory.offHeap.enabled':'true',
'spark.memory.offHeap.size':'2048mb'}