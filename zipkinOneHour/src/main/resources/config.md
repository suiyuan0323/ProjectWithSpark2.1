每小时从es做聚合：serviceName、fromService、path、method、max、min、avg、amount
因为5m， 的watermark 延迟是10 minutes。就造成数据库中 42才会有25-30的聚合。延迟12分钟。
所以设置 每小时的14分钟时刻，触发job。

{'spark.aispeech.job.type':'1h',
'spark.aispeech.es.nodes':'10.24.1.44,10.24.1.23,10.24.1.24',
'spark.aispeech.es.port':'9200',
'spark.aispeech.read.es.type':'summary',
'spark.aispeech.read.es.index':'zipkin_summary_5m-',
'spark.aispeech.write.es.index':'zipkin_summary_1h-',
'spark.aispeech.write.es.type':'summary',
'spark.memory.fraction':'0.8',
'spark.memory.storageFraction':'0.2',
'spark.memory.offHeap.enabled':'true',
'spark.memory.offHeap.size':'2048mb'}
