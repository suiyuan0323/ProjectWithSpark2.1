# 说明：从hive同步数据到es
# 1、hive tables：可以传多个tablename，逗号分隔。比如 temp.baserver_duration
# 2、hive partitions：
#    （1）同步前一天到es，partitions='' 空，默认日期分区字段在default.partition.day.field中设置，是p_day，值取前一天
#    （2）也可以补之前日期的同步，传partitions，支持多分区字段json格式、每个分区字段支持多值，值之间逗号分隔
#         比如有两个分区字段day(string)和hour(int)，且都有多个值，则 partitions='{"day":"20190728,20190729","hour":"20,21"}'
# 3、es的index命名，前缀必须是hive_source，其他的自定义，后缀是表示日期的分区日期字段的值
#    由于es中提前设置了index的template，设置shard数是5，实现分布式存储
# 4、数据不支持重复插入，如果要重复执行，需要先进es删除已有的数据

{'spark.aispeech.read.hive.table.names':'fact_ba.fact_kg_query_repeat',
'spark.aispeech.read.hive.table.partitions':'{"p_day":"20190804"}',
'spark.aispeech.read.hive.table.default.partition.day.field':'p_day',
'spark.aispeech.read.hive.table.columns':'skillId,question,p_flag,repeatNum',
'spark.aispeech.es.nodes':'10.24.1.44,10.24.1.23,10.24.1.24',
'spark.aispeech.es.port':'9200',
'spark.aispeech.write.es.index':'hive_source_fact_kg_query_repeat-',
'spark.aispeech.write.es.type':'duration',
'spark.memory.fraction':'0.8',
'spark.memory.storageFraction':'0.2',
'spark.memory.offHeap.enabled':'true',
'spark.memory.offHeap.size':'2048mb'}