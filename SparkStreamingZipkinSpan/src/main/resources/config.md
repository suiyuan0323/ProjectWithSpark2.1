测试
'spark.aispeech.zk.hosts':'test-namenode-1.aispeech.com:2181,test-namenode-2.aispeech.com:2181,test-datanode-1.aispeech.com:2181,test-datanode-2.aispeech.com:2181,test-kafka-1.aispeech.com:2181'',
'spark.aispeech.zk.session.timeout':'300000',
'spark.aispeech.zk.connection.timeout':'60000',
'spark.aispeech.kafka.brokers':'10.12.6.57:6667,10.12.6.58:6667,10.12.6.59:6667',
'spark.aispeech.kafka.consumer.topics':'ba-test-zipkin-log',
'spark.aispeech.kafka.consumer.groupId':'zipkin-span-consumer',
'spark.aispeech.kafka.producer.topic':'bz_ba_zipkin_spanSpark',
'spark.aispeech.streaming.duration.Seconds':'300',
'spark.aispeech.data.exclude.services':'ddsserver,dm_dispatch,cdmsvr-v2'

线上
'spark.aispeech.zk.hosts':'insight-hadoop-namenode-1.aispeech.com:2181,insight-hadoop-namenode-2.aispeech.com:2181,insight-hadoop-namenode-3.aispeech.com:2181,insight-hadoop-namenode-4.aispeech.com:2181,insight-hadoop-namenode-5.aispeech.com:2181',
'spark.aispeech.zk.session.timeout':'300000',
'spark.aispeech.zk.connection.timeout':'60000',
'spark.aispeech.kafka.brokers':'10.24.1.10:6667,10.24.1.21:6667,10.24.1.32:6667,10.24.1.43:6667',
'spark.aispeech.kafka.consumer.topics':'ba-prod-zipkin-log',
'spark.aispeech.kafka.consumer.groupId':'zipkin-span-consumer',
'spark.aispeech.kafka.producer.topic':'bz_ba_zipkin_spanSpark',
'spark.aispeech.streaming.duration.Seconds':'300',
'spark.aispeech.data.exclude.services':'ddsserver,dm_dispatch,cdmsvr-v2'

main 参数
'spark.aispeech.checkpoint':'/user/rsbj_ba_backend/zipkin/span/checkpoint/'