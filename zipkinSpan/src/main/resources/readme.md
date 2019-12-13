1、合并zipkin信息为span
2、发送给kafka

合并的规则：
1、kind是server：
（1）localpoint的serviceName是serviceName
（2）duration
（3）logTime
（4）fromService：如果id是spanId的后缀，则为itself，否则取remote的serviceName，如果再没有就是unknown
（5）method
（6）path
