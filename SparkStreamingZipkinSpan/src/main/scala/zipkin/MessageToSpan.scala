package zipkin

import com.alibaba.fastjson.{JSON, JSONObject}

object MessageToSpan {

  val KIND_CLIENT = "client"
  val KIND_SERVER = "server"
  /**
    * 解析每一条log，取出相应字段
    */
  def parseMessage(item: String): (String, Map[String, String]) = {
    var res = ("", Map[String, String]())
    try {
      val values = JSON.parseObject(item)
      val message = values.getJSONObject("message")
      var valueMap = Map[String, String]()
      valueMap += ("kind" -> (if (message.containsKey("kind")) message.get("kind").toString else ""))
      valueMap += ("duration" -> (if (message.containsKey("duration")) message.get("duration").toString else ""))
      valueMap += ("serviceName" -> (if (message.containsKey("localEndpoint") && message.getJSONObject("localEndpoint").containsKey("serviceName"))
        message.getJSONObject("localEndpoint").get("serviceName").toString else ""))

      valueMap += ("remoteName" -> (if (message.containsKey("kind") && message.get("kind").equals(KIND_SERVER)) getServerRemoteName(message) else ""))

      valueMap += ("method" -> (if (message.containsKey("kind") && message.get("kind").toString.equals(KIND_SERVER) && message.containsKey("tags")
        && message.getJSONObject("tags").containsKey("http.method")) message.getJSONObject("tags").get("http.method").toString else ""))

      valueMap += ("path" -> (if (message.containsKey("kind") && message.get("kind").toString.equals(KIND_SERVER) && message.containsKey("tags")
        && message.getJSONObject("tags").containsKey("http.path")) message.getJSONObject("tags").get("http.path").toString else ""))

      valueMap += ("timestamp" -> (if (message.containsKey("kind") && message.get("kind").toString.equals(KIND_SERVER) && message.containsKey("timestamp"))
        message.get("timestamp").toString else ""))

      val id = if (message.containsKey("id")) message.get("id").toString else ""
      val traceId = if (message.containsKey("traceId")) message.get("traceId").toString else ""
      res = (id + "_" + traceId, valueMap)
    } catch {
      case e: Exception => println(e.getMessage + item)
    }
    res
  }

  /**
    * 过滤log，取id+traceid存在，且kind是client和servver，且去掉不统计的server
    */
  def filterServiceName(spanMessage: (String, Map[String, String]), excludes: List[String]): Boolean = {
    if (spanMessage._1 != "" // id+traceId
      && spanMessage._2.get("kind").get != "" // kind
      && (spanMessage._2.get("kind").get.equals(KIND_CLIENT) || spanMessage._2.get("kind").get.equals(KIND_SERVER))
      && spanMessage._2.get("serviceName") != ""
      && !excludes.contains(spanMessage._2.get("serviceName"))
    )
      true
    else false
  }

  /**
    * server 和client 合并成span
    */
  def combineLogToSpan(firstRow: Map[String, String], secondRow: Map[String, String]) = {
    if (secondRow.get("kind").get.equals(KIND_SERVER)) {
      Map[String, String]("kind" -> "",
        "duration" -> (if (!firstRow.get("duration").get.equals("")) firstRow.get("duration").get else secondRow.get("duration").get),
        "serviceName" -> secondRow.get("serviceName").get,
        "remoteName" -> (if (!firstRow.get("serviceName").get.equals("")) firstRow.get("serviceName").get else secondRow.get("remoteName").get),
        "method" -> secondRow.get("method").get,
        "path" -> secondRow.get("path").get,
        "timestamp" -> secondRow.get("timestamp").get)
    } else {
      Map[String, String]("kind" -> "",
        "duration" -> (if (!secondRow.get("duration").get.equals("")) secondRow.get("duration").get else firstRow.get("duration").get),
        "serviceName" -> firstRow.get("serviceName").get,
        "remoteName" -> (if (!secondRow.get("serviceName").get.equals("")) secondRow.get("serviceName").get else firstRow.get("remoteName").get),
        "method" -> firstRow.get("method").get,
        "path" -> firstRow.get("path").get,
        "timestamp" -> firstRow.get("timestamp").get)
    }
  }

  // 从server的log中取出remotename
  private def getServerRemoteName(message: JSONObject): String = {
    if (message.containsKey("remoteEndpoint") && message.getJSONObject("remoteEndpoint").containsKey("ipv4"))
      message.getJSONObject("remoteEndpoint").get("ipv4").toString
    else if (message.containsKey("traceId") && message.containsKey("id") && message.get("traceId").toString.endsWith(message.get("id").toString)) {
      "itself"
    } else ""
  }
}
