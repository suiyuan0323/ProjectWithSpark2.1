package org.test

import com.alibaba.fastjson.{JSON, JSONObject}

/**
  * @author xiaomei.wang
  * @date 2019/9/2 10:13
  * @version 1.0
  */
object ArraySpanParse {
  def main(args: Array[String]): Unit = {
    // ArrayJson ==》 然后炸裂
    val messageJson =
      """
        |[{"duration":212398,"id":"6e840af0f17b8316","kind":"SERVER","localEndpoint":{"ipv4":"10.244.37.181","serviceName":"qa"},"name":"post","parentId":"eac8886023cdf866",
        |"remoteEndpoint":{"ipv4":"10.244.27.211","port":35780},"shared":true,"tags":{"http.method":"POST","http.path":"/qa/simple_query"},"timestamp":1567389655382008,
        |"traceId":"5d6c77d73ccb72c05ee4342aef40af2b"},{"duration":212398,"id":"6e840af0f17b8316","kind":"SERVER","localEndpoint":{"ipv4":"10.244.37.181","serviceName":"qa"},"name":"post","parentId":"eac8886023cdf866",
        |"remoteEndpoint":{"ipv4":"10.244.27.211","port":35780},"shared":true,"tags":{"http.method":"POST","http.path":"/qa/simple_query"},"timestamp":1567389655382008,
        |"traceId":"5d6c77d73ccb72c05ee4342aef40af2b"}]"""
        .stripMargin
    JSON.isValidObject(messageJson)
    val jsonArray = JSON.parseArray(messageJson).iterator()
    var resArray = scala.collection.mutable.ArrayBuffer[String]()
    while(jsonArray.hasNext){
      val jsonItem = jsonArray.next()
      resArray += jsonItem.toString
    }

    //      val jsonArray = JSON.parseArray(messageString)
    //    var resArray = scala.collection.mutable.ArrayBuffer()
    // jsonArray.forEach(jsonItem =>

    println("-------------")
  }
}
