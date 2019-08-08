package hive

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author xiaomei.wang
  * @date 2019/7/25 18:02
  * @version 1.0
  */
object DataFrameImplicit {

  implicit class DataFrameToHive(val df: DataFrame) {
    /**
      * spark内存表数据插入到hive指定表的指定分区
      *
      * @param spark
      * @param tableName hive table name
      * @param pField
      * @param pValue
      * @param fields
      * @param sparkTable
      */
    def saveToHive(spark: SparkSession, tableName: String, pField: String, pValue: String, fields: List[String], sparkTable: String): Unit = {
      spark.sql(s"insert into ${tableName} partition(${pField}=${pValue}) select ${fields.mkString(",")} from ${sparkTable} ")
    }
  }

}