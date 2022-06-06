// Databricks notebook source
import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.types._
import com.snowflake.snowpark.SaveMode

object BankingDataSF extends App{
  def connect_to_snowflake(): Unit = {
    val builder = Session.builder.configs(Map(
      "URL" -> "https://xj21352.central-india.azure.snowflakecomputing.com",
      "USER" -> "RP926463",
      "PRIVATE_KEY_FILE" -> "/dbfs/FileStore/tables/rsa_key.p8",
      "PRIVATE_KEY_FILE_PWD" -> "qweasdzxc",
      "ROLE" -> "ACCOUNTADMIN",
      "WAREHOUSE" -> "COMPUTE_WH",
      "DB" -> "SNOWFLAKE_SAMPLE_DATA",
      "SCHEMA" -> "TPCDS_SF100TCL"
    ))
    val session = builder.create
    
    var df1 = session.table("CALL_CENTER")
    df1.show(1)
  }
}


// COMMAND ----------

BankingDataSF.connect_to_snowflake

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.rm("dbfs:/FileStore/tables/data.json")

// COMMAND ----------

//READ JSON IN SCALA

import scala.io._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper


val json = Source.fromFile("/dbfs/FileStore/tables/data.json")
val mapper = new ObjectMapper() with ScalaObjectMapper
mapper.registerModule(DefaultScalaModule)
val parsedJson = mapper.readValue[Map[String, Object]](json.reader())

// COMMAND ----------

import scala.io._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper


val json = Source.fromFile("/dbfs/FileStore/tables/data.json")
val mapper = new ObjectMapper() with ScalaObjectMapper
mapper.registerModule(DefaultScalaModule)
val parsedJson = mapper.readValue[Map[String, Object]](json.reader())

// COMMAND ----------

parsedJson("mappings")

// COMMAND ----------

val names = List(Map(source -> Map(name -> SSN, type -> String), target -> Map(name -> Adhar, type -> String)), Map(source -> Map(name -> America, type -> String), target -> Map(name -> India, type -> String)))

for (name <- names)
    println(name)


// COMMAND ----------

val listOfMaps: List[Map[String, String]] = List(
  Map("a" -> "A1", "b" -> "B1"),
  Map("a" -> "A2", "b" -> "B2"),
  Map("a" -> "A3", "b" -> "B3")
)

// COMMAND ----------

val listOfMaps: List[Map[String, Map[String, String]]] = List(
  Map("source" -> Map("name" -> "SSN", "DT" -> "String"), "target" -> Map("name" -> "Adhar", "DT" -> "String")), 
  Map("source" -> Map("name" -> "America", "DT" -> "String"), "target" -> Map("name" -> "India", "DT" -> "String"))
)

// COMMAND ----------

val df = spark.read.json("/FileStore/tables/data.json")
//df.printSchema()
df.show(false)

//val df = spark.read.schema(schema).json("/FileStore/tables/data.json").cache()
//df.filter($"_corrupt_record".isNotNull).count()

// COMMAND ----------

val multiline_df = spark.read.option("multiline","true")
      .json("/FileStore/tables/data.json")
multiline_df.show(false) 

// COMMAND ----------

import scala.io.Source
import org.apache.spark.sql.types._
val schemaSource = Source.fromFile("/dbfs/FileStore/tables/data.json").getLines.mkString
val schemaFromJson = DataType.fromJson(schemaSource).asInstanceOf[StructType]
//val df3 = spark.createDataFrame(
//      spark.sparkContext.parallelize(structureData),schemaFromJson)
//df3.printSchema()

// COMMAND ----------

//import oslib
import scala.io.Source._
val jsonString = os.read("/dbfs/FileStore/tables/data.json")
val data = ujson.read(jsonString)

// COMMAND ----------

val multiline_df = spark.read.option("multiline","true")
      .json("/FileStore/tables/data.json")
multiline_df.show(false) 

// COMMAND ----------

import org.apache.spark.sql.functions.explode
var mapped_df = multiline_df.select($"FeedSource",$"SourceEON",$"TargetEON",$"TargetSource",explode($"mappings").alias("map"))

// COMMAND ----------

mapped_df.show(false)

// COMMAND ----------

mapped_df.select("map")[0].show(false)

// COMMAND ----------

import org.json4s._
import org.json4s.jackson.JsonMethods._

//...
def jsonStrToMap(jsonStr: String): Map[String, Any] = {
  implicit val formats = org.json4s.DefaultFormats

  parse(jsonStr).extract[Map[String, Any]]
}

// COMMAND ----------

var jsonStr = """
{
  "FeedSource":"RAP",
  "TargetSource":"Refinery",
  "SourceEON":"1234",
  "TargetEON":"5678",
  "mappings":[
    {
      "source":{
        "name":"SSN",
        "type":"String"
      },
      "target":{
        "name":"Adhar",
        "type":"String"
      }
    },
    {
      "source":{
        "name":"America",
        "type":"String"
      },
      "target":{
        "name":"India",
        "type":"String"
      }
    }
  ]
}
"""

var mapped_str = jsonStrToMap(jsonStr)

// COMMAND ----------

mapped_str("mappings")

// COMMAND ----------

mapped_str("mappings").toList()

// COMMAND ----------

val listOfMaps: List[Map[String, Map[String, String]]] = List(
  Map("source" -> Map("name" -> "SSN", "DT" -> "String"), "target" -> Map("name" -> "Adhar", "DT" -> "String")), 
  Map("source" -> Map("name" -> "America", "DT" -> "String"), "target" -> Map("name" -> "India", "DT" -> "String"))
)

// COMMAND ----------

parsedJson("mappings").foreach(x => println(x("source")("name")))

// COMMAND ----------

parsedJson("mappings")

// COMMAND ----------


