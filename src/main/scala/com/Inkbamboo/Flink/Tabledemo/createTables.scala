package com.Inkbamboo.Flink.Tabledemo

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.table.api.{Table, TableEnvironment, Types}
import org.apache.flink.table.sinks.{CsvTableSink, TableSink}
import org.apache.kafka.common.config.ConfigDef.Type



/**
  * Created By InkBamboo
  * Date: 2018/12/17 10:41
  * Calm Positive
  * Think Then Ask
  *
  * desc:  官方api，上手测试tableApi
  */
object streamcreateTables {
  import org.apache.flink.streaming.api.scala._
  def main(args: Array[String]): Unit = {

    val streamenv = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(streamenv)

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "192.168.183.135:9092")
    prop.setProperty("zookeeper.connect", "192.168.183.135:2181")
    prop.setProperty("group.id", "test")

    val resstream = streamenv.addSource(new FlinkKafkaConsumer010[String]("wiki_test",new SimpleStringSchema(),prop))
    val wordecount = resstream
      .filter(x=>x.nonEmpty)
      .flatMap(x=>x.split(" "))
      .map(x=>((x,x.hashCode),1))
      .keyBy(0)
      .timeWindow(Time.seconds(10))
      .sum(1)
      .map(x=>(x._1._1,x._1._2,x._2))


    val tbl = tableEnv.fromDataStream(wordecount)
    tbl.printSchema()

   tableEnv.registerTable("tbl_testOne",tbl)

    val restbl =  tableEnv.sqlQuery("select count(1) from tbl_testOne")

    streamenv.execute("tableApiTest")
  }

}

object batchTable{
  import org.apache.flink.api.scala._
  def main(args: Array[String]): Unit = {

    val batchenv = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(batchenv)

    val csvDataset  = new CsvTableSink("D:\\scala_workspace\\flinktest\\src\\main\\resources\\UserBehavior.csv")
    val fieldNames = Array("a","b","c","d","e")
    val fieldTypes:Array[TypeInformation[_]] = Array(Types.STRING,Types.STRING,Types.STRING,Types.STRING,Types.STRING)
    tableEnv.registerTableSink("tbl_test_sink",fieldNames,fieldTypes,csvDataset)

    val restbl = tableEnv.sqlQuery("select a,d,b from tbl_test_sink")
    val writesink = new CsvTableSink("D:\\scala_workspace\\flinktest\\src\\main\\resources\\testtable")

    restbl.writeToSink(writesink)

    batchenv.execute("table_test")
  }
}
