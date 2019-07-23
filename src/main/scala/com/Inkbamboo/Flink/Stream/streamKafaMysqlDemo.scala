package com.Inkbamboo.Flink.Stream

import java.sql.{Connection, DriverManager}
import java.util.Properties

import com.google.gson.Gson
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}

/**
  *author Inkbamboo
  * 2018/12/13
  *
  * kafka数据读取以及写入，
  * 数据输出到mysql
  */
object streamKafaMysqlDemo {

  def main(args: Array[String]): Unit = {

    val gson = new Gson
    val streamenv = StreamExecutionEnvironment.getExecutionEnvironment

    /**
      * 设置检查点，以及flink失败恢复策略
      * 从指定的检查点恢复：flink run -c com.Inkbamboo.streamdemo.streamKafaMysqlDemo -j flinktest-1.0-SNAPSHOT.jar -p 1 -s /home/hadoop/runjars/flink_checkPoints/255f99e593e2671f72a3660b661bb19d/chk-8
      *  其中-s参数指定了需要从指定checkpoint恢复的目录
      */
    //设置检查点，检查点更新周期为60s，类型为精确一次（可选至少一次）
    streamenv.enableCheckpointing(60000,CheckpointingMode.EXACTLY_ONCE)
    //当作业失败或者取消的时候 ，不会自动清除这些保留的检查点
    streamenv.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //设置检查点的保存路径
    val cpp = new Path("file:///home/hadoop/runjars/flink_checkPoints/")
    val fsBackend: StateBackend = new FsStateBackend(cpp)
    streamenv.setStateBackend(fsBackend)


    /**
      * kafka配置信息
      */
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "192.168.137.131:9092")
    //prop.setProperty("zookeeper.connect", "192.168.183.133:2181")
    prop.setProperty("group.id", "test")
    //收集kafka中的数据
    val kafkaConsumer010 = new FlinkKafkaConsumer010[String]("wiki_test",new SimpleStringSchema(),prop)
    //此处可以设置kafka读取时的开始位置，还可以自己设置开始的offset
    kafkaConsumer010.setCommitOffsetsOnCheckpoints(true)
    kafkaConsumer010.setStartFromEarliest()
    //添加kafka数据源
    val dataStreamK  = streamenv.addSource(kafkaConsumer010)
    /**输出到kafka中
      * kafka-topics.sh --zookeeper 192.168.183.135:2181 --topic wiki_test --replication-factor 1 --partitions 1 --create
      *
      * kafka-console-producer.sh --broker-list 192.168.183.133:9092 --topic wiki_test
      *
      * kafka-console-consumer.sh  --bootstrap-server 192.168.183.135:9092 --topic wiki_test --from-beginning
      */




    val res = dataStreamK
      .filter(x=>x.nonEmpty)
      .flatMap(x=>x.split(" "))
      .map(x=>(x,1))
      .keyBy(0)
      .timeWindow(Time.seconds(10))
      .sum(1)

    res.print()

      //.map(x=>x._1+"-"+x._2)
      // 将处理之后的数据输出到 新的kafka topic中
      //.addSink(new FlinkKafkaProducer010[String]("wiki_test2",new SimpleStringSchema(),prop))
      //将数据写入到mysql数据库中  ,java代码需要自定义类并实现RichSinkFunction，重写invoke方法。参考mysqlSink.scala编写java类
      //  res.addSink(x=>writeToMysql(x._1,x._2))
    //

    streamenv.execute("kafkaRead")
  }

  /**
    * 数据写入mysql，此处可以使用连接池
    *
    * 注意：此处的代码执行位置在task的运行节点上，故如果创建连接池，那myql最终的连接数为 taskNum*eachTaskConnections
    * @param str
    * @param ii
    */
  def writeToMysql(str:String,ii:Int): Unit ={
    val serialVersionUID = 1L
    var connection:Connection = null
    val username = "hadoop"
    val password = "hadoop"
    val drivername = "com.mysql.jdbc.Driver"
    val dburl = "jdbc:mysql://92.168.183.133:3306/flinkdatas"
    Class.forName(drivername)
    try {
      connection = DriverManager.getConnection(dburl, username, password)
      val sql = "insert into kafka_res values(?,?)"
      val preparedStatement = connection.prepareStatement(sql)
      preparedStatement.setInt(2, ii)
      preparedStatement.setString(1, str)
      preparedStatement.executeUpdate()
      if (preparedStatement != null) {
        preparedStatement.close()
      }
      if (connection != null) {
        connection.close()
      }
    }catch {
      case e=>e.printStackTrace()
    }finally {
      if (connection != null) {
        connection.close()
      }
    }
  }

}



