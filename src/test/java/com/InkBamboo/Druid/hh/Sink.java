/*
package com.InkBamboo.Druid.hh;
import java.util.Properties;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import com.metamx.common.logger.Logger;
import com.metamx.tranquility.flink.BeamSink;


*/
/**
 * 获取kafka数据
 * @author dell
 *
 *//*

public class Sink {
	  private static final Logger log = new Logger(Sink.class);

	  public static void main(String[] args) {
		  
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties pro = new Properties();
		pro.put("bootstrap.servers", "192.168.0.101:9092");
		pro.put("zookeeper.connect", "192.168.0.101:2181/kafka");
		pro.put("group.id", "group");
		env.getConfig().disableSysoutLogging();  //设置此可以屏蔽掉日记打印情况
		env.getConfig().setRestartStrategy(
				RestartStrategies.fixedDelayRestart(4, 10000)); 
		env.enableCheckpointing(5000);
	    	 try {
	    		 DataStream<String> stream = env.addSource(new FlinkKafkaConsumer08<>("metrics-039", new SimpleStringSchema(), pro));
	    		 stream.print();
	    		 //处理逻辑..................
	    		 CtiDruidEventBeamFactory cb = new CtiDruidEventBeamFactory();
	    		 BeamSink sink = new BeamSink(cb, false);
		    	 stream.addSink(sink);
				env.execute("data to mysql start");
			} catch (Exception e) {
				e.printStackTrace();
			}
	  }
}
*/
